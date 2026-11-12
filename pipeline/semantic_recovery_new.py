import json, os
from pathlib import Path
import numpy as np
from openai import OpenAI
from psycopg.types.json import Jsonb
from common.database import get_connection
from common.nemotron_embedding import embed_rows
from semantic_recovery_fast import load_references, parse_vector, normalize_matrix, calculate_top_k

ROOT=Path(__file__).resolve().parents[1]; THRESHOLD=.80

def classify_recovered(row):
    config=json.loads((ROOT/'settings/filters.json').read_text(encoding='utf-8-sig')); names=[x['name'] for x in config['categories']]
    schema={'type':'object','properties':{'is_legal':{'type':'boolean'},'primary_category':{'type':'string','enum':names},'secondary_categories':{'type':'array','items':{'type':'string','enum':names},'maxItems':2},'confidence':{'type':'number','minimum':0,'maximum':1},'reason':{'type':'string'}},'required':['is_legal','primary_category','secondary_categories','confidence','reason'],'additionalProperties':False}
    key=os.getenv('OPENAI_API_KEY')
    if not key: raise RuntimeError('OPENAI_API_KEY is not set')
    out=OpenAI(api_key=key).responses.create(model=os.getenv('OPENAI_CLASSIFICATION_MODEL','gpt-5.6-luna'),store=False,instructions='Decide whether the supplied news is substantively legal-related. Return the strict JSON schema.',input='Title:\n%s\n\nArticle text:\n%s'%(row['title'] or '',row['description'] or ''),text={'format':{'type':'json_schema','name':'semantic_legal_recheck','strict':True,'schema':schema}})
    result=json.loads(out.output_text)
    if result['is_legal']:
        ids={x['name']:x['id'] for x in config['categories']}; result['categories']=[{'id':ids[x],'name':x,'category_id':ids[x],'category':x,'role':'primary' if i==0 else 'secondary','source':'semantic_recovery_openai','confidence':result['confidence']} for i,x in enumerate([result['primary_category'],*(result['secondary_categories'] or [])])]
    return result

def main():
    db=get_connection()
    try:
        with db.transaction():
            db.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS semantic_recovery_status TEXT")
            db.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS semantic_recovery_error TEXT")
        # NULL is the legacy checkpoint state.  Include it once so historical
        # keyword misses can be recovered; terminal states are intentionally
        # excluded to make reruns resumable and idempotent.
        rows=db.execute("SELECT document_id,title,description,nemotron_embedding,source FROM news_articles WHERE filter_status='excluded_stage1' AND (semantic_recovery_status IS NULL OR semantic_recovery_status IN ('PENDING','CLASSIFY_PENDING')) ORDER BY id").fetchall()
        print('FIRST FILTER REJECTED:',len(rows))
        missing=[r for r in rows if r['nemotron_embedding'] is None]
        if missing:
            embed_rows(db,missing,batch_size=32)
            rows=db.execute("SELECT document_id,title,description,nemotron_embedding,source FROM news_articles WHERE filter_status='excluded_stage1' AND (semantic_recovery_status IS NULL OR semantic_recovery_status IN ('PENDING','CLASSIFY_PENDING')) ORDER BY id").fetchall()
        refs,refmat=load_references(db); refmat=normalize_matrix(refmat.astype(np.float32))
        candidates=[r for r in rows if r['nemotron_embedding'] is not None and db.execute("SELECT semantic_recovery_status FROM news_articles WHERE document_id=%s",(r['document_id'],)).fetchone()['semantic_recovery_status'] in (None,'PENDING')]
        if candidates:
            idx,scores=calculate_top_k(normalize_matrix(np.vstack([parse_vector(r['nemotron_embedding']) for r in candidates]).astype(np.float32)),refmat,5)
            with db.transaction():
                for row,ii,ss in zip(candidates,idx,scores):
                    payload={'max_similarity':float(ss[0]),'avg_top_k_similarity':float(np.mean(ss)),'matched_legal_articles':[{'document_id':refs[int(i)]['document_id'],'similarity':float(s)} for i,s in zip(ii,ss)]}
                    if float(ss[0])>=THRESHOLD: db.execute("UPDATE news_articles SET semantic_recovery_status='CLASSIFY_PENDING',semantic_recovery_error=NULL,category_candidates=%s,updated_at=CURRENT_TIMESTAMP WHERE document_id=%s",(Jsonb(payload),row['document_id']))
                    else: db.execute("UPDATE news_articles SET semantic_recovery_status='REVIEW',semantic_recovery_error='below semantic threshold 0.80; manual review required',category_candidates=%s,updated_at=CURRENT_TIMESTAMP WHERE document_id=%s",(Jsonb(payload),row['document_id']))
        pending=db.execute("SELECT document_id,title,description FROM news_articles WHERE filter_status='excluded_stage1' AND semantic_recovery_status='CLASSIFY_PENDING' ORDER BY id").fetchall(); recovered=rejected=failed=0
        for row in pending:
            try:
                result=classify_recovered(row)
                with db.transaction(): db.execute("UPDATE news_articles SET filter_status=%s,categories=%s,semantic_recovery_status=%s,semantic_recovery_error=%s,updated_at=CURRENT_TIMESTAMP WHERE document_id=%s",('categorized' if result['is_legal'] else 'excluded_final',Jsonb(result.get('categories',[])),'RECOVERED_LEGAL' if result['is_legal'] else 'NON_LEGAL',None if result['is_legal'] else result.get('reason','OpenAI marked non-legal'),row['document_id']))
                if result['is_legal']: recovered+=1
                else: rejected+=1
            except Exception as exc:
                with db.transaction(): db.execute("UPDATE news_articles SET semantic_recovery_status='FAILED',semantic_recovery_error=%s WHERE document_id=%s",(str(exc)[:2000],row['document_id']))
                failed+=1
                break
        print('REJECT EMBEDDED:',len(rows)-len(missing)); print('SIM >= 0.80:',len(candidates)); print('RECOVERED LEGAL:',recovered); print('FINAL REJECTED:',rejected); print('FAILED:',failed)
        if failed: raise RuntimeError('semantic recovery failed; inspect semantic_recovery_error')
    finally: db.close()
if __name__=='__main__': main()
