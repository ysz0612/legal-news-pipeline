import json,os
import psycopg
from dotenv import load_dotenv
from neo4j import GraphDatabase
from psycopg.rows import dict_row
load_dotenv(); P=dict(host='127.0.0.1',port=55432,user=os.getenv('POSTGRES_USER','postgres'),password=os.getenv('POSTGRES_PASSWORD','1234')); U=os.getenv('NEO4J_URI','bolt://127.0.0.1:7687'); PW=os.getenv('NEO4J_PASSWORD'); MODEL='nvidia/nemotron-3-embed-1b'
def main():
    pg=psycopg.connect(dbname='law_db',row_factory=dict_row,**P); lg=psycopg.connect(dbname='legal_shared',row_factory=dict_row,**P); d=GraphDatabase.driver(U,auth=(os.getenv('NEO4J_USER','neo4j'),PW)); d.verify_connectivity()
    try:
        base=pg.execute("SELECT DISTINCT news_document_id FROM news_legal_relations WHERE embedding_model=%s",(MODEL,)).fetchall(); ids=[x['news_document_id'] for x in base]
        rel=pg.execute("SELECT news_document_id,legal_chunk_id,legal_document_id,similarity_score,rank FROM news_legal_relations WHERE news_document_id=ANY(%s) AND embedding_model=%s",(ids,MODEL)).fetchall(); dbn={}
        for x in rel: dbn[x['news_document_id']]=dbn.get(x['news_document_id'],0)+1
        with d.session() as s: existing=s.run("UNWIND $ids AS id OPTIONAL MATCH (n:News {document_id:id}) OPTIONAL MATCH (n)-[:RELATED_TO]->() RETURN id,count(n) AS nodes,count(*) AS rel",ids=ids).data()
        have={x['id']:(x['nodes'],x['rel']) for x in existing}; target=[i for i in ids if have.get(i,(0,0))[0]==0 or have[i][1]<dbn[i]]
        news=pg.execute("SELECT document_id,title,description FROM news_articles WHERE document_id=ANY(%s)",(target,)).fetchall(); ch=lg.execute("SELECT DISTINCT c.id,c.document_id,c.chunk_key,c.chunk_index,c.chunk_type,c.content FROM public.legal_chunks c JOIN news_legal_relations r ON r.legal_chunk_id=c.id WHERE r.news_document_id=ANY(%s)",(target,)).fetchall(); did=list({x['document_id'] for x in ch}); docs=lg.execute("SELECT id,title,document_type,case_number FROM legal_documents WHERE id=ANY(%s)",(did,)).fetchall(); cfg=json.loads(open('settings/filters.json',encoding='utf-8-sig').read())
        with d.session() as s:
            s.run("CREATE CONSTRAINT category_id_unique IF NOT EXISTS FOR (c:Category) REQUIRE c.category_id IS UNIQUE").consume(); s.run("UNWIND $x AS r MERGE (c:Category {category_id:r.id}) SET c.name=r.name",x=[{'id':x['id'],'name':x['name']} for x in cfg['categories']]).consume(); s.run("UNWIND $x AS r MERGE (n:News {document_id:r.id}) ON CREATE SET n.title=r.title,n.description=r.description",x=[{'id':x['document_id'],'title':x['title'] or '','description':x['description'] or ''} for x in news]).consume()
            cats=pg.execute("SELECT document_id,categories FROM news_articles WHERE document_id=ANY(%s)",(target,)).fetchall()
            for x in cats: s.run("MATCH (n:News {document_id:$id}) UNWIND $x AS r MATCH (c:Category {category_id:coalesce(r.id,r.category_id)}) MERGE (n)-[:BELONGS_TO {role:r.role}]->(c)",id=x['document_id'],x=x['categories'] or []).consume()
            s.run("UNWIND $x AS r MERGE (d:LegalDocument {document_id:r.id}) ON CREATE SET d.title=r.title,d.document_type=r.type,d.document_number=r.num",x=[{'id':x['id'],'title':x['title'] or '','type':x['document_type'],'num':x['case_number']} for x in docs]).consume(); s.run("UNWIND $x AS r MERGE (c:LegalChunk {chunk_id:r.id}) ON CREATE SET c.document_id=r.did,c.chunk_key=r.key,c.chunk_index=r.idx,c.chunk_type=r.type,c.content=r.content",x=[{'id':x['id'],'did':x['document_id'],'key':x['chunk_key'],'idx':x['chunk_index'],'type':x['chunk_type'],'content':x['content'] or ''} for x in ch]).consume(); s.run("UNWIND $x AS r MATCH (c:LegalChunk {chunk_id:r.cid}) MATCH (d:LegalDocument {document_id:r.did}) MERGE (c)-[:PART_OF]->(d)",x=[{'cid':x['id'],'did':x['document_id']} for x in ch]).consume()
            for i in range(0,len(rel),500):
                x=[dict(r) for r in rel[i:i+500] if r['news_document_id'] in target]
                if x:s.run("UNWIND $x AS r MATCH (n:News {document_id:r.news_document_id}) MATCH (c:LegalChunk {chunk_id:r.legal_chunk_id}) MERGE (n)-[q:RELATED_TO]->(c) SET q.similarity=r.similarity_score,q.rank=r.rank,q.model=$model",x=x,model=MODEL).consume()
        print('NEO4J SYNC TARGET:',len(target)); print('NEO4J SYNCED:',len(target))
    finally: pg.close(); lg.close(); d.close()
if __name__=='__main__': main()
