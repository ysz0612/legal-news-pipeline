import argparse
import importlib.util
import json
from pathlib import Path
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from common.settings import filters, DATA
from common.store import connect, now, save_stage, save_final, digest, encode
from common.rules import legal_filter, categorize
from kafka.schemas import normalize_article
from psycopg.types.json import Jsonb


_RAG_MAIN_MODULE = None


def _load_rag_main_module():
    global _RAG_MAIN_MODULE
    if _RAG_MAIN_MODULE is not None:
        return _RAG_MAIN_MODULE
    rag_dir = Path(__file__).resolve().parent / "pycharm-rag-search-sample-main-py"
    spec = importlib.util.spec_from_file_location("_pycharm_rag_main", rag_dir / "main.py")
    if spec is None or spec.loader is None:
        raise ImportError(f"RAG main module not found: {rag_dir / 'main.py'}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    sys.path.insert(0, str(rag_dir))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    _RAG_MAIN_MODULE = module
    return module


def answer_question_once(question: str, output_format: str = "terminal") -> str:
    """Expose the existing RAG one-question handler to integrations."""
    rag_dir = Path(__file__).resolve().parent / "pycharm-rag-search-sample-main-py"
    sys.path.insert(0, str(rag_dir))
    try:
        return _load_rag_main_module().answer_question_once(question, output_format)
    finally:
        sys.path.pop(0)


def _article_from_row(row):
    """Rebuild the Kafka article shape from the PostgreSQL article columns."""
    return {
        'document_id': row['document_id'],
        'event_id': row['event_id'],
        'title': row['title'],
        'content': row['description'] or '',
        'source': row['source'],
        'url': row['url'],
        'published_at': row['published_at'].isoformat() if row['published_at'] else None,
        'collected_at': row['collected_at'].isoformat(),
        'legal_keywords': row['legal_keywords'] or [],
        'categories': row['categories'] or [],
        'category_candidates': row['category_candidates'] or [],
        'article_types': row['article_types'] or [],
        'filter_status': row['filter_status'],
        'filter_version': row['filter_version'],
    }


def classify_openai_all(db, batch_size=25, max_retries=4, workers=8):
    from common.openai_classifier import classify_article

    rows = db.execute(
        "SELECT document_id,title,description,filter_status FROM news_articles "
        "WHERE filter_status IN ('categorized','unclassified') ORDER BY id"
    ).fetchall()
    before_unclassified = sum(row['filter_status'] == 'unclassified' for row in rows)
    def classify_one(row):
        for attempt in range(max_retries + 1):
            try:
                result = classify_article({'title': row['title'], 'content': row['description'] or ''})
                categories = [
                    {
                        'category': category,
                        'role': 'primary' if position == 0 else 'secondary',
                        'source': 'openai',
                        'confidence': result['confidence'],
                    }
                    for position, category in enumerate(
                        [result['primary_category'], *result['secondary_categories']]
                    )
                ]
                return ('success', row['document_id'], categories)
            except Exception as exc:
                if attempt < max_retries:
                    time.sleep(min(2 ** attempt, 30))
                else:
                    return ('failure', row, type(exc).__name__)

    completed = 0
    for offset in range(0, len(rows), batch_size):
        batch_rows = rows[offset:offset + batch_size]
        successes, failures = [], []
        with ThreadPoolExecutor(max_workers=min(workers, len(batch_rows))) as pool:
            futures = [pool.submit(classify_one, row) for row in batch_rows]
            for future in as_completed(futures):
                outcome = future.result()
                if outcome[0] == 'success':
                    successes.append((outcome[1], outcome[2]))
                else:
                    failures.append((outcome[1], outcome[2]))
        with db.transaction():
            for document_id, categories in successes:
                    db.execute(
                        "UPDATE news_articles SET categories=%s, "
                        "category_candidates='[]'::jsonb, filter_status='categorized', "
                        "updated_at=CURRENT_TIMESTAMP WHERE document_id=%s",
                        (Jsonb(categories), document_id),
                    )
            for failed_row, error_type in failures:
                    failure_id = f"openai-classification:{failed_row['document_id']}"
                    db.execute(
                        "INSERT INTO news_processing_failures "
                        "(failure_id,input_topic,error_message,payload) "
                        "VALUES (%s,%s,%s,%s) "
                        "ON CONFLICT (failure_id) DO UPDATE SET "
                        "error_message=EXCLUDED.error_message,payload=EXCLUDED.payload",
                        (
                            failure_id,
                            'openai-classification',
                            error_type,
                            Jsonb({'document_id': failed_row['document_id'],
                                   'error_type': error_type}),
                        ),
                    )
        completed += len(batch_rows)
        print(f'Committed {completed}/{len(rows)}; successes={len(successes)} failures={len(failures)}', flush=True)

    db.commit()
    after_unclassified = db.execute(
        "SELECT count(*) AS count FROM news_articles WHERE filter_status='unclassified'"
    ).fetchone()['count']
    print('OpenAI reclassification complete')
    print('Target articles:', len(rows))
    print('Previously unclassified:', before_unclassified)
    print('Unclassified resolved:', before_unclassified - after_unclassified)


def publish_pending(db):
    from kafka.producer import send_article, flush
    count = 0
    while True:
        with db.transaction():
            rows = db.execute(
                'SELECT event_id, payload FROM kafka_outbox '
                'WHERE sent_at IS NULL ORDER BY created_at LIMIT 100'
            ).fetchall()
            if not rows:
                break
            for row in rows:
                send_article(row['payload'])
                db.execute('UPDATE kafka_outbox SET sent_at=%s WHERE event_id=%s',
                           (now(), row['event_id']))
                count += 1
    flush()
    print('Confirmed sent:', count)


def local_filter(db, config):
    # Uses exactly the same functions as Kafka consumers, for offline review/refilter.
    rows = db.execute(
        'SELECT document_id,event_id,title,description,source,url,published_at,collected_at,'
        'legal_keywords,categories,category_candidates,article_types,filter_status,filter_version '
        'FROM news_articles ORDER BY id'
    ).fetchall()
    for row in rows:
        article = legal_filter(_article_from_row(row), config)
        save_stage(db, 'local-legal-' + article['filter_version'], article)
        if article['filter_status'] != 'excluded_stage1':
            article = categorize(article, config)
        else:
            article.update(categories=[], category_candidates=[], article_types=[])
        save_final(db, article)
    print('Local filtering complete. This does not send or consume Kafka messages.')


def main():
    parser = argparse.ArgumentParser(description='Google RSS + 15 publishers + Kafka two-stage filters')
    subs = parser.add_subparsers(dest='command', required=True)
    p = subs.add_parser('collect')
    p.add_argument('--mode', choices=['broad','expanded'], default='broad')
    p.add_argument('--publisher', action='append')
    p.add_argument('--days', type=int, default=7)
    p.add_argument('--date', help='YYYY-MM-DD; daily query window hint')
    p.add_argument('--max-feeds', type=int, default=100)
    p.add_argument('--offset', type=int, default=0)
    p.add_argument('--delay', type=float, default=2)
    p.add_argument('--publish', action='store_true')
    for cmd in ('publish', 'stats', 'filter-local', 'sample', 'requeue', 'classify-openai-test', 'classify-openai'):
        subs.add_parser(cmd)
    subs.add_parser('classify-openai-pending')
    p = subs.add_parser('embed-news-test')
    p.add_argument('--limit', type=int, default=10)
    p.add_argument('--force', action='store_true')
    p = subs.add_parser('embed-news')
    p.add_argument('--batch-size', type=int, default=32)
    p.add_argument('--force', action='store_true')
    p = subs.add_parser('search-news')
    p.add_argument('query')
    p.add_argument('--limit', type=int, default=5)
    subs.add_parser('embed-news-nemotron-test')
    p = subs.add_parser('embed-news-nemotron')
    p.add_argument('--batch-size', type=int, default=16)
    p.add_argument('--force', action='store_true')
    p = subs.add_parser('search-news-nemotron')
    p.add_argument('query')
    p.add_argument('--top-k', type=int, default=5)
    subs.add_parser('embed-legal-nemotron-test')
    p = subs.add_parser('embed-legal-nemotron')
    p.add_argument('--batch-size', type=int, default=16)
    p = subs.add_parser('link-news-to-legal')
    p.add_argument('--top-k', type=int, default=5)
    p.add_argument('--min-score', type=float)
    p.add_argument('--limit', type=int)
    p.add_argument('--force', action='store_true')
    p = subs.add_parser('embed-excluded-nemotron')
    p.add_argument('--batch-size', type=int, default=16)
    p = subs.add_parser('recover-legal-semantic')
    p.add_argument('--top-k', type=int, default=5)
    p.add_argument('--limit', type=int)
    p.add_argument('--min-score', type=float)
    p.add_argument('--output', default='data/legal_semantic_candidates.json')
    subs.add_parser('analyze-legal-semantic-scores')
    p = subs.add_parser('export')
    p.add_argument('--raw', action='store_true')
    p.add_argument('--include-unclassified', action='store_true')
    p = subs.add_parser('replay-failure')
    p.add_argument('failure_id')
    p.add_argument('--corrected-file', help='Optional corrected article JSON')
    args = parser.parse_args()
    config, db = filters(), connect()
    try:
        if args.command == 'collect':
            if args.max_feeds < 1 or args.offset < 0 or args.days < 1 or args.delay < 1:
                parser.error('max-feeds/days >= 1, offset >= 0, delay >= 1 required')
            from collector.google_rss import collect
            collect(db, config, args)
            if args.publish:
                publish_pending(db)
        elif args.command == 'publish':
            publish_pending(db)
        elif args.command == 'filter-local':
            local_filter(db, config)
        elif args.command == 'classify-openai-test':
            from common.openai_classifier import classify_article
            rows = db.execute(
                "SELECT document_id,title,description FROM news_articles "
                "WHERE filter_status IN ('categorized','unclassified') "
                "ORDER BY id LIMIT 10"
            ).fetchall()
            for row in rows:
                result = classify_article({'title': row['title'], 'content': row['description'] or ''})
                print(json.dumps({
                    'document_id': row['document_id'],
                    'title': row['title'],
                    **result,
                }, ensure_ascii=False))
            print('OpenAI test classifications:', len(rows))
        elif args.command == 'classify-openai':
            classify_openai_all(db)
        elif args.command == 'classify-openai-pending':
            rows = db.execute(
                "SELECT document_id,title,description FROM news_articles "
                "WHERE filter_status='categorized' AND (categories IS NULL OR jsonb_array_length(categories)=0) "
                "ORDER BY id"
            ).fetchall()
            from common.openai_classifier import openai_categorize
            with db.transaction():
                for row in rows:
                    article = openai_categorize({'title': row['title'], 'content': row['description'] or ''})
                    db.execute(
                        "UPDATE news_articles SET categories=%s, category_candidates='[]'::jsonb, "
                        "filter_status='categorized', updated_at=CURRENT_TIMESTAMP WHERE document_id=%s",
                        (Jsonb(article['categories']), row['document_id']),
                    )
            print('OpenAI pending classifications:', len(rows))
        elif args.command in ('embed-news-test', 'embed-news'):
            from common.embedding import device_info, ensure_schema, eligible_rows, load_model, embed_rows, model_name
            model, device = load_model()
            _, gpu = device_info()
            dimension = (model.get_embedding_dimension()
                         if hasattr(model, 'get_embedding_dimension')
                         else model.get_sentence_embedding_dimension())
            batch_size = 32 if args.command == 'embed-news-test' else args.batch_size
            print('model name:', model_name())
            print('device:', device)
            print('GPU name:', gpu)
            print('embedding dimension:', dimension)
            print('batch size:', batch_size)
            ensure_schema(db, dimension)
            rows = eligible_rows(db, args.limit if args.command == 'embed-news-test' else None, args.force)
            db.commit()
            before = {row['document_id']: row for row in rows}
            embed_rows(db, model, rows, batch_size=batch_size)
            if args.command == 'embed-news-test':
                for row in db.execute(
                    "SELECT document_id,title,embedding_status,vector_dims(embedding) AS dimension, "
                    "sqrt((embedding <#> embedding) * -1) AS norm "
                    "FROM news_articles WHERE document_id = ANY(%s)",
                    (list(before),),
                ).fetchall():
                    print(json.dumps(dict(row), ensure_ascii=False, default=str))
        elif args.command == 'search-news':
            from common.embedding import device_info, load_model, model_name, search
            model, device = load_model()
            print('model name:', model_name())
            print('device:', device)
            print('GPU name:', device_info()[1])
            for row in search(db, model, args.query, args.limit):
                print(json.dumps(dict(row), ensure_ascii=False, default=str))
        elif args.command in ('embed-news-nemotron-test', 'embed-news-nemotron'):
            from common.nemotron_embedding import (
                DIMENSION, MODEL_NAME, embed_rows, embed_texts, ensure_schema,
                eligible_rows, text_for_article,
            )
            ensure_schema(db)
            if args.command == 'embed-news-nemotron-test':
                from common.nemotron_embedding import split_embedding_rows
                rows = eligible_rows(db, force=True, limit=5)
                valid_rows, empty_rows = split_embedding_rows(rows)
                if not valid_rows:
                    raise RuntimeError('No non-empty categorized articles available for test')
                vectors = embed_texts([text_for_article(row) for row in valid_rows], 'passage')
                print('Nemotron API test: success')
                print('model name:', MODEL_NAME)
                print('articles:', len(valid_rows))
                print('skipped empty articles:', len(empty_rows))
                print('vector dimension:', len(vectors[0]) if vectors else 0)
                if vectors and len(vectors[0]) != DIMENSION:
                    raise RuntimeError(f'Unexpected vector dimension: {len(vectors[0])}')
            else:
                if args.batch_size < 1:
                    parser.error('batch-size must be >= 1')
                rows = eligible_rows(db, force=args.force)
                print('model name:', MODEL_NAME)
                print('articles:', len(rows))
                embed_rows(db, rows, batch_size=args.batch_size)
        elif args.command == 'search-news-nemotron':
            from common.nemotron_embedding import search
            if args.top_k < 1:
                parser.error('top-k must be >= 1')
            for row in search(db, args.query, args.top_k):
                print(json.dumps(dict(row), ensure_ascii=False, default=str))
        elif args.command == 'embed-excluded-nemotron':
            from common.nemotron_embedding import MODEL_NAME, embed_rows, eligible_rows, ensure_schema
            if args.batch_size < 1:
                parser.error('batch-size must be >= 1')
            ensure_schema(db)
            rows = eligible_rows(db, filter_status='excluded_stage1')
            print('model name:', MODEL_NAME)
            print('excluded articles:', len(rows))
            embed_rows(db, rows, batch_size=args.batch_size)
        elif args.command in ('embed-legal-nemotron-test', 'embed-legal-nemotron'):
            from common.legal_nemotron import (
                DIMENSION, MODEL_NAME, connect_legal, eligible_rows,
                embed_rows, ensure_schema, test_rows,
            )
            legal_db = connect_legal()
            try:
                ensure_schema(legal_db)
                if args.command == 'embed-legal-nemotron-test':
                    rows = test_rows(legal_db, 5)
                    if not rows:
                        raise RuntimeError('No non-empty legal chunks available for test')
                    from common.nemotron_embedding import embed_texts
                    vectors = embed_texts([row['embedding_text'].strip() for row in rows], 'passage')
                    print('Legal Nemotron API test: success')
                    print('model name:', MODEL_NAME)
                    print('chunks:', len(rows))
                    print('vector dimension:', len(vectors[0]) if vectors else 0)
                    if vectors and len(vectors[0]) != DIMENSION:
                        raise RuntimeError(f'Unexpected vector dimension: {len(vectors[0])}')
                else:
                    if args.batch_size < 1:
                        parser.error('batch-size must be >= 1')
                    rows = eligible_rows(legal_db)
                    print('model name:', MODEL_NAME)
                    print('legal chunks:', len(rows))
                    completed, skipped, failed = embed_rows(legal_db, rows, args.batch_size)
                    print(f'completed={completed} skipped={skipped} failed={failed}')
            finally:
                legal_db.close()
        elif args.command == 'link-news-to-legal':
            import numpy as np
            from common.legal_nemotron import (
                MODEL_NAME, connect_legal, ensure_relation_schema,
                link_news, load_legal_vectors,
            )
            if args.top_k < 1 or (args.limit is not None and args.limit < 1):
                parser.error('top-k and limit must be >= 1')
            ensure_relation_schema(db)
            legal_db = connect_legal()
            try:
                legal_rows, legal_vectors, legal_norms = load_legal_vectors(legal_db)
            finally:
                legal_db.close()
            total, created, updated, failed, scores = link_news(
                db, legal_rows, legal_vectors, legal_norms, args.top_k,
                args.min_score, args.limit, args.force,
            )
            print('NEWS TOTAL:', total)
            print('LEGAL CHUNKS:', len(legal_rows))
            print('RELATIONS CREATED:', created)
            print('RELATIONS UPDATED:', updated)
            print('FAILED:', failed)
            print('AVG TOP1 SCORE:', float(np.mean(scores)) if scores else None)
            print('P50 TOP1:', float(np.percentile(scores, 50)) if scores else None)
            print('P90 TOP1:', float(np.percentile(scores, 90)) if scores else None)
            print('P95 TOP1:', float(np.percentile(scores, 95)) if scores else None)
        elif args.command in ('recover-legal-semantic', 'analyze-legal-semantic-scores'):
            from pathlib import Path
            from common.nemotron_embedding import semantic_matches, semantic_score_distribution
            if args.command == 'analyze-legal-semantic-scores':
                stats, buckets = semantic_score_distribution(db)
                print(json.dumps(dict(stats), ensure_ascii=False, default=str))
                print(json.dumps(dict(buckets), ensure_ascii=False, default=str))
            else:
                if args.top_k < 1 or (args.limit is not None and args.limit < 1):
                    parser.error('top-k and limit must be >= 1')
                rows = semantic_matches(db, args.top_k, args.limit, args.min_score)
                candidates = []
                for row in rows:
                    matches = row['matched_legal_articles'] or []
                    counts = {}
                    for match in matches:
                        category = match.get('primary_category') or {}
                        name = category.get('name')
                        if name:
                            counts[name] = counts.get(name, 0) + 1
                    suggested = max(counts, key=counts.get) if counts else None
                    suggested_id = next(
                        (m.get('primary_category', {}).get('id') for m in matches
                         if m.get('primary_category', {}).get('name') == suggested),
                        None,
                    )
                    candidates.append({
                        'candidate_document_id': row['document_id'],
                        'candidate_title': row['title'],
                        'candidate_description': row['description'],
                        'candidate_filter_status': row['filter_status'],
                        'max_similarity': float(row['max_similarity']),
                        'avg_top_k_similarity': float(row['avg_top_k_similarity']),
                        'matched_legal_articles': matches,
                        'suggested_primary_category': suggested,
                        'suggested_primary_category_id': suggested_id,
                    })
                path = Path(args.output)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(candidates[:max(300, len(candidates))],
                                           ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
                print('Candidates:', len(candidates))
                print('Output:', path)
                for label, sample in (('TOP', candidates[:30]), ('LOW', candidates[-10:])):
                    print(label)
                    for candidate in sample:
                        print(json.dumps({
                            'title': candidate['candidate_title'],
                            'max_similarity': candidate['max_similarity'],
                            'suggested_primary_category': candidate['suggested_primary_category'],
                        }, ensure_ascii=False))
        elif args.command == 'sample':
            from common.store import save_raw
            sample = normalize_article({'title':'[테스트] 법원, 임대보증금 반환 판결',
                'content':'임차인의 보증금 반환청구 사건', 'content_type':'synthetic_test',
                'url':'https://example.invalid/test-rss-001', 'source':'법률신문',
                'is_test':True, 'published_at':None, 'collected_at':now()})
            save_raw(db, sample)
            print('Synthetic test record saved. Excluded from regular exports.')
        elif args.command == 'requeue':
            version = digest(encode(config))
            with db.transaction():
                rows = db.execute(
                    'SELECT document_id,event_id,title,description,source,url,published_at,collected_at,'
                    'legal_keywords,categories,category_candidates,article_types,filter_status,filter_version '
                    'FROM news_articles ORDER BY id'
                ).fetchall()
                for row in rows:
                    a = _article_from_row(row)
                    a['event_id'] = digest(a['event_id'] + '|filter-replay|' + version)
                    a['reprocessing_version'] = version
                    db.execute(
                        'INSERT INTO kafka_outbox '
                        '(event_id, document_id, topic, payload, sent_at) '
                        'VALUES (%s, %s, %s, %s, NULL) '
                        'ON CONFLICT (event_id) DO NOTHING',
                        (a['event_id'], a['document_id'], 'raw-news', Jsonb(a)),
                    )
            print('Reprocessing queued for current config. Restart consumers to load new config, then publish.')
        elif args.command == 'replay-failure':
            import base64
            from pathlib import Path
            from kafka.producer import send_message
            from kafka.config import TOPIC_RAW, TOPIC_LEGAL, TOPIC_CATEGORY
            row = db.execute(
                'SELECT payload FROM news_processing_failures WHERE failure_id=%s',
                (args.failure_id,),
            ).fetchone()
            if row is None:
                raise ValueError('Unknown failure_id')
            failure = row['payload']
            a = json.loads(Path(args.corrected_file).read_text(encoding='utf-8-sig')) if args.corrected_file else json.loads(base64.b64decode(failure['raw_base64']))
            a = normalize_article(a)
            topic = failure['input_topic']
            if topic not in (TOPIC_RAW,TOPIC_LEGAL,TOPIC_CATEGORY):
                raise ValueError('Unexpected replay topic')
            send_message(topic,a['document_id'],a)
            print('Replayed once:', args.failure_id)
        elif args.command == 'stats':
            for p in config['publishers']:
                count = db.execute('SELECT count(*) FROM news_articles WHERE source=%s',
                                   (p['name'],)).fetchone()['count']
                print(p['name'], count)
            print('Kafka pending:', db.execute(
                'SELECT count(*) FROM kafka_outbox WHERE sent_at IS NULL').fetchone()['count'])
            print('Final status:', db.execute(
                'SELECT filter_status, count(*) FROM news_articles GROUP BY filter_status').fetchall())
            print('Failures:', db.execute(
                'SELECT count(*) FROM news_processing_failures').fetchone()['count'])
        elif args.command == 'export':
            directory = DATA / 'exports'
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / (datetime.now().strftime('%Y%m%d_%H%M%S_%f') + '.jsonl')
            count = 0
            with path.open('w',encoding='utf-8') as out:
                rows = db.execute(
                    'SELECT document_id,event_id,title,description,source,url,published_at,collected_at,'
                    'legal_keywords,categories,category_candidates,article_types,filter_status,filter_version '
                    'FROM news_articles ORDER BY id'
                ).fetchall()
                for row in rows:
                    a = _article_from_row(row)
                    if a.get('is_test'):
                        continue
                    if not args.raw and a.get('filter_status') != 'categorized' and not (args.include_unclassified and a.get('filter_status') == 'unclassified'):
                        continue
                    out.write(encode(a) + '\n')
                    count += 1
            print('Export:',path,'records:',count)
    except KeyboardInterrupt:
        print('Stopped. Saved records and pending messages retained.')
    finally:
        db.close()


if __name__ == '__main__':
    main()
