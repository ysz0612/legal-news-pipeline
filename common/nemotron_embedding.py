import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


MODEL_NAME = 'nvidia/nemotron-3-embed-1b'
API_URL = 'https://integrate.api.nvidia.com/v1/embeddings'
DIMENSION = 2048
DEFAULT_TIMEOUT = 60
MAX_RETRIES = 4


def ensure_schema(db):
    with db.transaction():
        db.execute('ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS nemotron_embedding vector(2048)')
        db.execute('ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS nemotron_embedding_model TEXT')
        db.execute('ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS nemotron_embedding_status TEXT')
        db.execute('ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS nemotron_embedding_error TEXT')
        db.execute('ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS nemotron_embedded_at TIMESTAMPTZ')


def _api_key():
    key = os.environ.get('NVIDIA_API_KEY', '').strip()
    if not key:
        raise RuntimeError('NVIDIA_API_KEY is not set')
    return key


def text_for_article(row):
    title = (row.get('title') or '').strip()
    description = (row.get('description') or '').strip()
    parts = [value for value in (title, description) if value]
    return '\n'.join(parts).strip()


def split_embedding_rows(rows):
    valid = []
    empty = []
    for row in rows:
        if text_for_article(row):
            valid.append(row)
        else:
            empty.append(row)
    return valid, empty


def vector_literal(values):
    return '[' + ','.join(format(float(value), '.9g') for value in values) + ']'


def _request(payload, timeout=DEFAULT_TIMEOUT):
    body = json.dumps(payload).encode('utf-8')
    request = Request(
        API_URL,
        data=body,
        headers={
            'Authorization': f'Bearer {_api_key()}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        },
        method='POST',
    )
    for attempt in range(MAX_RETRIES + 1):
        try:
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode('utf-8'))
        except HTTPError as exc:
            retryable = exc.code == 429 or 500 <= exc.code <= 599
            response_body = exc.read().decode('utf-8', 'replace')[:900]
            try:
                details = json.loads(response_body)
                response_body = details.get('detail') or details.get('message') or response_body
            except (TypeError, ValueError):
                pass
            if not retryable or attempt >= MAX_RETRIES:
                raise RuntimeError(
                    f'NVIDIA embeddings API HTTP {exc.code}: {response_body}'
                ) from None
        except (URLError, TimeoutError, OSError) as exc:
            if attempt >= MAX_RETRIES:
                raise RuntimeError(f'NVIDIA embeddings API connection failed: {type(exc).__name__}') from None
        time.sleep(2 ** attempt)


def embed_texts(texts, input_type, timeout=DEFAULT_TIMEOUT):
    if input_type not in {'passage', 'query'}:
        raise ValueError('input_type must be passage or query')
    if not texts or any(not isinstance(text, str) or not text.strip() for text in texts):
        raise ValueError('empty embedding text')
    payload = {
        'model': MODEL_NAME,
        'input': texts,
        'input_type': input_type,
        'encoding_format': 'float',
        'truncate': 'END',
    }
    response = _request(payload, timeout=timeout)
    data = response.get('data')
    if not isinstance(data, list) or len(data) != len(texts):
        raise RuntimeError('NVIDIA embeddings API returned an invalid item count')
    try:
        data = sorted(data, key=lambda item: item['index'])
        vectors = [item['embedding'] for item in data]
    except (KeyError, TypeError) as exc:
        raise RuntimeError('NVIDIA embeddings API returned an invalid response') from exc
    if any(not isinstance(vector, list) or len(vector) != DIMENSION for vector in vectors):
        raise RuntimeError(f'Nemotron vector dimension must be {DIMENSION}')
    return vectors


def eligible_rows(db, force=False, limit=None, filter_status='categorized'):
    sql = (
        'SELECT document_id,title,description FROM news_articles '
        'WHERE filter_status=%s '
    )
    params = [filter_status]
    if not force:
        sql += "AND nemotron_embedding_status IS DISTINCT FROM 'COMPLETED' "
    sql += 'ORDER BY id'
    if limit is not None:
        sql += ' LIMIT %s'
        params.append(limit)
    return db.execute(sql, params).fetchall()


def semantic_matches(db, top_k=5, limit=None, min_score=None):
    params = []
    limit_sql = ''
    if limit is not None:
        limit_sql = 'LIMIT %s'
        params.append(limit)
    params.append(top_k)
    rows = db.execute(
        "WITH excluded AS ("
        "SELECT document_id,title,description,filter_status,nemotron_embedding "
        "FROM news_articles "
        "WHERE filter_status='excluded_stage1' "
        "AND nemotron_embedding IS NOT NULL "
        "ORDER BY id " + limit_sql + ") "
        "SELECT e.document_id,e.title,e.description,e.filter_status,"
        "max(m.similarity) AS max_similarity,avg(m.similarity) AS avg_top_k_similarity,"
        "jsonb_agg(jsonb_build_object("
        "'document_id',m.document_id,'title',m.title,'similarity',m.similarity,"
        "'primary_category',jsonb_build_object("
        "'id',coalesce(m.categories->0->>'id',m.categories->0->>'category_id'),"
        "'name',coalesce(m.categories->0->>'name',m.categories->0->>'category'))"
        ") ORDER BY m.similarity DESC) AS matched_legal_articles "
        "FROM excluded e CROSS JOIN LATERAL ("
        "SELECT r.document_id,r.title,r.categories,"
        "1-(r.nemotron_embedding <=> e.nemotron_embedding) AS similarity "
        "FROM news_articles r WHERE r.filter_status='categorized' "
        "AND r.nemotron_embedding IS NOT NULL "
        "ORDER BY r.nemotron_embedding <=> e.nemotron_embedding LIMIT %s"
        ") m GROUP BY e.document_id,e.title,e.description,e.filter_status "
        "HAVING max(m.similarity) >= coalesce(%s, -1) "
        "ORDER BY max(m.similarity) DESC",
        params + [min_score],
    ).fetchall()
    return rows


def semantic_score_distribution(db):
    return db.execute(
        "WITH scores AS ("
        "SELECT 1-(r.nemotron_embedding <=> e.nemotron_embedding) AS score "
        "FROM news_articles e CROSS JOIN LATERAL ("
        "SELECT nemotron_embedding FROM news_articles r "
        "WHERE r.filter_status='categorized' AND r.nemotron_embedding IS NOT NULL "
        "ORDER BY r.nemotron_embedding <=> e.nemotron_embedding LIMIT 1"
        ") r WHERE e.filter_status='excluded_stage1' "
        "AND e.nemotron_embedding IS NOT NULL) "
        "SELECT count(*) AS count,min(score) AS min,max(score) AS max,"
        "percentile_cont(0.10) WITHIN GROUP (ORDER BY score) AS p10,"
        "percentile_cont(0.25) WITHIN GROUP (ORDER BY score) AS p25,"
        "percentile_cont(0.50) WITHIN GROUP (ORDER BY score) AS p50,"
        "percentile_cont(0.75) WITHIN GROUP (ORDER BY score) AS p75,"
        "percentile_cont(0.90) WITHIN GROUP (ORDER BY score) AS p90,"
        "percentile_cont(0.95) WITHIN GROUP (ORDER BY score) AS p95,"
        "percentile_cont(0.97) WITHIN GROUP (ORDER BY score) AS p97,"
        "percentile_cont(0.99) WITHIN GROUP (ORDER BY score) AS p99 FROM scores"
    ).fetchone(), db.execute(
        "WITH scores AS ("
        "SELECT 1-(r.nemotron_embedding <=> e.nemotron_embedding) AS score "
        "FROM news_articles e CROSS JOIN LATERAL ("
        "SELECT nemotron_embedding FROM news_articles r "
        "WHERE r.filter_status='categorized' AND r.nemotron_embedding IS NOT NULL "
        "ORDER BY r.nemotron_embedding <=> e.nemotron_embedding LIMIT 1"
        ") r WHERE e.filter_status='excluded_stage1' "
        "AND e.nemotron_embedding IS NOT NULL) "
        "SELECT count(*) FILTER (WHERE score < 0.50) AS below_050,"
        "count(*) FILTER (WHERE score >= 0.50 AND score < 0.60) AS s050_060,"
        "count(*) FILTER (WHERE score >= 0.60 AND score < 0.70) AS s060_070,"
        "count(*) FILTER (WHERE score >= 0.70 AND score < 0.75) AS s070_075,"
        "count(*) FILTER (WHERE score >= 0.75 AND score < 0.80) AS s075_080,"
        "count(*) FILTER (WHERE score >= 0.80 AND score < 0.85) AS s080_085,"
        "count(*) FILTER (WHERE score >= 0.85 AND score < 0.90) AS s085_090,"
        "count(*) FILTER (WHERE score >= 0.90) AS at_least_090 FROM scores"
    ).fetchone()


def embed_rows(db, rows, batch_size=16, timeout=DEFAULT_TIMEOUT):
    completed = 0
    failed = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        valid_batch, empty_batch = split_embedding_rows(batch)
        if empty_batch:
            with db.transaction():
                for row in empty_batch:
                    db.execute(
                        "UPDATE news_articles SET nemotron_embedding_status='FAILED', "
                        "nemotron_embedding_error='empty embedding text' WHERE document_id=%s",
                        (row['document_id'],),
                    )
            failed += len(empty_batch)
        if not valid_batch:
            db.commit()
            print(f'Nemotron batch {min(start + len(batch), len(rows))}/{len(rows)} '
                  f'completed={completed} failed={failed}', flush=True)
            continue
        try:
            vectors = embed_texts([text_for_article(row) for row in valid_batch], 'passage', timeout)
            with db.transaction():
                for row, vector in zip(valid_batch, vectors):
                    db.execute(
                        "UPDATE news_articles SET nemotron_embedding=%s::vector, "
                        "nemotron_embedding_model=%s, nemotron_embedding_status='COMPLETED', "
                        "nemotron_embedding_error=NULL, nemotron_embedded_at=CURRENT_TIMESTAMP "
                        'WHERE document_id=%s',
                        (vector_literal(vector), MODEL_NAME, row['document_id']),
                    )
            completed += len(batch)
        except Exception as exc:
            error = str(exc)[:1000]
            with db.transaction():
                for row in batch:
                    db.execute(
                        "UPDATE news_articles SET nemotron_embedding_status='FAILED', "
                        'nemotron_embedding_error=%s WHERE document_id=%s',
                        (error, row['document_id']),
                    )
            failed += len(valid_batch)
        db.commit()
        print(f'Nemotron batch {min(start + len(batch), len(rows))}/{len(rows)} '
              f'completed={completed} failed={failed}', flush=True)
    return completed, failed


def search(db, query, top_k=5, timeout=DEFAULT_TIMEOUT):
    vector = embed_texts([query], 'query', timeout)[0]
    literal = vector_literal(vector)
    return db.execute(
        "SELECT document_id,title,description,"
        "categories->0->>'category' AS primary_category, "
        '1 - (nemotron_embedding <=> %s::vector) AS similarity '
        "FROM news_articles WHERE filter_status='categorized' "
        'AND nemotron_embedding IS NOT NULL '
        'ORDER BY nemotron_embedding <=> %s::vector LIMIT %s',
        (literal, literal, top_k),
    ).fetchall()
