import os

import numpy as np

from common.nemotron_embedding import (
    DIMENSION,
    MODEL_NAME,
    embed_texts,
    split_embedding_rows,
    text_for_article,
)


def _vector_values(value):
    if isinstance(value, str):
        return [float(item) for item in value.strip('[]').split(',') if item]
    return list(value)


def connect_legal():
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(
        host=os.getenv('LEGAL_POSTGRES_HOST', os.getenv('POSTGRES_HOST', 'localhost')),
        port=os.getenv('LEGAL_POSTGRES_PORT', os.getenv('POSTGRES_PORT', '5432')),
        dbname=os.getenv('LEGAL_POSTGRES_DB', 'legal_shared'),
        user=os.getenv('LEGAL_POSTGRES_USER', os.getenv('POSTGRES_USER', 'postgres')),
        password=os.getenv('LEGAL_POSTGRES_PASSWORD', os.getenv('POSTGRES_PASSWORD', '1234')),
        row_factory=dict_row,
    )


def ensure_schema(db):
    with db.transaction():
        db.execute('ALTER TABLE public.legal_chunks ADD COLUMN IF NOT EXISTS nemotron_embedding vector(2048)')
        db.execute('ALTER TABLE public.legal_chunks ADD COLUMN IF NOT EXISTS nemotron_embedding_model varchar(100)')
        db.execute('ALTER TABLE public.legal_chunks ADD COLUMN IF NOT EXISTS nemotron_embedding_status varchar(20)')
        db.execute('ALTER TABLE public.legal_chunks ADD COLUMN IF NOT EXISTS nemotron_embedding_error text')
        db.execute('ALTER TABLE public.legal_chunks ADD COLUMN IF NOT EXISTS nemotron_embedded_at timestamptz')


def eligible_rows(db, force=False, limit=None):
    sql = (
        'SELECT id,document_id,embedding_text FROM public.legal_chunks '
        'WHERE nemotron_embedding_status IS DISTINCT FROM %s '
    )
    params = ['COMPLETED']
    if force:
        sql = 'SELECT id,document_id,embedding_text FROM public.legal_chunks '
        sql += 'ORDER BY id'
    else:
        sql += 'ORDER BY id'
    if limit is not None:
        sql += ' LIMIT %s'
        params.append(limit)
    return db.execute(sql, params).fetchall()


def test_rows(db, limit=5):
    return db.execute(
        "SELECT id,document_id,embedding_text FROM public.legal_chunks "
        "WHERE btrim(coalesce(embedding_text,'')) <> '' ORDER BY id LIMIT %s",
        (limit,),
    ).fetchall()


def _text(row):
    return (row['embedding_text'] or '').strip()


def embed_rows(db, rows, batch_size=16, timeout=60):
    completed = failed = skipped = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        empty_rows = [row for row in batch if not _text(row)]
        if empty_rows:
            with db.transaction():
                for row in empty_rows:
                    db.execute(
                        "UPDATE public.legal_chunks SET nemotron_embedding_status='SKIPPED', "
                        "nemotron_embedding_error='empty embedding text' WHERE id=%s",
                        (row['id'],),
                    )
            skipped += len(empty_rows)
        valid_rows = [row for row in batch if _text(row)]
        if not valid_rows:
            db.commit()
            continue
        try:
            vectors = embed_texts([_text(row) for row in valid_rows], 'passage', timeout)
            with db.transaction():
                for row, vector in zip(valid_rows, vectors):
                    literal = '[' + ','.join(format(float(value), '.9g') for value in vector) + ']'
                    db.execute(
                        "UPDATE public.legal_chunks SET nemotron_embedding=%s::vector, "
                        "nemotron_embedding_model=%s,nemotron_embedding_status='COMPLETED', "
                        "nemotron_embedding_error=NULL,nemotron_embedded_at=CURRENT_TIMESTAMP "
                        'WHERE id=%s',
                        (literal, MODEL_NAME, row['id']),
                    )
            completed += len(valid_rows)
        except Exception as exc:
            error = str(exc)[:2000]
            with db.transaction():
                for row in valid_rows:
                    db.execute(
                        "UPDATE public.legal_chunks SET nemotron_embedding_status='FAILED', "
                        'nemotron_embedding_error=%s WHERE id=%s',
                        (error, row['id']),
                    )
            failed += len(valid_rows)
        db.commit()
        print(f'Legal Nemotron batch {min(start + len(batch), len(rows))}/{len(rows)} '
              f'completed={completed} skipped={skipped} failed={failed}', flush=True)
    return completed, skipped, failed


def load_legal_vectors(db):
    rows = db.execute(
        "SELECT id,document_id,content,nemotron_embedding "
        "FROM public.legal_chunks WHERE nemotron_embedding_status='COMPLETED' "
        'AND nemotron_embedding IS NOT NULL ORDER BY id'
    ).fetchall()
    vectors = np.asarray([_vector_values(row['nemotron_embedding']) for row in rows], dtype=np.float32)
    if len(rows) and (vectors.ndim != 2 or vectors.shape[1] != DIMENSION):
        raise RuntimeError(f'Legal Nemotron vectors must have dimension {DIMENSION}')
    norms = np.linalg.norm(vectors, axis=1) if len(rows) else np.empty(0, dtype=np.float32)
    return rows, vectors, norms


def ensure_relation_schema(db):
    with db.transaction():
        db.execute('''
            CREATE TABLE IF NOT EXISTS news_legal_relations (
                id BIGSERIAL PRIMARY KEY,
                news_document_id TEXT NOT NULL,
                legal_chunk_id BIGINT NOT NULL,
                legal_document_id BIGINT,
                similarity_score DOUBLE PRECISION NOT NULL,
                rank INTEGER NOT NULL,
                relation_type VARCHAR(50) NOT NULL DEFAULT 'SEMANTIC_SIMILARITY',
                embedding_model VARCHAR(100),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (news_document_id, legal_chunk_id, embedding_model)
            )
        ''')


def link_news(db, legal_rows, legal_vectors, legal_norms, top_k=5,
              min_score=None, limit=None, force=False, batch_size=64):
    if not len(legal_rows):
        raise RuntimeError('No completed legal Nemotron embeddings available')
    existing = set()
    if not force:
        existing = {
            row['news_document_id'] for row in db.execute(
                "SELECT DISTINCT news_document_id FROM news_legal_relations "
                'WHERE embedding_model=%s', (MODEL_NAME,)
            ).fetchall()
        }
    sql = (
        "SELECT document_id,title,description,nemotron_embedding FROM news_articles "
        "WHERE filter_status='categorized' AND nemotron_embedding IS NOT NULL "
        'ORDER BY id'
    )
    if limit is not None:
        sql += ' LIMIT %s'
        news_rows = db.execute(sql, (limit,)).fetchall()
    else:
        news_rows = db.execute(sql).fetchall()
    news_rows = [row for row in news_rows if force or row['document_id'] not in existing]
    created = updated = failed = 0
    top_scores = []
    for start in range(0, len(news_rows), batch_size):
        batch = news_rows[start:start + batch_size]
        try:
            matrix = np.asarray([_vector_values(row['nemotron_embedding']) for row in batch], dtype=np.float32)
            matrix /= np.maximum(np.linalg.norm(matrix, axis=1, keepdims=True), 1e-12)
            legal_matrix = legal_vectors / np.maximum(legal_norms[:, None], 1e-12)
            scores = matrix @ legal_matrix.T
            with db.transaction():
                for row, row_scores in zip(batch, scores):
                    indexes = np.argpartition(-row_scores, min(top_k, len(row_scores)) - 1)[:top_k]
                    indexes = indexes[np.argsort(-row_scores[indexes])]
                    selected = [(int(index), float(row_scores[index])) for index in indexes]
                    selected = [(index, score) for index, score in selected
                                if min_score is None or score >= min_score]
                    if selected:
                        top_scores.append(selected[0][1])
                    for rank, (index, score) in enumerate(selected, 1):
                        legal = legal_rows[index]
                        result = db.execute(
                            "INSERT INTO news_legal_relations "
                            "(news_document_id,legal_chunk_id,legal_document_id,similarity_score,rank,relation_type,embedding_model) "
                            "VALUES (%s,%s,%s,%s,%s,'SEMANTIC_SIMILARITY',%s) "
                            "ON CONFLICT (news_document_id,legal_chunk_id,embedding_model) DO UPDATE SET "
                            "similarity_score=EXCLUDED.similarity_score,rank=EXCLUDED.rank,created_at=NOW() "
                            'RETURNING (xmax = 0) AS inserted',
                            (row['document_id'], legal['id'], legal['document_id'], score, rank, MODEL_NAME),
                        ).fetchone()
                        if result['inserted']:
                            created += 1
                        else:
                            updated += 1
            db.commit()
        except Exception:
            failed += len(batch)
            db.rollback()
    return len(news_rows), created, updated, failed, top_scores
