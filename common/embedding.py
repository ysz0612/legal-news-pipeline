import os

DEFAULT_MODEL = 'BAAI/bge-m3'
LEGAL_STATUS = 'categorized'


def model_name():
    return os.environ.get('EMBEDDING_MODEL', DEFAULT_MODEL)


def device_info():
    import torch
    requested = os.environ.get('EMBEDDING_DEVICE', '').strip().lower()
    if requested in {'cpu', 'cuda'}:
        if requested == 'cuda' and not torch.cuda.is_available():
            raise RuntimeError('EMBEDDING_DEVICE=cuda was requested but CUDA is unavailable')
        device = requested
        gpu = torch.cuda.get_device_name(0) if device == 'cuda' else 'N/A'
        return device, gpu
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    gpu = torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'N/A'
    return device, gpu


def load_model():
    from sentence_transformers import SentenceTransformer
    import torch
    device, _ = device_info()
    try:
        model = SentenceTransformer(model_name(), device=device)
    except torch.cuda.OutOfMemoryError:
        if device != 'cuda':
            raise
        torch.cuda.empty_cache()
        print('CUDA memory is insufficient for BGE-M3; falling back to CPU.', flush=True)
        model = SentenceTransformer(model_name(), device='cpu')
        device = 'cpu'
    return model, device


def ensure_schema(db, dimension):
    dimension = int(dimension)
    with db.transaction():
        db.execute('CREATE EXTENSION IF NOT EXISTS vector')
        db.execute('ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS embedding vector(%s)' % dimension)
        db.execute('ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS embedding_model TEXT')
        db.execute('ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS embedding_status TEXT')
        db.execute('ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS embedding_error TEXT')
        db.execute('ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ')


def text_for_article(row):
    title = row['title'] or ''
    description = row['description'] or ''
    return f'제목: {title}\n내용: {description}' if description else f'제목: {title}'


def vector_literal(values):
    return '[' + ','.join(format(float(value), '.9g') for value in values) + ']'


def embed_rows(db, model, rows, batch_size=32, commit_size=100, force=False):
    total = len(rows)
    done = 0
    for start in range(0, total, commit_size):
        chunk = rows[start:start + commit_size]
        pending = []
        for batch_start in range(0, len(chunk), batch_size):
            batch = chunk[batch_start:batch_start + batch_size]
            texts = [text_for_article(row) for row in batch]
            current_batch = batch_size
            error_message = 'embedding failed'
            while True:
                try:
                    vectors = model.encode(texts, batch_size=current_batch, normalize_embeddings=False,
                                           convert_to_numpy=True, show_progress_bar=False)
                    break
                except RuntimeError as exc:
                    error_message = str(exc)[:1000]
                    message = str(exc).lower()
                    if 'out of memory' not in message or current_batch <= 8:
                        vectors = None
                        break
                    current_batch //= 2
                    if hasattr(model, 'device') and str(model.device).startswith('cuda'):
                        import torch
                        torch.cuda.empty_cache()
                except Exception as exc:
                    error_message = str(exc)[:1000]
                    vectors = None
                    break
            if vectors is None:
                for row in batch:
                    pending.append(('failed', row['document_id'], error_message))
            else:
                for row, vector in zip(batch, vectors):
                    pending.append(('completed', row['document_id'], vector_literal(vector)))
        with db.transaction():
            for status, document_id, value in pending:
                if status == 'completed':
                    db.execute(
                        "UPDATE news_articles SET embedding=%s::vector, embedding_model=%s, "
                        "embedding_status='COMPLETED', embedding_error=NULL, embedded_at=CURRENT_TIMESTAMP "
                        "WHERE document_id=%s",
                        (value, model_name(), document_id),
                    )
                    done += 1
                else:
                    db.execute(
                        "UPDATE news_articles SET embedding_status='FAILED', embedding_error=%s "
                        "WHERE document_id=%s",
                        (value, document_id),
                    )
        print(f'Embedded {min(start + len(chunk), total)}/{total}', flush=True)
    return done


def eligible_rows(db, limit=None, force=False):
    sql = (
        "SELECT document_id,title,description FROM news_articles "
        "WHERE filter_status=%s "
    )
    params = [LEGAL_STATUS]
    if not force:
        # Never re-embed an already completed article unless explicitly forced.
        sql += "AND embedding_status IS DISTINCT FROM 'COMPLETED' "
    sql += 'ORDER BY id'
    if limit is not None:
        sql += ' LIMIT %s'
        params.append(limit)
    return db.execute(sql, params).fetchall()


def search(db, model, query, limit=5):
    vector = model.encode([query], normalize_embeddings=False, convert_to_numpy=True, show_progress_bar=False)[0]
    literal = vector_literal(vector)
    return db.execute(
        "SELECT document_id,title,source,categories->0->>'category' AS primary_category, "
        "1 - (embedding <=> %s::vector) AS similarity "
        "FROM news_articles WHERE filter_status=%s AND embedding IS NOT NULL "
        "ORDER BY embedding <=> %s::vector LIMIT %s",
        (literal, LEGAL_STATUS, literal, limit),
    ).fetchall()
