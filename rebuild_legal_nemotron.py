from __future__ import annotations

import argparse
import hashlib
import os
import re
import statistics
import time
from typing import Iterable

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

from common.nemotron_embedding import DIMENSION, MODEL_NAME, embed_texts

BASE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE, ".env"))

PG = dict(
    host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
    port=int(os.getenv("POSTGRES_PORT", "55432")),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
STAGE_DB = "legal_rebuild_stage"
LEGAL_DB = "legal_shared"
NEWS_DB = os.getenv("POSTGRES_DB", "law_db")
MAX_CHARS = 3200
MIN_CHARS = 80
EMBED_BATCH = 16
RELATION_THRESHOLD = 0.70
DOC_THRESHOLD = 0.55


def conn(db: str):
    connection = psycopg.connect(dbname=db, row_factory=dict_row, **PG)
    connection.autocommit = True
    return connection


def normalize(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def split_long(text: str, max_chars: int = MAX_CHARS) -> list[str]:
    text = normalize(text)
    if len(text) <= max_chars:
        return [text] if text else []
    pieces: list[str] = []
    rest = text
    while len(rest) > max_chars:
        cut = max_chars
        for marker in (". ", "다. ", "; ", "。", "."):
            pos = rest.rfind(marker, MIN_CHARS, max_chars)
            if pos >= MIN_CHARS:
                cut = pos + len(marker)
                break
        pieces.append(rest[:cut].strip())
        rest = rest[cut:].strip()
    if rest:
        pieces.append(rest)
    return pieces


ARTICLE_RE = re.compile(r"(?m)^\s*(제\s*\d+조(?:의\s*\d+)?(?:\s*\([^\n]{0,240}\))?[^\n]*)")


def article_chunks(law_name: str, document_id: int, content: str) -> Iterable[dict]:
    text = content or ""
    raw_matches = list(ARTICLE_RE.finditer(text))
    matches = []
    by_article: dict[str, list] = {}
    for match in raw_matches:
        header = normalize(match.group(1))
        article_key_match = re.match(r"(\uC81C\s*\d+\uC870(?:\uC758\s*\d+)?)", header)
        if article_key_match:
            key = article_key_match.group(1).replace(" ", "")
            by_article.setdefault(key, []).append(match)
    for candidates in by_article.values():
        # The source contains standalone article labels, headings, and the
        # actual article line. Keep the longest candidate, preferring a line
        # with a parenthesized article title.
        chosen = max(
            candidates,
            key=lambda item: ("(" in normalize(item.group(1)), len(normalize(item.group(1)))),
        )
        matches.append(chosen)
    matches.sort(key=lambda item: item.start())
    if not matches:
        for index, piece in enumerate(split_long(text)):
            if piece:
                yield {
                    "article_no": None,
                    "article_title": None,
                    "content": piece,
                    "embedding_text": f"[{law_name}]\n{piece}",
                    "chunk_type": "BODY",
                    "chunk_index": index,
                }
        return
    for article_index, match in enumerate(matches):
        header = normalize(match.group(1))
        article_no_match = re.match(r"(제\s*\d+조(?:의\s*\d+)?)", header)
        article_no = article_no_match.group(1).replace(" ", "") if article_no_match else None
        title_match = re.search(r"\(([^()]*)\)", header)
        article_title = title_match.group(1).strip() if title_match else None
        start = match.start()
        end = matches[article_index + 1].start() if article_index + 1 < len(matches) else len(text)
        body = normalize(text[start:end])
        for part_index, piece in enumerate(split_long(body)):
            if len(piece) < MIN_CHARS:
                continue
            context = f"[{law_name}]\n{header}\n"
            yield {
                "article_no": article_no,
                "article_title": article_title,
                "content": piece,
                "embedding_text": context + piece,
                "chunk_type": "ARTICLE",
                "chunk_index": article_index * 1000 + part_index,
            }


def prepare() -> None:
    with conn(STAGE_DB) as source, conn(LEGAL_DB) as target:
        with target.transaction():
            target.execute("DROP TABLE IF EXISTS legal_chunks_v2 CASCADE")
            target.execute("DROP TABLE IF EXISTS legal_documents_v2 CASCADE")
            target.execute("""
                CREATE TABLE legal_documents_v2 (
                    id BIGINT PRIMARY KEY,
                    document_key VARCHAR(255) UNIQUE NOT NULL,
                    document_type VARCHAR(50), law_id VARCHAR(255),
                    law_name TEXT, title TEXT, source_name VARCHAR(255),
                    content_type VARCHAR(100), body_status VARCHAR(100),
                    content TEXT, category_ids TEXT[], category_names TEXT[],
                    effective_date DATE, promulgation_date DATE,
                    source_url TEXT, metadata JSONB
                )
            """)
            target.execute("""
                CREATE TABLE legal_chunks_v2 (
                    id BIGSERIAL PRIMARY KEY,
                    document_id BIGINT NOT NULL REFERENCES legal_documents_v2(id),
                    chunk_key VARCHAR(255) UNIQUE NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    chunk_type VARCHAR(50) NOT NULL,
                    article_no VARCHAR(80), article_title TEXT,
                    content TEXT NOT NULL, embedding_text TEXT NOT NULL,
                    nemotron_embedding vector(2048),
                    embedding_model VARCHAR(100),
                    embedding_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                    embedding_error TEXT,
                    embedded_at TIMESTAMPTZ,
                    UNIQUE(document_id, chunk_index)
                )
            """)
        docs = source.execute("""
            SELECT id,document_key,document_type,law_id,law_name,title,source_name,
                   content_type,body_status,content,category_ids,category_names,
                   effective_date,promulgation_date,source_url,source_payload
            FROM public.legal_documents ORDER BY id
        """).fetchall()
        total_chunks = 0
        lengths: list[int] = []
        with target.transaction():
            for doc in docs:
                target.execute("""
                    INSERT INTO legal_documents_v2
                    (id,document_key,document_type,law_id,law_name,title,source_name,
                     content_type,body_status,content,category_ids,category_names,
                     effective_date,promulgation_date,source_url,metadata)
                    VALUES (%(id)s,%(document_key)s,%(document_type)s,%(law_id)s,%(law_name)s,
                            %(title)s,%(source_name)s,%(content_type)s,%(body_status)s,
                            %(content)s,%(category_ids)s,%(category_names)s,%(effective_date)s,
                            %(promulgation_date)s,%(source_url)s,%(source_payload)s)
                """, doc)
                for item in article_chunks(doc["law_name"] or doc["title"] or "", doc["id"], doc["content"] or ""):
                    content = item["content"]
                    key = f"nemotron-v2:{doc['id']}:{item['chunk_index']}:{hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]}"
                    target.execute("""
                        INSERT INTO legal_chunks_v2
                        (document_id,chunk_key,chunk_index,chunk_type,article_no,article_title,content,embedding_text)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (doc["id"], key, item["chunk_index"], item["chunk_type"],
                          item["article_no"], item["article_title"], content, item["embedding_text"]))
                    total_chunks += 1
                    lengths.append(len(content))
        print(f"DOCUMENTS={len(docs)} CHUNKS={total_chunks}")
        print(f"MIN_CHARS={min(lengths) if lengths else 0} AVG_CHARS={statistics.mean(lengths) if lengths else 0:.1f} MEDIAN_CHARS={statistics.median(lengths) if lengths else 0} MAX_CHARS={max(lengths) if lengths else 0}")


def vector_literal(values: list[float]) -> str:
    return "[" + ",".join(format(float(x), ".9g") for x in values) + "]"


def embed() -> None:
    completed = failed = 0
    with conn(LEGAL_DB) as db:
        while True:
            rows = db.execute("""
                SELECT id,embedding_text FROM legal_chunks_v2
                WHERE embedding_status IN ('PENDING','FAILED') ORDER BY id LIMIT 256
            """).fetchall()
            if not rows:
                break
            for start in range(0, len(rows), EMBED_BATCH):
                batch = rows[start:start + EMBED_BATCH]
                last_error = None
                vectors = None
                for attempt in range(3):
                    try:
                        vectors = embed_texts([x["embedding_text"] for x in batch], "passage")
                        break
                    except Exception as exc:
                        last_error = exc
                        time.sleep(min(30, 2 ** attempt))
                try:
                    if vectors is None:
                        raise RuntimeError(str(last_error)[:2000])
                    with db.transaction():
                        for row, vector in zip(batch, vectors):
                            if len(vector) != DIMENSION:
                                raise RuntimeError(f"dimension={len(vector)}")
                            db.execute("""
                                UPDATE legal_chunks_v2
                                SET nemotron_embedding=%s::vector,embedding_model=%s,
                                    embedding_status='COMPLETED',embedding_error=NULL,embedded_at=NOW()
                                WHERE id=%s
                            """, (vector_literal(vector), MODEL_NAME, row["id"]))
                    completed += len(batch)
                except Exception as exc:
                    with db.transaction():
                        db.execute("""
                            UPDATE legal_chunks_v2 SET embedding_status='FAILED',embedding_error=%s
                            WHERE id = ANY(%s)
                        """, (str(exc)[:2000], [x["id"] for x in batch]))
                    failed += len(batch)
                print(f"EMBEDDED={completed} FAILED={failed}", flush=True)
    print(f"EMBED_COMPLETE={completed} EMBED_FAILED={failed}")


def verify_embeddings() -> None:
    with conn(LEGAL_DB) as db:
        row = db.execute("""
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE embedding_status='COMPLETED') AS completed,
                   count(*) FILTER (WHERE embedding_status='FAILED') AS failed,
                   count(*) FILTER (WHERE nemotron_embedding IS NULL) AS nulls,
                   count(*) FILTER (WHERE nemotron_embedding IS NOT NULL AND vector_dims(nemotron_embedding)<>2048) AS bad_dim
            FROM legal_chunks_v2
        """).fetchone()
        print(dict(row))


def prepare_document_embeddings() -> None:
    with conn(LEGAL_DB) as db:
        db.execute("""
            ALTER TABLE legal_documents_v2
            ADD COLUMN IF NOT EXISTS embedding_text TEXT,
            ADD COLUMN IF NOT EXISTS nemotron_embedding vector(2048),
            ADD COLUMN IF NOT EXISTS embedding_model VARCHAR(100),
            ADD COLUMN IF NOT EXISTS embedding_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
            ADD COLUMN IF NOT EXISTS embedding_error TEXT,
            ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ
        """)
        db.execute("""
            UPDATE legal_documents_v2 d
            SET embedding_text = left(
                '[' || coalesce(d.law_name,d.title,'') || '] ' ||
                coalesce(d.document_type,'') || E'\\n' ||
                coalesce((SELECT string_agg(x.line,E'\\n' ORDER BY x.chunk_index)
                          FROM (SELECT c.chunk_index,
                                       coalesce(c.article_no,'') || ' ' ||
                                       coalesce(c.article_title,'') || ' ' ||
                                       left(c.content,240) AS line
                                FROM legal_chunks_v2 c
                                WHERE c.document_id=d.id
                                ORDER BY c.chunk_index LIMIT 24) x),'')
            , 9000),
                embedding_status='PENDING'
            WHERE embedding_text IS NULL
        """)
        row = db.execute("""
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE embedding_text IS NOT NULL AND btrim(embedding_text)<>'') AS ready
            FROM legal_documents_v2
        """).fetchone()
        print(dict(row))


def embed_documents() -> None:
    completed = failed = 0
    with conn(LEGAL_DB) as db:
        while True:
            rows = db.execute("""
                SELECT id,embedding_text FROM legal_documents_v2
                WHERE embedding_status IN ('PENDING','FAILED')
                  AND embedding_text IS NOT NULL AND btrim(embedding_text)<>''
                ORDER BY id LIMIT 256
            """).fetchall()
            if not rows:
                break
            for start in range(0, len(rows), EMBED_BATCH):
                batch = rows[start:start + EMBED_BATCH]
                vectors = None
                last_error = None
                for attempt in range(3):
                    try:
                        vectors = embed_texts([x['embedding_text'] for x in batch], 'passage')
                        break
                    except Exception as exc:
                        last_error = exc
                        time.sleep(min(30, 2 ** attempt))
                try:
                    if vectors is None:
                        raise RuntimeError(str(last_error)[:2000])
                    with db.transaction():
                        for row, vector in zip(batch, vectors):
                            if len(vector) != DIMENSION:
                                raise RuntimeError(f'dimension={len(vector)}')
                            db.execute("""
                                UPDATE legal_documents_v2
                                SET nemotron_embedding=%s::vector,embedding_model=%s,
                                    embedding_status='COMPLETED',embedding_error=NULL,embedded_at=NOW()
                                WHERE id=%s
                            """, (vector_literal(vector), MODEL_NAME, row['id']))
                    completed += len(batch)
                except Exception as exc:
                    with db.transaction():
                        db.execute("""
                            UPDATE legal_documents_v2
                            SET embedding_status='FAILED',embedding_error=%s
                            WHERE id = ANY(%s)
                        """, (str(exc)[:2000], [x['id'] for x in batch]))
                    failed += len(batch)
                print(f'DOC_EMBEDDED={completed} DOC_FAILED={failed}', flush=True)
    print(f'DOC_EMBED_COMPLETE={completed} DOC_EMBED_FAILED={failed}')


def create_document_index() -> None:
    with conn(LEGAL_DB) as db:
        db.execute("""
            CREATE INDEX IF NOT EXISTS legal_documents_v2_nemotron_hnsw_idx
            ON legal_documents_v2
            USING hnsw ((nemotron_embedding::halfvec(2048)) halfvec_cosine_ops)
            WHERE nemotron_embedding IS NOT NULL
        """)
        row = db.execute("""
            SELECT indexname,indexdef FROM pg_indexes
            WHERE schemaname='public' AND indexname='legal_documents_v2_nemotron_hnsw_idx'
        """).fetchone()
        print(dict(row) if row else 'DOCUMENT_INDEX_NOT_FOUND')


def build_document_candidates(threshold: float = DOC_THRESHOLD) -> None:
    with conn(NEWS_DB) as news, conn(LEGAL_DB) as legal:
        news.execute("DROP TABLE IF EXISTS news_legal_document_candidates_v2")
        news.execute("""
            CREATE TABLE news_legal_document_candidates_v2 (
                news_document_id TEXT NOT NULL,
                legal_document_id BIGINT NOT NULL,
                similarity_score DOUBLE PRECISION NOT NULL,
                rank INTEGER NOT NULL,
                threshold DOUBLE PRECISION NOT NULL,
                embedding_model VARCHAR(100) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY(news_document_id,legal_document_id)
            )
        """)
        rows = news.execute("""
            SELECT document_id,nemotron_embedding FROM news_articles
            WHERE nemotron_embedding IS NOT NULL AND vector_dims(nemotron_embedding)=2048
            ORDER BY id
        """).fetchall()
        created = 0
        for number, row in enumerate(rows, 1):
            candidates = legal.execute("""
                SELECT id,1-(nemotron_embedding <=> %s::vector) AS similarity
                FROM legal_documents_v2
                WHERE nemotron_embedding IS NOT NULL
                ORDER BY (nemotron_embedding::halfvec(2048)) <=> (%s::halfvec(2048))
                LIMIT 20
            """, (row['nemotron_embedding'], row['nemotron_embedding'])).fetchall()
            selected = [x for x in candidates if float(x['similarity']) >= threshold][:5]
            with news.transaction():
                for rank, item in enumerate(selected, 1):
                    news.execute("""
                        INSERT INTO news_legal_document_candidates_v2
                        (news_document_id,legal_document_id,similarity_score,rank,threshold,embedding_model)
                        VALUES (%s,%s,%s,%s,%s,%s)
                    """, (row['document_id'],item['id'],item['similarity'],rank,threshold,MODEL_NAME))
                    created += 1
            if number % 100 == 0:
                print(f'DOC_CANDIDATE_ARTICLES={number} CREATED={created}', flush=True)
    print(f'DOC_CANDIDATES_COMPLETE ARTICLES={len(rows)} CREATED={created} THRESHOLD={threshold}')


def prepare_needed_chunks() -> None:
    with conn(NEWS_DB) as news, conn(LEGAL_DB) as legal:
        ids = [x['legal_document_id'] for x in news.execute(
            'SELECT DISTINCT legal_document_id FROM news_legal_document_candidates_v2'
        ).fetchall()]
        if not ids:
            raise RuntimeError('No document candidates available')
        legal.execute("""
            UPDATE legal_chunks_v2 SET embedding_status='PENDING'
            WHERE document_id = ANY(%s) AND embedding_status='PAUSED'
        """, (ids,))
        row = legal.execute("""
            SELECT count(*) AS selected,
                   count(*) FILTER (WHERE embedding_status='COMPLETED') AS completed,
                   count(*) FILTER (WHERE embedding_status='PENDING') AS pending
            FROM legal_chunks_v2 WHERE document_id = ANY(%s)
        """, (ids,)).fetchone()
        print(f'UNIQUE_DOCUMENT_CANDIDATES={len(ids)}')
        print(dict(row))


def create_vector_index() -> None:
    with conn(LEGAL_DB) as db:
        db.execute("""
            CREATE INDEX IF NOT EXISTS legal_chunks_v2_nemotron_hnsw_idx
            ON legal_chunks_v2
            USING hnsw ((nemotron_embedding::halfvec(2048)) halfvec_cosine_ops)
            WHERE nemotron_embedding IS NOT NULL
        """)
        row = db.execute("""
            SELECT indexname,indexdef FROM pg_indexes
            WHERE schemaname='public' AND indexname='legal_chunks_v2_nemotron_hnsw_idx'
        """).fetchone()
        print(dict(row) if row else "INDEX_NOT_FOUND")


def create_relation_stage() -> None:
    with conn(NEWS_DB) as db:
        db.execute("DROP TABLE IF EXISTS news_legal_relations_v2")
        db.execute("""
            CREATE TABLE news_legal_relations_v2 (
                id BIGSERIAL PRIMARY KEY,
                news_document_id TEXT NOT NULL,
                legal_chunk_id BIGINT NOT NULL,
                legal_document_id BIGINT NOT NULL,
                similarity_score DOUBLE PRECISION NOT NULL,
                rank INTEGER NOT NULL CHECK (rank BETWEEN 1 AND 5),
                relation_type VARCHAR(50) NOT NULL DEFAULT 'SEMANTIC_SIMILARITY',
                embedding_model VARCHAR(100) NOT NULL,
                embedding_dimension INTEGER NOT NULL,
                relation_version VARCHAR(80) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(news_document_id, legal_chunk_id, relation_version)
            )
        """)
        db.execute("CREATE INDEX news_legal_relations_v2_news_idx ON news_legal_relations_v2(news_document_id,rank)")


def build_relations(threshold: float = RELATION_THRESHOLD) -> None:
    create_relation_stage()
    version = f"nemotron-v2-{time.strftime('%Y%m%d_%H%M%S')}"
    with conn(NEWS_DB) as news, conn(LEGAL_DB) as legal:
        rows = news.execute("""
            SELECT document_id,nemotron_embedding
            FROM news_articles
            WHERE nemotron_embedding IS NOT NULL
              AND vector_dims(nemotron_embedding)=2048
            ORDER BY id
        """).fetchall()
        created = 0
        for number, row in enumerate(rows, 1):
            doc_ids = [x['legal_document_id'] for x in news.execute(
                """
                SELECT legal_document_id FROM news_legal_document_candidates_v2
                WHERE news_document_id=%s ORDER BY rank LIMIT 5
                """, (row['document_id'],)
            ).fetchall()]
            if not doc_ids:
                continue
            literal = row["nemotron_embedding"]
            candidates = legal.execute("""
                WITH candidate AS (
                    SELECT id,document_id,
                           1-(nemotron_embedding <=> %s::vector) AS similarity,
                           row_number() OVER (
                               PARTITION BY document_id
                               ORDER BY nemotron_embedding <=> %s::vector
                           ) AS doc_rank
                    FROM legal_chunks_v2
                    WHERE nemotron_embedding IS NOT NULL AND document_id=ANY(%s)
                )
                SELECT c.id,c.document_id,
                       c.similarity
                FROM candidate c
                WHERE c.doc_rank=1
                ORDER BY c.similarity DESC
                LIMIT 5
            """, (literal, literal, doc_ids)).fetchall()
            selected = [x for x in candidates if float(x["similarity"]) >= threshold][:5]
            with news.transaction():
                for rank, item in enumerate(selected, 1):
                    news.execute("""
                        INSERT INTO news_legal_relations_v2
                        (news_document_id,legal_chunk_id,legal_document_id,similarity_score,rank,
                         relation_type,embedding_model,embedding_dimension,relation_version)
                        VALUES (%s,%s,%s,%s,%s,'SEMANTIC_SIMILARITY',%s,%s,%s)
                    """, (row["document_id"],item["id"],item["document_id"],item["similarity"],rank,
                          MODEL_NAME,DIMENSION,version))
                    created += 1
            if number % 100 == 0:
                print(f"RELATION_ARTICLES={number} CREATED={created}", flush=True)
    print(f"RELATION_COMPLETE ARTICLES={len(rows)} CREATED={created} THRESHOLD={threshold}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["prepare", "embed", "verify", "doc_prepare", "doc_embed", "doc_index", "doc_candidates", "needed_chunks", "index", "relations"])
    parser.add_argument("--threshold", type=float, default=RELATION_THRESHOLD)
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare()
    elif args.phase == "embed":
        embed()
    elif args.phase == "verify":
        verify_embeddings()
    elif args.phase == "doc_prepare":
        prepare_document_embeddings()
    elif args.phase == "doc_embed":
        embed_documents()
    elif args.phase == "doc_index":
        create_document_index()
    elif args.phase == "doc_candidates":
        build_document_candidates(args.threshold)
    elif args.phase == "needed_chunks":
        prepare_needed_chunks()
    elif args.phase == "index":
        create_vector_index()
    else:
        build_relations(args.threshold)


if __name__ == "__main__":
    main()
