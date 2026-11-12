"""PostgreSQL/pgvector 연결과 스키마 초기화."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg2
from pgvector.psycopg2 import register_vector
from psycopg2.extensions import connection

import config


_article_type_checked = False


def get_connection(*, with_vector: bool = True) -> connection:
    global _article_type_checked
    conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )
    if with_vector:
        register_vector(conn)
    if not _article_type_checked:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT to_regclass('public.articles') IS NOT NULL"
            )
            table_exists = bool(cursor.fetchone()[0])
            if table_exists:
                cursor.execute(
                    "ALTER TABLE articles ADD COLUMN IF NOT EXISTS document_type VARCHAR(30)"
                )
                cursor.execute(
                    "UPDATE articles SET document_type = CASE WHEN source IS NULL THEN 'law' ELSE 'article' END "
                    "WHERE document_type IS NULL"
                )
                cursor.execute("ALTER TABLE articles ALTER COLUMN document_type SET DEFAULT 'article'")
                cursor.execute("ALTER TABLE articles ALTER COLUMN document_type SET NOT NULL")
        conn.commit()
        _article_type_checked = table_exists
    return conn


@contextmanager
def database_connection(*, with_vector: bool = True) -> Iterator[connection]:
    conn = get_connection(with_vector=with_vector)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_database() -> None:
    """pgvector 확장과 검색에 필요한 테이블/인덱스를 생성한다."""
    with database_connection(with_vector=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    article_id VARCHAR(64) PRIMARY KEY,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    source VARCHAR(100),
                    published_at TIMESTAMPTZ,
                    collected_at TIMESTAMPTZ,
                    legal_keywords JSONB NOT NULL DEFAULT '[]'::jsonb,
                    categories JSONB NOT NULL DEFAULT '[]'::jsonb,
                    document_type VARCHAR(30) NOT NULL DEFAULT 'article'
                )
                """
            )
            cursor.execute(
                "ALTER TABLE articles ADD COLUMN IF NOT EXISTS document_type VARCHAR(30)"
            )
            cursor.execute(
                "UPDATE articles SET document_type = CASE WHEN source IS NULL THEN 'law' ELSE 'article' END "
                "WHERE document_type IS NULL"
            )
            cursor.execute(
                "ALTER TABLE articles ALTER COLUMN document_type SET DEFAULT 'article'"
            )
            cursor.execute(
                "ALTER TABLE articles ALTER COLUMN document_type SET NOT NULL"
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS article_chunks (
                    chunk_id BIGSERIAL PRIMARY KEY,
                    article_id VARCHAR(64) NOT NULL
                        REFERENCES articles(article_id) ON DELETE CASCADE,
                    chunk_index INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    embedding VECTOR({config.EMBEDDING_DIMENSION}) NOT NULL,
                    UNIQUE (article_id, chunk_index)
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS article_chunks_embedding_hnsw_idx
                ON article_chunks
                USING hnsw (embedding vector_cosine_ops)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS article_chunks_article_id_idx
                ON article_chunks (article_id)
                """
            )


def get_embedding_dimension() -> int:
    """현재 검색 DB에 저장된 실제 vector 차원을 확인한다."""
    with database_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(DISTINCT vector_dims(embedding)),
                       MIN(vector_dims(embedding)), COUNT(*)
                FROM article_chunks
                """
            )
            distinct_count, dimension, row_count = cursor.fetchone()
    if not row_count or not dimension:
        raise RuntimeError("article_chunks에 확인 가능한 문서 임베딩이 없습니다.")
    if distinct_count != 1:
        raise RuntimeError("article_chunks에 서로 다른 임베딩 차원이 섞여 있습니다.")
    return int(dimension)
