"""JSON 기사를 PostgreSQL/pgvector와 Elasticsearch에 색인한다."""

from __future__ import annotations

import json
import argparse
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import Json

import config
from db import database_connection, init_database
from embedder import embed_documents


@dataclass
class PreparationStats:
    input_records: int = 0
    normalized_articles: int = 0
    excluded_records: int = 0
    excluded_reasons: dict[str, int] = field(default_factory=dict)
    duplicate_urls: int = 0
    source_format: str = "article-list"

    def exclude(self, reason: str) -> None:
        self.excluded_records += 1
        self.excluded_reasons[reason] = self.excluded_reasons.get(reason, 0) + 1


def _sha256_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def chunk_text(text: str, chunk_size: int = config.CHUNK_SIZE) -> list[str]:
    """문단 경계를 우선해 약 700자 청크를 만들고 소량 중첩한다."""
    cleaned = "\n".join(line.strip() for line in text.splitlines() if line.strip())
    if not cleaned:
        return []

    chunks: list[str] = []
    start = 0
    while start < len(cleaned):
        end = min(start + chunk_size, len(cleaned))
        if end < len(cleaned):
            boundary = max(
                cleaned.rfind("\n", start, end),
                cleaned.rfind(". ", start, end),
                cleaned.rfind("다. ", start, end),
            )
            if boundary > start + chunk_size // 2:
                end = boundary + 1
        chunk = cleaned[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= len(cleaned):
            break
        start = max(end - config.CHUNK_OVERLAP, start + 1)
    return chunks


def prepare_articles(input_path: Path) -> tuple[list[dict[str, Any]], PreparationStats]:
    with input_path.open("r", encoding="utf-8") as file:
        articles = json.load(file)
    if not isinstance(articles, list):
        raise ValueError("입력 JSON의 최상위 값은 배열이어야 합니다.")

    stats = PreparationStats(input_records=len(articles))
    if articles and isinstance(articles[0], dict) and {
        "document_key",
        "law_name",
        "chunk_index",
    }.issubset(articles[0]):
        stats.source_format = "document-chunk-list"
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in articles:
            document_key = str(row.get("document_key") or "").strip()
            if not document_key:
                stats.exclude("document_key_missing")
                continue
            if not str(row.get("content") or "").strip():
                stats.exclude("content_missing")
                continue
            grouped.setdefault(document_key, []).append(row)

        normalized: list[dict[str, Any]] = []
        for document_key, rows in grouped.items():
            rows.sort(key=lambda row: int(row.get("chunk_index", 0)))
            law_name = str(rows[0].get("law_name") or "").strip()
            if not law_name:
                stats.exclude("title_missing")
                continue
            url = f"https://example.com/law/{document_key}"
            normalized.append(
                {
                    "article_id": _sha256_url(url),
                    "title": law_name,
                    "content": "\n".join(str(row["content"]).strip() for row in rows),
                    "url": url,
                    # 입력에 출처/발행 시각이 없으므로 값을 추정하지 않는다.
                    "source": None,
                    "published_at": None,
                    "collected_at": None,
                    "legal_keywords": [],
                    "categories": [],
                    "document_type": "law",
                }
            )
        stats.normalized_articles = len(normalized)
        return normalized, stats

    normalized = []
    seen_urls: set[str] = set()
    for article in articles:
        if not isinstance(article, dict):
            stats.exclude("record_not_object")
            continue
        title = str(article.get("title") or "").strip()
        content = str(article.get("content") or "").strip()
        url = str(article.get("url") or "").strip()
        if not title:
            stats.exclude("title_missing")
            continue
        if not content:
            stats.exclude("content_missing")
            continue
        if not url:
            stats.exclude("url_missing")
            continue
        if url in seen_urls:
            stats.duplicate_urls += 1
        seen_urls.add(url)
        item = dict(article)
        item["title"] = title
        item["content"] = content
        item["url"] = url
        item["article_id"] = str(item.get("article_id") or _sha256_url(url))
        item["legal_keywords"] = item.get("legal_keywords") or []
        item["categories"] = item.get("categories") or []
        item["document_type"] = item.get("document_type") or "article"
        normalized.append(item)
    stats.normalized_articles = len(normalized)
    return normalized, stats


def load_articles(input_path: Path | None = None) -> list[dict[str, Any]]:
    """기존 호출 호환용 로더. 경로를 생략하면 sample_articles.json을 사용한다."""
    articles, _ = prepare_articles(input_path or config.SAMPLE_ARTICLES_PATH)
    return articles


def ensure_elasticsearch_index(client: Elasticsearch) -> None:
    if client.indices.exists(index=config.ELASTICSEARCH_INDEX):
        return
    client.indices.create(
        index=config.ELASTICSEARCH_INDEX,
        settings={
            "analysis": {
                "tokenizer": {
                    "legal_nori_tokenizer": {
                        "type": "nori_tokenizer",
                        "decompound_mode": "mixed",
                    }
                },
                "analyzer": {
                    "legal_nori_analyzer": {
                        "type": "custom",
                        "tokenizer": "legal_nori_tokenizer",
                        "filter": ["lowercase"],
                    }
                },
            }
        },
        mappings={
            "properties": {
                "article_id": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "analyzer": "legal_nori_analyzer",
                    "search_analyzer": "legal_nori_analyzer",
                },
                "content": {
                    "type": "text",
                    "analyzer": "legal_nori_analyzer",
                    "search_analyzer": "legal_nori_analyzer",
                },
                "url": {"type": "keyword"},
                "source": {"type": "keyword"},
                "published_at": {"type": "date"},
                "collected_at": {"type": "date"},
                "legal_keywords": {"type": "keyword"},
                "categories": {"type": "keyword"},
                "document_type": {"type": "keyword"},
            }
        },
    )


def _upsert_article(cursor: Any, article: dict[str, Any]) -> tuple[str, bool]:
    """article_id 또는 URL이 이미 있으면 그 레코드를 갱신한다."""
    cursor.execute(
        """
        SELECT article_id
        FROM articles
        WHERE article_id = %s OR url = %s
        ORDER BY CASE WHEN article_id = %s THEN 0 ELSE 1 END
        LIMIT 1
        FOR UPDATE
        """,
        (article["article_id"], article["url"], article["article_id"]),
    )
    existing = cursor.fetchone()
    canonical_id = existing[0] if existing else article["article_id"]

    if existing:
        cursor.execute(
            """
            UPDATE articles
            SET title = %s,
                content = %s,
                url = %s,
                source = %s,
                published_at = %s,
                collected_at = %s,
                legal_keywords = %s,
                categories = %s,
                document_type = %s
            WHERE article_id = %s
            """,
            (
                article["title"],
                article["content"],
                article["url"],
                article.get("source"),
                article.get("published_at"),
                article.get("collected_at"),
                Json(article.get("legal_keywords", [])),
                Json(article.get("categories", [])),
                article.get("document_type", "article"),
                canonical_id,
            ),
        )
    else:
        cursor.execute(
            """
            INSERT INTO articles (
                article_id, title, content, url, source, published_at,
                collected_at, legal_keywords, categories
                , document_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                canonical_id,
                article["title"],
                article["content"],
                article["url"],
                article.get("source"),
                article.get("published_at"),
                article.get("collected_at"),
                Json(article.get("legal_keywords", [])),
                Json(article.get("categories", [])),
                article.get("document_type", "article"),
            ),
        )
    return canonical_id, not bool(existing)


def index_articles(articles: list[dict[str, Any]]) -> dict[str, int]:
    if config.DOCUMENT_EMBEDDING_PROVIDER != "local":
        raise RuntimeError(
            "현재 indexer.py는 NVIDIA 문서 임베딩을 생성하지 않습니다. "
            "기존 문서 벡터를 자동으로 재생성하지 않도록 DOCUMENT_EMBEDDING_PROVIDER=local로 두세요."
        )
    if config.EMBEDDING_MODEL != config.DOCUMENT_EMBEDDING_MODEL or config.EMBEDDING_DIMENSION != config.DOCUMENT_EMBEDDING_DIMENSION:
        raise RuntimeError(
            "문서 색인 모델/차원 설정이 서로 다릅니다. 기존 벡터를 임의로 재생성하지 않도록 확인하세요."
        )
    elasticsearch = Elasticsearch(config.ELASTICSEARCH_URL)
    if not elasticsearch.ping():
        raise ConnectionError(f"Elasticsearch에 연결할 수 없습니다: {config.ELASTICSEARCH_URL}")
    ensure_elasticsearch_index(elasticsearch)

    elastic_actions: list[dict[str, Any]] = []
    inserted_count = 0
    updated_count = 0
    for number, article in enumerate(articles, start=1):
        chunks = chunk_text(article["content"])
        if not chunks:
            raise ValueError(f"본문이 비어 있어 청크를 만들 수 없습니다: {article['article_id']}")
        embeddings = embed_documents(chunks)

        with database_connection() as conn:
            with conn.cursor() as cursor:
                canonical_id, inserted = _upsert_article(cursor, article)
                if inserted:
                    inserted_count += 1
                else:
                    updated_count += 1
                cursor.execute(
                    "DELETE FROM article_chunks WHERE article_id = %s",
                    (canonical_id,),
                )
                cursor.executemany(
                    """
                    INSERT INTO article_chunks
                        (article_id, chunk_index, content, embedding)
                    VALUES (%s, %s, %s, %s)
                    """,
                    [
                        (canonical_id, index, chunk, embedding)
                        for index, (chunk, embedding) in enumerate(zip(chunks, embeddings))
                    ],
                )

        document = dict(article)
        document["article_id"] = canonical_id
        elastic_actions.append(
            {
                "_op_type": "index",
                "_index": config.ELASTICSEARCH_INDEX,
                "_id": canonical_id,
                "_source": document,
            }
        )
        print(f"[{number}/{len(articles)}] {article['title']} 처리 완료")

    helpers.bulk(elasticsearch, elastic_actions, refresh="wait_for")
    return {
        "inserted": inserted_count,
        "updated": updated_count,
        "indexed": len(elastic_actions),
    }


def main(json_path: str | None = None) -> None:
    print("PostgreSQL 스키마를 준비합니다...")
    init_database()
    input_path = Path(json_path) if json_path else config.SAMPLE_ARTICLES_PATH
    articles, preparation = prepare_articles(input_path)
    print(f"입력 파일: {input_path}")
    print(
        f"입력 레코드 {preparation.input_records}건, "
        f"정규화 기사 {preparation.normalized_articles}건, "
        f"제외 {preparation.excluded_records}건, "
        f"입력 내 URL 중복 {preparation.duplicate_urls}건, "
        f"형식={preparation.source_format}"
    )
    if preparation.excluded_reasons:
        print(f"제외 사유: {preparation.excluded_reasons}")
    result = index_articles(articles)
    print(
        f"색인 완료: 신규 {result['inserted']}건, "
        f"갱신 {result['updated']}건, Elasticsearch 처리 {result['indexed']}건"
    )
    print("색인이 완료되었습니다.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="JSON 기사를 DB와 Elasticsearch에 색인합니다.")
    parser.add_argument(
        "json_path",
        nargs="?",
        help="입력 JSON 경로 (생략 시 sample_articles.json)",
    )
    args = parser.parse_args()
    main(args.json_path)
