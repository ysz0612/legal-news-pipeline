"""Nori/BM25, pgvector 및 RRF 하이브리드 검색."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from elasticsearch import Elasticsearch

import config
from db import database_connection, get_embedding_dimension
from embedder import embed_query
from synonyms import expand_query


def _require_query(query: str) -> str:
    cleaned = query.strip()
    if not cleaned:
        raise ValueError("검색어를 한 글자 이상 입력해주세요.")
    return cleaned


def _serialize_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def search_bm25(query: str, top_k: int = 10) -> list[dict[str, Any]]:
    """Nori로 분석한 title/content를 BM25로 검색한다."""
    query = _require_query(query)
    expanded_queries = expand_query(query)
    should_queries = [
        {
            "multi_match": {
                "query": term,
                "fields": ["title^3", "content"],
                "type": "best_fields",
            }
        }
        for term in expanded_queries
    ]

    client = Elasticsearch(config.ELASTICSEARCH_URL)
    response = client.search(
        index=config.ELASTICSEARCH_INDEX,
        size=top_k,
        query={"bool": {"should": should_queries, "minimum_should_match": 1}},
        source=["article_id", "title", "source", "url", "published_at", "document_type"],
    )

    results: list[dict[str, Any]] = []
    for rank, hit in enumerate(response["hits"]["hits"], start=1):
        source = hit["_source"]
        results.append(
            {
                "article_id": source["article_id"],
                "title": source["title"],
                "source": source.get("source"),
                "url": source.get("url"),
                "published_at": source.get("published_at"),
                "document_type": source.get("document_type") or ("law" if source.get("source") is None else "article"),
                "bm25_rank": rank,
                "bm25_score": float(hit["_score"]),
            }
        )
    return results


def search_vector(query: str, top_k: int = 10) -> list[dict[str, Any]]:
    """청크를 코사인 거리로 검색하고 기사별 최고 청크만 반환한다."""
    query = _require_query(query)
    stored_dimension = get_embedding_dimension()
    query_embedding = embed_query(query, expected_dimension=stored_dimension)

    sql = """
        WITH nearest_chunks AS MATERIALIZED (
            SELECT
                c.article_id,
                c.chunk_index,
                c.embedding <=> %s AS distance
            FROM article_chunks AS c
            ORDER BY c.embedding <=> %s
            LIMIT %s
        ),
        best_chunks AS (
            SELECT DISTINCT ON (a.article_id)
                a.article_id,
                a.title,
                a.source,
                a.url,
                a.published_at,
                a.document_type,
                c.chunk_index,
                c.distance
            FROM nearest_chunks AS c
            JOIN articles AS a ON a.article_id = c.article_id
            ORDER BY a.article_id, c.distance
        )
        SELECT
            article_id,
            title,
            source,
            url,
            published_at,
            document_type,
            chunk_index,
            1 - distance AS vector_score
        FROM best_chunks
        ORDER BY distance
        LIMIT %s
    """

    # pgvector의 psycopg2 어댑터가 등록하는 numpy 배열 형식을 그대로 사용한다.
    vector = query_embedding
    chunk_candidate_k = max(top_k * 5, top_k)
    with database_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (vector, vector, chunk_candidate_k, top_k))
            rows = cursor.fetchall()

    return [
        {
            "article_id": row[0],
            "title": row[1],
            "source": row[2],
            "url": row[3],
            "published_at": _serialize_datetime(row[4]),
            "document_type": row[5] or ("law" if row[2] is None else "article"),
            "matched_chunk_index": row[6],
            "vector_rank": rank,
            "vector_score": float(row[7]),
        }
        for rank, row in enumerate(rows, start=1)
    ]


def hybrid_search(query: str, top_k: int = 5) -> list[dict[str, Any]]:
    """두 검색 결과의 기사 순위를 Reciprocal Rank Fusion으로 합친다."""
    query = _require_query(query)
    candidate_k = max(10, top_k * 2)
    bm25_results = search_bm25(query, top_k=candidate_k)
    vector_results = search_vector(query, top_k=candidate_k)

    fused: dict[str, dict[str, Any]] = {}
    for result_type, results in (
        ("bm25", bm25_results),
        ("vector", vector_results),
    ):
        rank_key = f"{result_type}_rank"
        score_key = f"{result_type}_score"
        for result in results:
            article_id = result["article_id"]
            entry = fused.setdefault(
                article_id,
                {
                    "article_id": article_id,
                    "title": result["title"],
                    "source": result.get("source"),
                    "url": result.get("url"),
                    "published_at": result.get("published_at"),
                    "document_type": result.get("document_type", "article"),
                    "bm25_rank": None,
                    "bm25_score": None,
                    "vector_rank": None,
                    "vector_score": None,
                    "hybrid_score": 0.0,
                },
            )
            rank = int(result[rank_key])
            entry[rank_key] = rank
            entry[score_key] = result[score_key]
            entry["hybrid_score"] += 1.0 / (config.RRF_K + rank)

    ranked = sorted(
        fused.values(),
        key=lambda item: (-item["hybrid_score"], item["article_id"]),
    )
    return ranked[:top_k]


def hybrid_search_queries(queries: list[str], top_k: int = 10) -> list[dict[str, Any]]:
    """원문과 보조 법률 검색어를 각각 BM25/vector로 검색한 뒤 RRF로 융합한다."""
    normalized = []
    for query in queries:
        cleaned = _require_query(query)
        if cleaned not in normalized:
            normalized.append(cleaned)
    fused: dict[str, dict[str, Any]] = {}
    for query_index, query in enumerate(normalized):
        candidate_k = max(10, top_k * 2)
        for result_type, results in (
            ("bm25", search_bm25(query, top_k=candidate_k)),
            ("vector", search_vector(query, top_k=candidate_k)),
        ):
            rank_key = f"{result_type}_rank"
            score_key = f"{result_type}_score"
            for result in results:
                article_id = result["article_id"]
                entry = fused.setdefault(
                    article_id,
                    {
                        "article_id": article_id,
                        "title": result["title"],
                        "source": result.get("source"),
                        "url": result.get("url"),
                        "published_at": result.get("published_at"),
                        "document_type": result.get("document_type", "article"),
                        "bm25_rank": None,
                        "bm25_score": None,
                        "vector_rank": None,
                        "vector_score": None,
                        "matched_chunk_index": None,
                        "hybrid_score": 0.0,
                        "retrieval_queries": [],
                    },
                )
                entry[rank_key] = min(
                    rank for rank in [entry[rank_key], int(result[rank_key])] if rank is not None
                )
                entry[score_key] = result[score_key]
                if result_type == "vector":
                    entry["matched_chunk_index"] = result.get("matched_chunk_index")
                entry["retrieval_queries"].append(query)
                entry["hybrid_score"] += 1.0 / (config.RRF_K + int(result[rank_key]))
    ranked = sorted(fused.values(), key=lambda item: (-item["hybrid_score"], item["article_id"]))
    return ranked[:top_k]
