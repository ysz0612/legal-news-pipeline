import hashlib
import json
from datetime import datetime, timezone

from psycopg.types.json import Jsonb

from common.database import get_connection


def now():
    """현재 UTC 시각을 ISO 문자열로 반환합니다."""
    return datetime.now(timezone.utc).isoformat()


def encode(value):
    """Kafka 전송 등에 사용할 JSON 문자열을 만듭니다."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
    )


def digest(text):
    """기사와 이벤트의 고유 ID를 생성합니다."""
    return hashlib.sha256(
        text.encode("utf-8")
    ).hexdigest()


def connect():
    """
    PostgreSQL에 연결합니다.

    path는 기존 코드와의 호환성을 위해 남겨두지만
    PostgreSQL에서는 사용하지 않습니다.
    """
    return get_connection()


def save_raw(db, article):
    """
    수집한 기사를 news_articles에 저장하고
    Kafka 전송 대기 데이터를 kafka_outbox에 저장합니다.

    두 저장 작업은 하나의 트랜잭션으로 처리됩니다.
    """

    document_id = article["document_id"]
    event_id = article["event_id"]

    with db.transaction():
        previous = db.execute(
            """
            SELECT event_id
            FROM news_articles
            WHERE document_id = %s
            """,
            (document_id,),
        ).fetchone()

        # 이미 동일한 내용의 기사가 저장되어 있는 경우
        if previous and previous["event_id"] == event_id:
            db.execute(
                """
                UPDATE news_articles
                SET
                    collected_at = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE document_id = %s
                """,
                (
                    article["collected_at"],
                    document_id,
                ),
            )

            return False

        # 기사 원본 저장
        db.execute(
            """
            INSERT INTO news_articles (
                document_id,
                event_id,
                title,
                description,
                source,
                url,
                published_at,
                collected_at,
                legal_keywords,
                categories,
                category_candidates,
                article_types,
                filter_status,
                filter_version,
                created_at,
                updated_at
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (document_id)
            DO UPDATE SET
                event_id = EXCLUDED.event_id,
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                source = EXCLUDED.source,
                url = EXCLUDED.url,
                published_at = EXCLUDED.published_at,
                collected_at = EXCLUDED.collected_at,
                legal_keywords = EXCLUDED.legal_keywords,
                categories = EXCLUDED.categories,
                category_candidates = EXCLUDED.category_candidates,
                article_types = EXCLUDED.article_types,
                filter_status = EXCLUDED.filter_status,
                filter_version = EXCLUDED.filter_version,
                kafka_sent_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                document_id,
                event_id,
                article["title"],
                article.get("description")
                or article.get("content")
                or "",
                article["source"],
                article["url"],
                article.get("published_at"),
                article["collected_at"],
                Jsonb(article.get("legal_keywords", [])),
                Jsonb(article.get("categories", [])),
                Jsonb(article.get("category_candidates", [])),
                Jsonb(article.get("article_types", [])),
                article.get("filter_status", "raw"),
                article.get("filter_version"),
            ),
        )

        # Kafka 전송 대기열 저장
        db.execute(
            """
            INSERT INTO kafka_outbox (
                event_id,
                document_id,
                topic,
                payload,
                sent_at,
                created_at
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                NULL,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (event_id)
            DO NOTHING
            """,
            (
                event_id,
                document_id,
                "raw-news",
                Jsonb(article),
            ),
        )

    return True


def save_stage(db, stage, article):
    """
    1차 또는 2차 필터 결과를 news_articles에 반영합니다.

    stage 값은 기존 Kafka 코드와의 호환성을 위해 받습니다.
    """

    with db.transaction():
        db.execute(
            """
            UPDATE news_articles
            SET
                legal_keywords = %s,
                categories = %s,
                category_candidates = %s,
                article_types = %s,
                filter_status = %s,
                filter_version = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE document_id = %s
            """,
            (
                Jsonb(article.get("legal_keywords", [])),
                Jsonb(article.get("categories", [])),
                Jsonb(article.get("category_candidates", [])),
                Jsonb(article.get("article_types", [])),
                article.get("filter_status", "unknown"),
                article.get("filter_version"),
                article["document_id"],
            ),
        )


def save_final(db, article):
    """
    Kafka 분류가 끝난 최종 기사 정보를 저장합니다.

    같은 document_id가 있으면 분류 결과와 기사 정보를 갱신합니다.
    """

    with db.transaction():
        db.execute(
            """
            INSERT INTO news_articles (
                document_id,
                event_id,
                title,
                description,
                source,
                url,
                published_at,
                collected_at,
                legal_keywords,
                categories,
                category_candidates,
                article_types,
                filter_status,
                filter_version,
                created_at,
                updated_at
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (document_id)
            DO UPDATE SET
                event_id = EXCLUDED.event_id,
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                source = EXCLUDED.source,
                url = EXCLUDED.url,
                published_at = EXCLUDED.published_at,
                collected_at = EXCLUDED.collected_at,
                legal_keywords = EXCLUDED.legal_keywords,
                categories = EXCLUDED.categories,
                category_candidates = EXCLUDED.category_candidates,
                article_types = EXCLUDED.article_types,
                filter_status = EXCLUDED.filter_status,
                filter_version = EXCLUDED.filter_version,
                updated_at = CURRENT_TIMESTAMP
            WHERE
                news_articles.collected_at <= EXCLUDED.collected_at
            """,
            (
                article["document_id"],
                article["event_id"],
                article["title"],
                article.get("description")
                or article.get("content")
                or "",
                article["source"],
                article["url"],
                article.get("published_at"),
                article["collected_at"],
                Jsonb(article.get("legal_keywords", [])),
                Jsonb(article.get("categories", [])),
                Jsonb(article.get("category_candidates", [])),
                Jsonb(article.get("article_types", [])),
                article.get("filter_status", "unclassified"),
                article.get("filter_version"),
            ),
        )
