"""기존 PostgreSQL 텍스트만 Elasticsearch에 넣는다. 임베딩/DB는 변경하지 않는다."""

from __future__ import annotations

from elasticsearch import Elasticsearch, helpers

import config
from db import database_connection


def main() -> None:
    client = Elasticsearch(config.ELASTICSEARCH_URL)
    actions = []
    with database_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT article_id, title, content, url, source, published_at, document_type FROM articles"
            )
            for row in cursor.fetchall():
                actions.append({
                    "_op_type": "index", "_index": config.ELASTICSEARCH_INDEX, "_id": row[0],
                    "_source": {"article_id": row[0], "title": row[1], "content": row[2],
                                 "url": row[3], "source": row[4], "published_at": row[5],
                                 "document_type": row[6]},
                })
    if actions:
        helpers.bulk(client, actions, refresh="wait_for")
    print(f"텍스트 색인 완료: {len(actions)}건. PostgreSQL 임베딩은 변경하지 않았습니다.")


if __name__ == "__main__":
    main()
