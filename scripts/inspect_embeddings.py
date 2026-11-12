"""저장 벡터의 실제 차원과 기록된 provenance를 점검한다."""

from __future__ import annotations

from db import database_connection


def main() -> None:
    with database_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(DISTINCT vector_dims(embedding)), MIN(vector_dims(embedding)), "
                "MAX(vector_dims(embedding)), COUNT(*) FROM article_chunks"
            )
            distinct_dims, min_dim, max_dim, rows = cursor.fetchone()
            print(f"article_chunks rows={rows}, distinct_dimensions={distinct_dims}, min={min_dim}, max={max_dim}")
            cursor.execute("SELECT to_regclass('public.embedding_provenance')")
            has_provenance = cursor.fetchone()[0]
            if not has_provenance:
                print("embedding_provenance table 없음: 모델명/입력 방식은 DB에서 독립 확인되지 않음")
                return
            cursor.execute(
                "SELECT provider, model, dimensions, document_prefix, normalized_embeddings "
                "FROM embedding_provenance ORDER BY id"
            )
            for row in cursor.fetchall():
                print({"provider": row[0], "model": row[1], "dimensions": row[2],
                       "document_prefix": row[3], "normalized": row[4]})


if __name__ == "__main__":
    main()
