import argparse
import json
from pathlib import Path

from psycopg.types.json import Jsonb

from common.database import get_connection


INPUT_FILE = Path(
    "data/legal_semantic_final_review.json"
)


def load_auto_items():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"파일이 없습니다: {INPUT_FILE.resolve()}"
        )

    with INPUT_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:
        items = json.load(file)

    auto_items = [
        item
        for item in items
        if item.get("final_decision") == "AUTO"
    ]

    return auto_items


def build_categories(item):
    category = item.get(
        "suggested_category"
    )

    if not category:
        raise ValueError(
            f"category missing: "
            f"{item.get('document_id')}"
        )

    category_id = category.get("id")
    category_name = category.get("name")

    if not category_id or not category_name:
        raise ValueError(
            f"invalid category: "
            f"{item.get('document_id')} "
            f"{category}"
        )

    return [
        {
            "id": category_id,
            "name": category_name,
            "role": "primary",
        }
    ]


# ============================================================
# PREVIEW
# ============================================================

def preview():
    items = load_auto_items()

    conn = get_connection()

    try:
        print()
        print("=" * 90)
        print("SEMANTIC RECOVERY APPLY PREVIEW")
        print("=" * 90)

        print(
            f"AUTO ITEMS IN JSON: "
            f"{len(items):,}"
        )

        document_ids = [
            item["document_id"]
            for item in items
        ]

        query = """
            SELECT
                document_id,
                title,
                filter_status,
                categories,
                nemotron_embedding_status
            FROM news_articles
            WHERE document_id = ANY(%s)
            ORDER BY id
        """

        with conn.cursor() as cursor:
            cursor.execute(
                query,
                (document_ids,)
            )

            rows = cursor.fetchall()

        print(
            f"DB MATCHED ROWS   : "
            f"{len(rows):,}"
        )

        excluded_count = 0
        categorized_count = 0
        other_count = 0

        for row in rows:
            if isinstance(row, dict):
                status = row.get(
                    "filter_status"
                )
            else:
                status = row[2]

            if status == "excluded_stage1":
                excluded_count += 1

            elif status == "categorized":
                categorized_count += 1

            else:
                other_count += 1

        print(
            f"excluded_stage1   : "
            f"{excluded_count:,}"
        )

        print(
            f"already categorized: "
            f"{categorized_count:,}"
        )

        print(
            f"other status      : "
            f"{other_count:,}"
        )

        print()
        print("=" * 90)
        print("AUTO SAMPLE")
        print("=" * 90)

        for index, item in enumerate(
            items[:30],
            start=1
        ):
            category = build_categories(
                item
            )

            print()
            print(
                f"[{index}]"
            )

            print(
                f"document_id: "
                f"{item['document_id']}"
            )

            print(
                f"title: "
                f"{item.get('title')}"
            )

            print(
                f"similarity: "
                f"{item.get('max_similarity')}"
            )

            print(
                f"confidence: "
                f"{item.get('confidence')}"
            )

            print(
                f"category: "
                f"{category}"
            )

        print()
        print("=" * 90)
        print("PREVIEW RESULT")
        print("=" * 90)

        print(
            "DB MODIFIED: NO"
        )

    finally:
        try:
            conn.rollback()
        except Exception:
            pass

        conn.close()


# ============================================================
# APPLY
# ============================================================

def apply_changes():
    items = load_auto_items()

    conn = get_connection()

    updated = 0
    skipped = 0
    failed = 0

    try:
        print()
        print("=" * 90)
        print("SEMANTIC RECOVERY APPLY")
        print("=" * 90)

        print(
            f"TARGET AUTO ITEMS: "
            f"{len(items):,}"
        )

        with conn.cursor() as cursor:

            for index, item in enumerate(
                items,
                start=1
            ):
                document_id = (
                    item["document_id"]
                )

                try:
                    categories = (
                        build_categories(
                            item
                        )
                    )

                    cursor.execute(
                        """
                        SELECT
                            filter_status
                        FROM news_articles
                        WHERE document_id = %s
                        FOR UPDATE
                        """,
                        (document_id,)
                    )

                    row = cursor.fetchone()

                    if row is None:
                        print(
                            f"[SKIP] "
                            f"{document_id} "
                            f"not found"
                        )

                        skipped += 1
                        continue

                    if isinstance(row, dict):
                        current_status = (
                            row.get(
                                "filter_status"
                            )
                        )
                    else:
                        current_status = row[0]

                    if (
                        current_status
                        != "excluded_stage1"
                    ):
                        print(
                            f"[SKIP] "
                            f"{document_id} "
                            f"status="
                            f"{current_status}"
                        )

                        skipped += 1
                        continue

                    cursor.execute(
                        """
                        UPDATE news_articles
                        SET
                            filter_status =
                                'categorized',

                            categories =
                                %s,

                            updated_at =
                                NOW()

                        WHERE
                            document_id = %s
                            AND filter_status =
                                'excluded_stage1'
                        """,
                        (
                            Jsonb(
                                categories
                            ),
                            document_id,
                        )
                    )

                    if cursor.rowcount == 1:
                        updated += 1

                        print(
                            f"[UPDATE "
                            f"{index}/"
                            f"{len(items)}] "
                            f"{document_id}"
                        )

                    else:
                        skipped += 1

                        print(
                            f"[SKIP] "
                            f"{document_id} "
                            f"rowcount=0"
                        )

                except Exception as exc:
                    failed += 1

                    print(
                        f"[ERROR] "
                        f"{document_id} "
                        f"{exc}"
                    )

                    raise

        conn.commit()

        print()
        print("=" * 90)
        print("APPLY RESULT")
        print("=" * 90)

        print(
            f"UPDATED : "
            f"{updated:,}"
        )

        print(
            f"SKIPPED : "
            f"{skipped:,}"
        )

        print(
            f"FAILED  : "
            f"{failed:,}"
        )

        print(
            "TRANSACTION: COMMITTED"
        )

    except Exception:
        conn.rollback()

        print()
        print(
            "TRANSACTION: ROLLED BACK"
        )

        raise

    finally:
        conn.close()


# ============================================================
# VERIFY
# ============================================================

def verify():
    items = load_auto_items()

    document_ids = [
        item["document_id"]
        for item in items
    ]

    conn = get_connection()

    try:
        with conn.cursor() as cursor:

            cursor.execute(
                """
                SELECT
                    filter_status,
                    COUNT(*)
                FROM news_articles
                WHERE document_id = ANY(%s)
                GROUP BY filter_status
                ORDER BY filter_status
                """,
                (document_ids,)
            )

            status_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    COUNT(*)
                FROM news_articles
                WHERE
                    document_id = ANY(%s)
                    AND filter_status =
                        'categorized'
                    AND categories IS NOT NULL
                    AND jsonb_array_length(
                        categories
                    ) > 0
                """,
                (document_ids,)
            )

            category_row = (
                cursor.fetchone()
            )

            if isinstance(
                category_row,
                dict
            ):
                category_ok = (
                    next(
                        iter(
                            category_row.values()
                        )
                    )
                )
            else:
                category_ok = (
                    category_row[0]
                )

            cursor.execute(
                """
                SELECT
                    COUNT(*)
                FROM news_articles
                WHERE
                    document_id = ANY(%s)
                    AND nemotron_embedding
                        IS NOT NULL
                    AND nemotron_embedding_status
                        = 'COMPLETED'
                """,
                (document_ids,)
            )

            embedding_row = (
                cursor.fetchone()
            )

            if isinstance(
                embedding_row,
                dict
            ):
                embedding_ok = (
                    next(
                        iter(
                            embedding_row.values()
                        )
                    )
                )
            else:
                embedding_ok = (
                    embedding_row[0]
                )

        print()
        print("=" * 90)
        print("SEMANTIC RECOVERY VERIFY")
        print("=" * 90)

        print(
            f"EXPECTED AUTO: "
            f"{len(items):,}"
        )

        print()
        print("STATUS")

        for row in status_rows:
            if isinstance(row, dict):
                status = row.get(
                    "filter_status"
                )

                count = row.get(
                    "count"
                )
            else:
                status = row[0]
                count = row[1]

            print(
                f"{status}: "
                f"{count:,}"
            )

        print()
        print(
            f"categorized + category OK: "
            f"{category_ok:,}"
        )

        print(
            f"Nemotron embedding preserved: "
            f"{embedding_ok:,}"
        )

        print()
        print(
            "DB MODIFIED: NO"
        )

    finally:
        try:
            conn.rollback()
        except Exception:
            pass

        conn.close()


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()

    subparsers = (
        parser.add_subparsers(
            dest="command",
            required=True
        )
    )

    subparsers.add_parser(
        "preview"
    )

    subparsers.add_parser(
        "apply"
    )

    subparsers.add_parser(
        "verify"
    )

    args = parser.parse_args()

    if args.command == "preview":
        preview()

    elif args.command == "apply":
        apply_changes()

    elif args.command == "verify":
        verify()


if __name__ == "__main__":
    main()