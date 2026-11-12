import argparse
import json
from collections import Counter
from pathlib import Path
from difflib import SequenceMatcher

import numpy as np

from common.database import get_connection


VECTOR_DIMENSION = 2048
DEFAULT_TOP_K = 5
DEFAULT_THRESHOLD = 0.85
DEFAULT_BATCH_SIZE = 512


REFERENCE_WHERE = """
filter_status = 'categorized'
AND nemotron_embedding_status = 'COMPLETED'
AND nemotron_embedding IS NOT NULL
"""


CANDIDATE_WHERE = """
filter_status = 'excluded_stage1'
AND nemotron_embedding_status = 'COMPLETED'
AND nemotron_embedding IS NOT NULL
"""


def row_value(row, key: str, index: int):
    if isinstance(row, dict):
        return row.get(key)

    return row[index]


def parse_vector(value):
    if value is None:
        raise ValueError("embedding is NULL")

    if isinstance(value, np.ndarray):
        vector = value.astype(np.float32, copy=False)

    elif isinstance(value, (list, tuple)):
        vector = np.asarray(value, dtype=np.float32)

    elif hasattr(value, "tolist"):
        vector = np.asarray(
            value.tolist(),
            dtype=np.float32
        )

    else:
        text = str(value).strip()

        if text.startswith("[") and text.endswith("]"):
            text = text[1:-1]

        vector = np.fromstring(
            text,
            sep=",",
            dtype=np.float32
        )

    vector = vector.reshape(-1)

    if vector.size != VECTOR_DIMENSION:
        raise ValueError(
            f"dimension mismatch: "
            f"expected={VECTOR_DIMENSION}, "
            f"actual={vector.size}"
        )

    return vector


def normalize_matrix(matrix):
    norms = np.linalg.norm(
        matrix,
        axis=1,
        keepdims=True
    )

    norms[norms == 0] = 1

    return matrix / norms


# ============================================================
# CATEGORY 파싱
# ============================================================

def parse_categories(value):
    if value is None:
        return []

    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []

    if isinstance(value, dict):
        value = [value]

    if not isinstance(value, list):
        return []

    result = []

    for item in value:
        if not isinstance(item, dict):
            continue

        category_id = (
            item.get("id")
            or item.get("category_id")
            or item.get("code")
            or item.get("categoryCode")
        )

        category_name = (
            item.get("name")
            or item.get("category_name")
            or item.get("label")
            or item.get("category")
        )

        role = (
            item.get("role")
            or item.get("type")
        )

        if category_id or category_name:
            result.append({
                "id": category_id,
                "name": category_name,
                "role": role,
            })

    return result


def get_primary_category(categories):
    parsed = parse_categories(categories)

    for category in parsed:
        role = str(
            category.get("role") or ""
        ).lower()

        if role == "primary":
            return {
                "id": category.get("id"),
                "name": category.get("name"),
            }

    if parsed:
        first = parsed[0]

        return {
            "id": first.get("id"),
            "name": first.get("name"),
        }

    return None


def category_key(category):
    if not category:
        return None

    return (
        category.get("id")
        or category.get("name")
    )


# ============================================================
# REFERENCE
# ============================================================

def load_references(conn):
    query = f"""
        SELECT
            document_id,
            title,
            categories,
            nemotron_embedding::text
                AS nemotron_embedding
        FROM news_articles
        WHERE {REFERENCE_WHERE}
        ORDER BY id
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    references = []
    vectors = []

    skipped = 0

    for row in rows:
        document_id = row_value(
            row,
            "document_id",
            0
        )

        title = row_value(
            row,
            "title",
            1
        )

        categories = row_value(
            row,
            "categories",
            2
        )

        embedding = row_value(
            row,
            "nemotron_embedding",
            3
        )

        try:
            vector = parse_vector(
                embedding
            )

            primary_category = (
                get_primary_category(
                    categories
                )
            )

            references.append({
                "document_id":
                    str(document_id),

                "title":
                    title or "",

                "categories":
                    categories,

                "primary_category":
                    primary_category,
            })

            vectors.append(
                vector
            )

        except Exception as exc:
            skipped += 1

            if skipped <= 10:
                print(
                    f"[WARN] reference skip: "
                    f"{document_id} | {exc}"
                )

    matrix = np.vstack(
        vectors
    ).astype(
        np.float32,
        copy=False
    )

    matrix = normalize_matrix(
        matrix
    )

    print(
        f"[LOAD] references: "
        f"{len(references):,}"
    )

    print(
        f"[LOAD] skipped: "
        f"{skipped:,}"
    )

    return references, matrix


# ============================================================
# CATEGORY 구조 확인
# ============================================================

def inspect_categories():
    conn = get_connection()

    try:
        query = """
            SELECT
                document_id,
                title,
                categories
            FROM news_articles
            WHERE
                filter_status = 'categorized'
                AND categories IS NOT NULL
            LIMIT 10
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        print()
        print("=" * 80)
        print("CATEGORY STRUCTURE SAMPLE")
        print("=" * 80)

        for i, row in enumerate(
            rows,
            start=1
        ):
            document_id = row_value(
                row,
                "document_id",
                0
            )

            title = row_value(
                row,
                "title",
                1
            )

            categories = row_value(
                row,
                "categories",
                2
            )

            print()
            print(
                f"[{i}] {document_id}"
            )

            print(
                f"title: {title}"
            )

            print(
                "raw categories:"
            )

            print(
                json.dumps(
                    categories,
                    ensure_ascii=False,
                    indent=2,
                    default=str
                )
            )

            print(
                "parsed primary:"
            )

            print(
                get_primary_category(
                    categories
                )
            )

    finally:
        try:
            conn.rollback()
        except Exception:
            pass

        conn.close()


# ============================================================
# TITLE SIMILARITY
# ============================================================

def normalize_title(title):
    if not title:
        return ""

    return (
        str(title)
        .lower()
        .replace(" ", "")
        .replace('"', "")
        .replace("'", "")
        .replace("[속보]", "")
        .replace("[종합]", "")
        .replace("(종합)", "")
        .replace("(종합2보)", "")
    )


def title_similarity(
    title_a,
    title_b
):
    a = normalize_title(
        title_a
    )

    b = normalize_title(
        title_b
    )

    if not a or not b:
        return 0.0

    return SequenceMatcher(
        None,
        a,
        b
    ).ratio()


# ============================================================
# CATEGORY VOTE
# ============================================================

def vote_categories(
    matches
):
    keys = []

    category_map = {}

    weighted_scores = {}

    for match in matches:
        category = match.get(
            "primary_category"
        )

        key = category_key(
            category
        )

        if not key:
            continue

        keys.append(
            key
        )

        category_map[
            key
        ] = category

        weighted_scores[
            key
        ] = (
            weighted_scores.get(
                key,
                0.0
            )
            +
            float(
                match["similarity"]
            )
        )

    if not keys:
        return {
            "suggested_category":
                None,

            "vote_count":
                0,

            "vote_ratio":
                0.0,

            "weighted_score":
                0.0,
        }

    counts = Counter(
        keys
    )

    best_key = max(
        counts.keys(),
        key=lambda key: (
            counts[key],
            weighted_scores.get(
                key,
                0
            )
        )
    )

    vote_count = counts[
        best_key
    ]

    vote_ratio = (
        vote_count
        /
        len(matches)
    )

    return {
        "suggested_category":
            category_map[
                best_key
            ],

        "vote_count":
            vote_count,

        "vote_ratio":
            round(
                vote_ratio,
                4
            ),

        "weighted_score":
            round(
                weighted_scores[
                    best_key
                ],
                6
            ),
    }


# ============================================================
# CONFIDENCE
# ============================================================

def calculate_confidence(
    top_scores,
    vote_ratio,
    duplicate_similarity
):
    top1 = float(
        top_scores[0]
    )

    avg_top5 = float(
        np.mean(
            top_scores
        )
    )

    top_gap = 0.0

    if len(top_scores) >= 2:
        top_gap = (
            float(top_scores[0])
            -
            float(top_scores[1])
        )

    score_component = min(
        max(
            (
                top1 - 0.75
            ) / 0.20,
            0.0
        ),
        1.0
    )

    avg_component = min(
        max(
            (
                avg_top5 - 0.70
            ) / 0.20,
            0.0
        ),
        1.0
    )

    category_component = min(
        max(
            vote_ratio,
            0.0
        ),
        1.0
    )

    duplicate_component = min(
        max(
            duplicate_similarity,
            0.0
        ),
        1.0
    )

    confidence = (
        score_component * 0.40
        +
        avg_component * 0.25
        +
        category_component * 0.20
        +
        duplicate_component * 0.15
    )

    return {
        "confidence":
            round(
                confidence,
                4
            ),

        "top1":
            round(
                top1,
                6
            ),

        "avg_top5":
            round(
                avg_top5,
                6
            ),

        "top1_top2_gap":
            round(
                top_gap,
                6
            ),
    }


# ============================================================
# REFINE
# ============================================================

def refine(
    threshold=DEFAULT_THRESHOLD,
    top_k=DEFAULT_TOP_K,
    batch_size=DEFAULT_BATCH_SIZE,
    output=(
        "data/"
        "legal_semantic_refined.json"
    )
):
    conn = get_connection()

    try:
        (
            references,
            reference_matrix
        ) = load_references(
            conn
        )

        query = f"""
            SELECT
                document_id,
                title,
                description,
                filter_status,
                nemotron_embedding::text
                    AS nemotron_embedding
            FROM news_articles
            WHERE {CANDIDATE_WHERE}
            ORDER BY id
        """

        results = []

        processed = 0

        with conn.cursor() as cursor:
            cursor.execute(
                query
            )

            while True:
                rows = cursor.fetchmany(
                    batch_size
                )

                if not rows:
                    break

                candidates = []
                vectors = []

                for row in rows:
                    document_id = row_value(
                        row,
                        "document_id",
                        0
                    )

                    title = row_value(
                        row,
                        "title",
                        1
                    )

                    description = row_value(
                        row,
                        "description",
                        2
                    )

                    embedding = row_value(
                        row,
                        "nemotron_embedding",
                        4
                    )

                    try:
                        vector = parse_vector(
                            embedding
                        )

                        candidates.append({
                            "document_id":
                                str(
                                    document_id
                                ),

                            "title":
                                title or "",

                            "description":
                                description or "",
                        })

                        vectors.append(
                            vector
                        )

                    except Exception as exc:
                        print(
                            f"[WARN] skip "
                            f"{document_id}: "
                            f"{exc}"
                        )

                if not vectors:
                    continue

                candidate_matrix = np.vstack(
                    vectors
                ).astype(
                    np.float32,
                    copy=False
                )

                candidate_matrix = (
                    normalize_matrix(
                        candidate_matrix
                    )
                )

                similarities = (
                    candidate_matrix
                    @ reference_matrix.T
                )

                k = min(
                    top_k,
                    len(references)
                )

                indices = np.argpartition(
                    similarities,
                    -k,
                    axis=1
                )[:, -k:]

                row_idx = np.arange(
                    similarities.shape[0]
                )[:, None]

                scores = similarities[
                    row_idx,
                    indices
                ]

                order = np.argsort(
                    scores,
                    axis=1
                )[:, ::-1]

                indices = indices[
                    row_idx,
                    order
                ]

                scores = scores[
                    row_idx,
                    order
                ]

                for i, candidate in enumerate(
                    candidates
                ):
                    top1 = float(
                        scores[i][0]
                    )

                    if top1 < threshold:
                        continue

                    matches = []

                    best_title_similarity = 0.0

                    for rank in range(k):
                        ref_idx = int(
                            indices[
                                i,
                                rank
                            ]
                        )

                        score = float(
                            scores[
                                i,
                                rank
                            ]
                        )

                        reference = (
                            references[
                                ref_idx
                            ]
                        )

                        duplicate_score = (
                            title_similarity(
                                candidate[
                                    "title"
                                ],
                                reference[
                                    "title"
                                ]
                            )
                        )

                        best_title_similarity = max(
                            best_title_similarity,
                            duplicate_score
                        )

                        matches.append({
                            "rank":
                                rank + 1,

                            "document_id":
                                reference[
                                    "document_id"
                                ],

                            "title":
                                reference[
                                    "title"
                                ],

                            "similarity":
                                round(
                                    score,
                                    6
                                ),

                            "title_similarity":
                                round(
                                    duplicate_score,
                                    4
                                ),

                            "primary_category":
                                reference[
                                    "primary_category"
                                ],
                        })

                    vote = vote_categories(
                        matches
                    )

                    confidence = (
                        calculate_confidence(
                            scores[i],
                            vote[
                                "vote_ratio"
                            ],
                            best_title_similarity
                        )
                    )

                    likely_duplicate = (
                        best_title_similarity
                        >= 0.85
                    )

                    if top1 >= 0.90:
                        tier = "VERY_HIGH"

                    elif top1 >= 0.85:
                        tier = "HIGH"

                    elif top1 >= 0.80:
                        tier = "REVIEW"

                    else:
                        tier = "LOW"

                    results.append({
                        "document_id":
                            candidate[
                                "document_id"
                            ],

                        "title":
                            candidate[
                                "title"
                            ],

                        "description":
                            candidate[
                                "description"
                            ],

                        "tier":
                            tier,

                        "max_similarity":
                            round(
                                top1,
                                6
                            ),

                        "avg_top_k_similarity":
                            confidence[
                                "avg_top5"
                            ],

                        "top1_top2_gap":
                            confidence[
                                "top1_top2_gap"
                            ],

                        "suggested_category":
                            vote[
                                "suggested_category"
                            ],

                        "category_vote_count":
                            vote[
                                "vote_count"
                            ],

                        "category_vote_ratio":
                            vote[
                                "vote_ratio"
                            ],

                        "confidence":
                            confidence[
                                "confidence"
                            ],

                        "likely_duplicate":
                            likely_duplicate,

                        "best_title_similarity":
                            round(
                                best_title_similarity,
                                4
                            ),

                        "matches":
                            matches,
                    })

                processed += len(
                    candidates
                )

                print(
                    f"[PROCESS] "
                    f"{processed:,}"
                )

        results.sort(
            key=lambda item: (
                item[
                    "confidence"
                ],
                item[
                    "max_similarity"
                ]
            ),
            reverse=True
        )

        output_path = Path(
            output
        )

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        with output_path.open(
            "w",
            encoding="utf-8"
        ) as file:
            json.dump(
                results,
                file,
                ensure_ascii=False,
                indent=2
            )

        print()
        print("=" * 80)
        print("REFINED RESULT")
        print("=" * 80)

        print(
            f"threshold: "
            f"{threshold}"
        )

        print(
            f"candidates: "
            f"{len(results):,}"
        )

        very_high = sum(
            1
            for item in results
            if item["tier"]
            == "VERY_HIGH"
        )

        high = sum(
            1
            for item in results
            if item["tier"]
            == "HIGH"
        )

        duplicate_count = sum(
            1
            for item in results
            if item[
                "likely_duplicate"
            ]
        )

        category_success = sum(
            1
            for item in results
            if item[
                "suggested_category"
            ]
            is not None
        )

        print(
            f"VERY_HIGH >=0.90: "
            f"{very_high:,}"
        )

        print(
            f"HIGH 0.85~0.90 : "
            f"{high:,}"
        )

        print(
            f"category assigned: "
            f"{category_success:,}"
        )

        print(
            f"likely duplicate: "
            f"{duplicate_count:,}"
        )

        print()
        print("=" * 80)
        print("TOP 30")
        print("=" * 80)

        for index, item in enumerate(
            results[:30],
            start=1
        ):
            category = item.get(
                "suggested_category"
            )

            print()
            print(
                f"[{index}] "
                f"{item['tier']} "
                f"sim="
                f"{item['max_similarity']:.4f} "
                f"conf="
                f"{item['confidence']:.4f}"
            )

            print(
                item[
                    "title"
                ]
            )

            print(
                f"category="
                f"{category}"
            )

            print(
                f"vote="
                f"{item['category_vote_count']}"
                f"/{top_k}"
            )

            print(
                f"duplicate="
                f"{item['likely_duplicate']} "
                f"title_sim="
                f"{item['best_title_similarity']}"
            )

            if item["matches"]:
                print(
                    "match="
                    f"{item['matches'][0]['title']}"
                )

        print()
        print(
            f"JSON: "
            f"{output_path.resolve()}"
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


def main():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )

    subparsers.add_parser(
        "inspect-categories"
    )

    refine_parser = (
        subparsers.add_parser(
            "refine"
        )
    )

    refine_parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD
    )

    refine_parser.add_argument(
        "--top-k",
        type=int,
        default=DEFAULT_TOP_K
    )

    refine_parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE
    )

    refine_parser.add_argument(
        "--output",
        type=str,
        default=(
            "data/"
            "legal_semantic_refined.json"
        )
    )

    args = parser.parse_args()

    if args.command == "inspect-categories":
        inspect_categories()

    elif args.command == "refine":
        refine(
            threshold=args.threshold,
            top_k=args.top_k,
            batch_size=args.batch_size,
            output=args.output
        )


if __name__ == "__main__":
    main()