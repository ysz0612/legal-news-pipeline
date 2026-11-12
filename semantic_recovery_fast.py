import argparse
import json
import math
from pathlib import Path
from typing import Any

import numpy as np

from common.database import get_connection


# ============================================================
# 설정
# ============================================================

VECTOR_DIMENSION = 2048
DEFAULT_BATCH_SIZE = 512
DEFAULT_TOP_K = 5


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


# ============================================================
# Row 처리
# ============================================================

def row_value(row, key: str, index: int):
    """
    psycopg cursor가 dict_row를 반환하든 tuple을 반환하든
    둘 다 처리하기 위한 함수.
    """

    if isinstance(row, dict):
        return row.get(key)

    try:
        return row[index]
    except (IndexError, TypeError):
        return None


# ============================================================
# pgvector -> numpy
# ============================================================

def parse_vector(value) -> np.ndarray:
    """
    PostgreSQL pgvector 값을 numpy float32 벡터로 변환.

    지원:
    - numpy.ndarray
    - list
    - tuple
    - pgvector Vector
    - 문자열 "[0.1,0.2,...]"
    """

    if value is None:
        raise ValueError("embedding is NULL")

    # numpy
    if isinstance(value, np.ndarray):
        vector = value.astype(np.float32, copy=False)

    # list / tuple
    elif isinstance(value, (list, tuple)):
        vector = np.asarray(value, dtype=np.float32)

    # pgvector 객체 등에 tolist()이 존재하는 경우
    elif hasattr(value, "tolist"):
        vector = np.asarray(
            value.tolist(),
            dtype=np.float32
        )

    else:
        text = str(value).strip()

        if not text:
            raise ValueError("empty vector")

        if text.startswith("[") and text.endswith("]"):
            text = text[1:-1]

        if not text.strip():
            raise ValueError("empty vector")

        vector = np.fromstring(
            text,
            sep=",",
            dtype=np.float32
        )

    if vector.ndim != 1:
        vector = vector.reshape(-1)

    if vector.size == 0:
        raise ValueError("parsed vector is empty")

    if vector.size != VECTOR_DIMENSION:
        raise ValueError(
            f"vector dimension mismatch: "
            f"expected={VECTOR_DIMENSION}, "
            f"actual={vector.size}"
        )

    if not np.all(np.isfinite(vector)):
        raise ValueError(
            "vector contains NaN or Inf"
        )

    return vector


# ============================================================
# 정규화
# ============================================================

def normalize_matrix(matrix: np.ndarray) -> np.ndarray:
    """
    행 단위 L2 normalize.
    normalize 후 dot product = cosine similarity
    """

    norms = np.linalg.norm(
        matrix,
        axis=1,
        keepdims=True
    )

    zero_mask = norms.squeeze(axis=1) == 0

    if np.any(zero_mask):
        raise ValueError(
            f"zero vectors detected: "
            f"{int(np.sum(zero_mask))}"
        )

    return matrix / norms


# ============================================================
# category 처리
# ============================================================

def parse_categories(value: Any) -> list:
    if value is None:
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, dict):
        return [value]

    if isinstance(value, str):
        try:
            parsed = json.loads(value)

            if isinstance(parsed, list):
                return parsed

            if isinstance(parsed, dict):
                return [parsed]

        except Exception:
            return []

    return []


def get_primary_category(categories: Any):
    categories = parse_categories(categories)

    # role=primary 우선
    for category in categories:
        if not isinstance(category, dict):
            continue

        if category.get("role") == "primary":
            return {
                "id": category.get("id"),
                "name": category.get("name"),
            }

    # 없으면 첫 번째 category
    if categories:
        first = categories[0]

        if isinstance(first, dict):
            return {
                "id": first.get("id"),
                "name": first.get("name"),
            }

    return None


# ============================================================
# Reference 로드
# ============================================================

def load_references(conn):
    """
    확실한 법률기사(categorized)를 기준 벡터로 로드.
    """

    query = f"""
        SELECT
            document_id,
            title,
            categories,
            nemotron_embedding::text AS nemotron_embedding
        FROM news_articles
        WHERE {REFERENCE_WHERE}
        ORDER BY id
    """

    print("[LOAD] categorized reference embeddings...")

    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    print(
        f"[LOAD] reference DB rows: "
        f"{len(rows):,}"
    )

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
            vector = parse_vector(embedding)

            references.append({
                "document_id": str(document_id),
                "title": title or "",
                "categories": categories,
                "primary_category":
                    get_primary_category(categories),
            })

            vectors.append(vector)

        except Exception as exc:
            skipped += 1

            # 너무 많은 로그 방지
            if skipped <= 10:
                print(
                    f"[WARN] reference skip: "
                    f"document_id={document_id}, "
                    f"error={exc}"
                )

    if skipped > 10:
        print(
            f"[WARN] additional reference skips: "
            f"{skipped - 10:,}"
        )

    if not vectors:
        raise RuntimeError(
            "사용 가능한 categorized "
            "Nemotron embedding이 없습니다."
        )

    matrix = np.vstack(vectors).astype(
        np.float32,
        copy=False
    )

    if matrix.shape[1] != VECTOR_DIMENSION:
        raise RuntimeError(
            f"reference vector dimension mismatch: "
            f"expected={VECTOR_DIMENSION}, "
            f"actual={matrix.shape[1]}"
        )

    matrix = normalize_matrix(matrix)

    print(
        f"[LOAD] reference embeddings: "
        f"{len(references):,}"
    )

    print(
        f"[LOAD] reference skipped: "
        f"{skipped:,}"
    )

    print(
        f"[LOAD] vector dimension: "
        f"{matrix.shape[1]}"
    )

    return references, matrix


# ============================================================
# Candidate 개수
# ============================================================

def get_candidate_count(conn, limit=None):
    query = f"""
        SELECT COUNT(*) AS count
        FROM news_articles
        WHERE {CANDIDATE_WHERE}
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()

    count = row_value(
        row,
        "count",
        0
    )

    count = int(count or 0)

    if limit is not None:
        return min(
            count,
            int(limit)
        )

    return count


# ============================================================
# Candidate SQL
# ============================================================

def get_candidate_query(limit=None):
    limit_sql = ""

    if limit is not None:
        limit_sql = f"LIMIT {int(limit)}"

    return f"""
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
        {limit_sql}
    """


# ============================================================
# Candidate row 변환
# ============================================================

def parse_candidate_row(row):
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

    filter_status = row_value(
        row,
        "filter_status",
        3
    )

    embedding = row_value(
        row,
        "nemotron_embedding",
        4
    )

    vector = parse_vector(embedding)

    return {
        "document_id": str(document_id),
        "title": title or "",
        "description": description or "",
        "filter_status": filter_status,
        "vector": vector,
    }


# ============================================================
# Top-K 계산
# ============================================================

def calculate_top_k(
    candidate_matrix: np.ndarray,
    reference_matrix: np.ndarray,
    top_k: int
):
    """
    NumPy 행렬곱으로 cosine similarity 계산.

    candidate:
        batch x 2048

    reference:
        2774 x 2048
    """

    similarities = (
        candidate_matrix
        @ reference_matrix.T
    )

    k = min(
        int(top_k),
        reference_matrix.shape[0]
    )

    # Top-K 후보 인덱스
    top_indices = np.argpartition(
        similarities,
        -k,
        axis=1
    )[:, -k:]

    row_indices = np.arange(
        similarities.shape[0]
    )[:, None]

    top_scores = similarities[
        row_indices,
        top_indices
    ]

    # Top-K 내부 내림차순
    order = np.argsort(
        top_scores,
        axis=1
    )[:, ::-1]

    top_indices = top_indices[
        row_indices,
        order
    ]

    top_scores = top_scores[
        row_indices,
        order
    ]

    return top_indices, top_scores


# ============================================================
# Score 분석
# ============================================================

def analyze_scores(
    limit=None,
    batch_size=DEFAULT_BATCH_SIZE
):
    conn = get_connection()

    try:
        references, reference_matrix = (
            load_references(conn)
        )

        candidate_count = get_candidate_count(
            conn,
            limit
        )

        print()
        print("=" * 60)
        print("SEMANTIC RECOVERY ANALYSIS")
        print("=" * 60)

        print(
            f"REFERENCE COUNT : "
            f"{len(references):,}"
        )

        print(
            f"CANDIDATE COUNT : "
            f"{candidate_count:,}"
        )

        print(
            f"VECTOR DIMENSION: "
            f"{reference_matrix.shape[1]}"
        )

        print(
            f"BATCH SIZE      : "
            f"{batch_size}"
        )

        print(
            "METHOD          : "
            "NumPy cosine matrix multiplication"
        )

        print()

        if candidate_count == 0:
            print(
                "분석할 excluded_stage1 "
                "embedding이 없습니다."
            )
            return

        query = get_candidate_query(limit)

        max_scores = []

        processed = 0
        skipped = 0
        batch_no = 0

        total_batches = math.ceil(
            candidate_count / batch_size
        )

        # 일반 cursor 사용
        # 11,810건 정도는 충분히 처리 가능
        with conn.cursor() as cursor:
            cursor.execute(query)

            while True:
                rows = cursor.fetchmany(
                    batch_size
                )

                if not rows:
                    break

                candidates = []
                vectors = []

                for row in rows:
                    try:
                        candidate = (
                            parse_candidate_row(row)
                        )

                        candidates.append(
                            candidate
                        )

                        vectors.append(
                            candidate["vector"]
                        )

                    except Exception as exc:
                        skipped += 1

                        document_id = row_value(
                            row,
                            "document_id",
                            0
                        )

                        if skipped <= 10:
                            print(
                                "[WARN] candidate "
                                f"skip: "
                                f"document_id="
                                f"{document_id}, "
                                f"error={exc}"
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

                # cosine similarity
                similarities = (
                    candidate_matrix
                    @ reference_matrix.T
                )

                batch_max = np.max(
                    similarities,
                    axis=1
                )

                max_scores.extend(
                    batch_max.tolist()
                )

                processed += len(candidates)
                batch_no += 1

                print(
                    f"[ANALYZE] "
                    f"batch {batch_no}/"
                    f"{total_batches} "
                    f"- {processed:,}/"
                    f"{candidate_count:,}"
                )

                # 즉시 메모리 해제 유도
                del similarities
                del candidate_matrix

        if skipped > 10:
            print(
                f"[WARN] additional candidate "
                f"skips: {skipped - 10:,}"
            )

        if not max_scores:
            raise RuntimeError(
                "유효한 similarity 결과가 없습니다."
            )

        scores = np.asarray(
            max_scores,
            dtype=np.float32
        )

        print()
        print("=" * 60)
        print("SEMANTIC SCORE DISTRIBUTION")
        print("=" * 60)

        stats = [
            ("count", len(scores)),
            ("min", np.min(scores)),
            ("p10", np.percentile(scores, 10)),
            ("p25", np.percentile(scores, 25)),
            ("p50", np.percentile(scores, 50)),
            ("p75", np.percentile(scores, 75)),
            ("p90", np.percentile(scores, 90)),
            ("p95", np.percentile(scores, 95)),
            ("p97", np.percentile(scores, 97)),
            ("p99", np.percentile(scores, 99)),
            ("max", np.max(scores)),
        ]

        for name, value in stats:
            if name == "count":
                print(
                    f"{name:5}: "
                    f"{int(value):,}"
                )
            else:
                print(
                    f"{name:5}: "
                    f"{float(value):.6f}"
                )

        print()
        print("=" * 60)
        print("SCORE BANDS")
        print("=" * 60)

        bands = [
            (
                "< 0.50",
                scores < 0.50
            ),
            (
                "0.50 ~ 0.60",
                (scores >= 0.50)
                & (scores < 0.60)
            ),
            (
                "0.60 ~ 0.70",
                (scores >= 0.60)
                & (scores < 0.70)
            ),
            (
                "0.70 ~ 0.75",
                (scores >= 0.70)
                & (scores < 0.75)
            ),
            (
                "0.75 ~ 0.80",
                (scores >= 0.75)
                & (scores < 0.80)
            ),
            (
                "0.80 ~ 0.85",
                (scores >= 0.80)
                & (scores < 0.85)
            ),
            (
                "0.85 ~ 0.90",
                (scores >= 0.85)
                & (scores < 0.90)
            ),
            (
                ">= 0.90",
                scores >= 0.90
            ),
        ]

        for label, condition in bands:
            count = int(
                np.sum(condition)
            )

            percentage = (
                count
                / len(scores)
                * 100
            )

            print(
                f"{label:12}: "
                f"{count:6,} "
                f"({percentage:6.2f}%)"
            )

        print()
        print(
            f"PROCESSED: {processed:,}"
        )

        print(
            f"SKIPPED  : {skipped:,}"
        )

        print(
            "DB MODIFIED: NO"
        )

    finally:
        # 이 프로그램은 READ ONLY
        try:
            conn.rollback()
        except Exception:
            pass

        conn.close()


# ============================================================
# 법률 후보 추출
# ============================================================

def recover_candidates(
    limit=None,
    batch_size=DEFAULT_BATCH_SIZE,
    top_k=DEFAULT_TOP_K,
    output="data/legal_semantic_candidates.json",
    save_top=300
):
    conn = get_connection()

    try:
        references, reference_matrix = (
            load_references(conn)
        )

        candidate_count = get_candidate_count(
            conn,
            limit
        )

        print()
        print("=" * 60)
        print("SEMANTIC LEGAL RECOVERY")
        print("=" * 60)

        print(
            f"REFERENCE COUNT : "
            f"{len(references):,}"
        )

        print(
            f"CANDIDATE COUNT : "
            f"{candidate_count:,}"
        )

        print(
            f"VECTOR DIMENSION: "
            f"{reference_matrix.shape[1]}"
        )

        print(
            f"TOP-K           : "
            f"{top_k}"
        )

        print(
            f"BATCH SIZE      : "
            f"{batch_size}"
        )

        print()

        if candidate_count == 0:
            print(
                "분석할 excluded_stage1 "
                "embedding이 없습니다."
            )
            return

        query = get_candidate_query(limit)

        results = []

        processed = 0
        skipped = 0
        batch_no = 0

        total_batches = math.ceil(
            candidate_count / batch_size
        )

        with conn.cursor() as cursor:
            cursor.execute(query)

            while True:
                rows = cursor.fetchmany(
                    batch_size
                )

                if not rows:
                    break

                candidates = []
                vectors = []

                for row in rows:
                    try:
                        candidate = (
                            parse_candidate_row(row)
                        )

                        candidates.append(
                            candidate
                        )

                        vectors.append(
                            candidate["vector"]
                        )

                    except Exception as exc:
                        skipped += 1

                        document_id = row_value(
                            row,
                            "document_id",
                            0
                        )

                        if skipped <= 10:
                            print(
                                "[WARN] candidate "
                                f"skip: "
                                f"document_id="
                                f"{document_id}, "
                                f"error={exc}"
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

                (
                    top_indices,
                    top_scores
                ) = calculate_top_k(
                    candidate_matrix,
                    reference_matrix,
                    top_k
                )

                for candidate_idx, candidate in (
                    enumerate(candidates)
                ):
                    matched = []

                    for rank in range(
                        top_indices.shape[1]
                    ):
                        reference_idx = int(
                            top_indices[
                                candidate_idx,
                                rank
                            ]
                        )

                        similarity = float(
                            top_scores[
                                candidate_idx,
                                rank
                            ]
                        )

                        reference = references[
                            reference_idx
                        ]

                        matched.append({
                            "rank": rank + 1,

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
                                    similarity,
                                    6
                                ),

                            "primary_category":
                                reference[
                                    "primary_category"
                                ],
                        })

                    max_similarity = float(
                        top_scores[
                            candidate_idx,
                            0
                        ]
                    )

                    avg_top_k_similarity = float(
                        np.mean(
                            top_scores[
                                candidate_idx
                            ]
                        )
                    )

                    suggested_category = None

                    if matched:
                        suggested_category = (
                            matched[0].get(
                                "primary_category"
                            )
                        )

                    results.append({
                        "candidate_document_id":
                            candidate[
                                "document_id"
                            ],

                        "candidate_title":
                            candidate[
                                "title"
                            ],

                        "candidate_description":
                            candidate[
                                "description"
                            ],

                        "candidate_filter_status":
                            candidate[
                                "filter_status"
                            ],

                        "max_similarity":
                            round(
                                max_similarity,
                                6
                            ),

                        "avg_top_k_similarity":
                            round(
                                avg_top_k_similarity,
                                6
                            ),

                        "suggested_primary_category":
                            suggested_category,

                        "matched_legal_articles":
                            matched,
                    })

                processed += len(candidates)
                batch_no += 1

                print(
                    f"[RECOVER] "
                    f"batch {batch_no}/"
                    f"{total_batches} "
                    f"- {processed:,}/"
                    f"{candidate_count:,}"
                )

                del candidate_matrix
                del top_indices
                del top_scores

        if skipped > 10:
            print(
                f"[WARN] additional candidate "
                f"skips: {skipped - 10:,}"
            )

        # 높은 similarity 순
        results.sort(
            key=lambda item:
                item["max_similarity"],
            reverse=True
        )

        output_path = Path(output)

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        # save_top <= 0 이면 전체 저장
        if save_top is None or save_top <= 0:
            save_results = results
        else:
            save_results = results[
                :save_top
            ]

        with output_path.open(
            "w",
            encoding="utf-8"
        ) as file:
            json.dump(
                save_results,
                file,
                ensure_ascii=False,
                indent=2
            )

        print()
        print("=" * 70)
        print("TOP 30 SEMANTIC LEGAL CANDIDATES")
        print("=" * 70)

        for index, item in enumerate(
            results[:30],
            start=1
        ):
            category = item.get(
                "suggested_primary_category"
            )

            category_name = None
            category_id = None

            if isinstance(category, dict):
                category_name = (
                    category.get("name")
                )

                category_id = (
                    category.get("id")
                )

            print()
            print(
                f"[{index}] "
                f"score="
                f"{item['max_similarity']:.6f}"
            )

            print(
                f"title: "
                f"{item['candidate_title']}"
            )

            print(
                f"suggested category: "
                f"{category_id} / "
                f"{category_name}"
            )

            if item[
                "matched_legal_articles"
            ]:
                top_match = item[
                    "matched_legal_articles"
                ][0]

                print(
                    f"matched legal news: "
                    f"{top_match['title']}"
                )

        print()
        print("=" * 70)
        print("LOW SCORE SAMPLE")
        print("=" * 70)

        for item in results[-10:]:
            print(
                f"{item['max_similarity']:.6f}"
                f" | "
                f"{item['candidate_title']}"
            )

        print()
        print("=" * 70)
        print("RESULT")
        print("=" * 70)

        print(
            f"Candidate JSON: "
            f"{output_path.resolve()}"
        )

        print(
            f"Saved candidates: "
            f"{len(save_results):,}"
        )

        print(
            f"Analyzed candidates: "
            f"{len(results):,}"
        )

        print(
            f"Skipped candidates: "
            f"{skipped:,}"
        )

        if results:
            scores = np.asarray(
                [
                    item[
                        "max_similarity"
                    ]
                    for item in results
                ],
                dtype=np.float32
            )

            print(
                f"Top candidate score: "
                f"{np.max(scores):.6f}"
            )

            print(
                f"Median score: "
                f"{np.percentile(scores, 50):.6f}"
            )

            print(
                f"P90: "
                f"{np.percentile(scores, 90):.6f}"
            )

            print(
                f"P95: "
                f"{np.percentile(scores, 95):.6f}"
            )

            print(
                f"P99: "
                f"{np.percentile(scores, 99):.6f}"
            )

        print()
        print(
            "Automatic filter_status changes: NO"
        )

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
# DB 상태 확인
# ============================================================

def check_status():
    conn = get_connection()

    try:
        query = """
            SELECT
                filter_status,
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE
                        nemotron_embedding_status
                        = 'COMPLETED'
                        AND nemotron_embedding
                        IS NOT NULL
                ) AS completed,
                COUNT(*) FILTER (
                    WHERE
                        nemotron_embedding
                        IS NULL
                ) AS null_embedding
            FROM news_articles
            GROUP BY filter_status
            ORDER BY filter_status
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        print()
        print("=" * 70)
        print("NEMOTRON NEWS STATUS")
        print("=" * 70)

        for row in rows:
            status = row_value(
                row,
                "filter_status",
                0
            )

            total = row_value(
                row,
                "total",
                1
            )

            completed = row_value(
                row,
                "completed",
                2
            )

            null_embedding = row_value(
                row,
                "null_embedding",
                3
            )

            print(
                f"{str(status):20} "
                f"total={int(total):6,} "
                f"completed="
                f"{int(completed):6,} "
                f"null="
                f"{int(null_embedding):6,}"
            )

        print()
        print("DB MODIFIED: NO")

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
    parser = argparse.ArgumentParser(
        description=(
            "Fast legal-news semantic recovery "
            "using Nemotron embeddings + NumPy"
        )
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )

    # --------------------------------------------------------
    # check
    # --------------------------------------------------------

    subparsers.add_parser(
        "check",
        help=(
            "Nemotron embedding 상태 확인 "
            "(READ ONLY)"
        )
    )

    # --------------------------------------------------------
    # analyze
    # --------------------------------------------------------

    analyze_parser = (
        subparsers.add_parser(
            "analyze",
            help=(
                "excluded 기사와 categorized "
                "기사의 similarity 분포 분석"
            )
        )
    )

    analyze_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="테스트용 candidate 제한"
    )

    analyze_parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE
    )

    # --------------------------------------------------------
    # recover
    # --------------------------------------------------------

    recover_parser = (
        subparsers.add_parser(
            "recover",
            help=(
                "법률 뉴스 후보 Top-K 추출"
            )
        )
    )

    recover_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="테스트용 candidate 제한"
    )

    recover_parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE
    )

    recover_parser.add_argument(
        "--top-k",
        type=int,
        default=DEFAULT_TOP_K
    )

    recover_parser.add_argument(
        "--save-top",
        type=int,
        default=300,
        help=(
            "JSON에 저장할 상위 후보 수. "
            "0 이하이면 전체 저장"
        )
    )

    recover_parser.add_argument(
        "--output",
        type=str,
        default=(
            "data/"
            "legal_semantic_candidates.json"
        )
    )

    args = parser.parse_args()

    if args.command == "check":
        check_status()

    elif args.command == "analyze":
        analyze_scores(
            limit=args.limit,
            batch_size=args.batch_size
        )

    elif args.command == "recover":
        recover_candidates(
            limit=args.limit,
            batch_size=args.batch_size,
            top_k=args.top_k,
            output=args.output,
            save_top=args.save_top
        )


if __name__ == "__main__":
    main()