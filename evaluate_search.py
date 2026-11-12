"""가상 평가 정답 파일을 사용한 BM25/Vector/Hybrid 검색 비교 도구."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from search import hybrid_search, search_bm25, search_vector


PROJECT_ROOT = Path(__file__).resolve().parent
GROUND_TRUTH_PATH = PROJECT_ROOT / "evaluation_ground_truth.json"


def _load_ground_truth() -> dict[str, Any]:
    with GROUND_TRUTH_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def _label(article_id: str, synthetic_ids: set[str]) -> str:
    return "가상 보강" if article_id in synthetic_ids else "기존 샘플"


def _precision_at_5(results: list[dict[str, Any]], relevant_ids: set[str]) -> tuple[float, int]:
    top_five = results[:5]
    hits = sum(result["article_id"] in relevant_ids for result in top_five)
    return hits / 5, hits


def _print_results(
    name: str,
    results: list[dict[str, Any]],
    relevant_ids: set[str],
    synthetic_ids: set[str],
) -> None:
    precision, hits = _precision_at_5(results, relevant_ids)
    duplicate_count = len(results) - len({result["article_id"] for result in results})
    print(f"  [{name}] Precision@5={precision:.2f} ({hits}/5), 중복 article_id={duplicate_count}")
    for rank, result in enumerate(results[:5], start=1):
        print(
            f"    {rank}. [{_label(result['article_id'], synthetic_ids)}] "
            f"{result['article_id']} | {result['title']} | "
            f"출처={result.get('source', '-')} | "
            f"BM25 rank={result.get('bm25_rank', '-')}, "
            f"Vector rank={result.get('vector_rank', '-')}, "
            f"RRF={result.get('hybrid_score', '-') }"
        )


def main() -> None:
    truth = _load_ground_truth()
    synthetic_ids = set(truth["synthetic_article_ids"])
    searchers: dict[str, Callable[[str, int], list[dict[str, Any]]]] = {
        "BM25": search_bm25,
        "Vector": search_vector,
        "Hybrid": hybrid_search,
    }

    for query, evaluation in truth["queries"].items():
        relevant_ids = set(evaluation["relevant_article_ids"])
        print(f"\n=== 질의: {query} ===")
        print(f"정답 후보 수: {len(relevant_ids)} | {evaluation.get('notes', '')}")
        for name, searcher in searchers.items():
            _print_results(name, searcher(query, top_k=10), relevant_ids, synthetic_ids)


if __name__ == "__main__":
    main()
