"""원문 검색, 법률 변환 검색, 병행 검색을 같은 질문으로 비교한다."""

from __future__ import annotations

import time

from answer import rewrite_query
from search import hybrid_search, hybrid_search_queries


QUESTIONS = [
    "개인정보 유출이 발생하면 기업은 무엇을 해야 하나요?",
    "학교폭력 피해학생 보호 조치는 무엇인가요?",
    "전세사기 관련 법적 지원이 있나요?",
    "존재하지 않는 가상의 법률 질문입니다.",
]


def _titles(results: list[dict]) -> str:
    return " | ".join(item["title"] for item in results[:5]) or "(결과 없음)"


def main() -> None:
    for question in QUESTIONS:
        print(f"\n=== {question} ===")
        plan = rewrite_query(question)
        queries = [plan.original_query]
        if plan.used_openai:
            queries.extend([plan.legal_query, *plan.legal_keywords])
        for name, searcher in (
            ("원문", lambda: hybrid_search(plan.original_query, top_k=5)),
            ("법률 변환문", lambda: hybrid_search(plan.legal_query, top_k=5)),
            ("원문+보조 병행", lambda: hybrid_search_queries(queries, top_k=5)),
        ):
            started = time.perf_counter()
            try:
                results = searcher()
                print(f"{name}: {time.perf_counter() - started:.2f}s / {_titles(results)}")
            except Exception as exc:
                print(f"{name}: 오류 {type(exc).__name__}: {exc}")
        print(f"변환 사용={plan.used_openai}, 보조문={plan.legal_query}, 키워드={plan.legal_keywords}")


if __name__ == "__main__":
    main()
