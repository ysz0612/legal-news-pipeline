"""검토를 거친 고정 동의어 사전. 외부/AI 기반 확장을 하지 않는다."""

from __future__ import annotations


CONTROLLED_SYNONYMS: dict[str, list[str]] = {
    "학교폭력": ["학교폭력", "학폭"],
    "가상자산": ["가상자산", "암호화폐"],
    "개인정보": ["개인정보", "개인정보유출"],
    "전세사기": ["전세사기"],
}


def expand_query(query: str) -> list[str]:
    """정확히 등록된 검색어만 확장하고, 나머지는 원문 그대로 반환한다."""
    normalized = query.strip()
    if not normalized:
        return []
    return CONTROLLED_SYNONYMS.get(normalized, [normalized]).copy()

