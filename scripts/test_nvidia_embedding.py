"""NVIDIA NIM 임베딩 연결/응답 차원 점검. API 키는 출력하지 않는다."""

from __future__ import annotations

import config
from embedder import _embed_query_nvidia


def main() -> None:
    expected = config.DOCUMENT_EMBEDDING_DIMENSION
    vector = _embed_query_nvidia("임베딩 연결 테스트", expected)
    print(f"NVIDIA embedding OK: model={config.NVIDIA_EMBEDDING_MODEL}, dimension={vector.size}")


if __name__ == "__main__":
    main()
