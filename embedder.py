"""문서 벡터와 호환되는 질의 임베딩을 생성한다."""

from __future__ import annotations

from functools import lru_cache
from typing import Sequence

import numpy as np
from sentence_transformers import SentenceTransformer

import config


class EmbeddingCompatibilityError(RuntimeError):
    """저장 벡터와 질의 임베딩 설정을 비교할 수 없거나 불일치한다."""


class NvidiaEmbeddingConfigurationError(RuntimeError):
    pass


class NvidiaEmbeddingAuthenticationError(RuntimeError):
    pass


class NvidiaEmbeddingTimeoutError(RuntimeError):
    pass


class NvidiaEmbeddingAPIError(RuntimeError):
    pass


@lru_cache(maxsize=1)
def get_model() -> SentenceTransformer:
    """기존 문서/로컬 질의 모델은 프로세스에서 한 번만 로드한다."""
    return SentenceTransformer(config.EMBEDDING_MODEL)


def _validate_vector(values: object, expected_dimension: int | None, label: str) -> np.ndarray:
    vector = np.asarray(values, dtype=np.float32)
    if vector.ndim != 1 or vector.size == 0:
        raise EmbeddingCompatibilityError(f"{label} 임베딩이 1차원 벡터가 아닙니다.")
    if not np.isfinite(vector).all():
        raise EmbeddingCompatibilityError(f"{label} 임베딩에 유효하지 않은 값이 있습니다.")
    if expected_dimension is not None and vector.size != expected_dimension:
        raise EmbeddingCompatibilityError(
            f"{label} 차원 불일치: 반환={vector.size}, 저장 벡터={expected_dimension}"
        )
    return vector


def validate_query_configuration(expected_dimension: int) -> None:
    """질의 모델을 다른 문서 모델에 몰래 적용하지 않는다."""
    if config.DOCUMENT_EMBEDDING_DIMENSION != expected_dimension:
        raise EmbeddingCompatibilityError(
            "설정된 문서 임베딩 차원과 실제 PostgreSQL 차원이 다릅니다: "
            f"설정={config.DOCUMENT_EMBEDDING_DIMENSION}, DB={expected_dimension}"
        )
    if config.EMBEDDING_PROVIDER == "nvidia":
        if config.DOCUMENT_EMBEDDING_PROVIDER != "nvidia":
            raise EmbeddingCompatibilityError(
                "NVIDIA 질의 임베딩을 선택했지만 문서 벡터 provenance가 NVIDIA로 확인되지 않았습니다."
            )
        if not config.NVIDIA_EMBEDDING_MODEL:
            raise NvidiaEmbeddingConfigurationError("NVIDIA_EMBEDDING_MODEL이 설정되지 않았습니다.")
        if config.DOCUMENT_EMBEDDING_MODEL != config.NVIDIA_EMBEDDING_MODEL:
            raise EmbeddingCompatibilityError(
                "NVIDIA 질의 모델과 저장 문서 모델이 다릅니다: "
                f"질의={config.NVIDIA_EMBEDDING_MODEL}, 문서={config.DOCUMENT_EMBEDDING_MODEL}"
            )
    elif config.EMBEDDING_PROVIDER == "local":
        if config.DOCUMENT_EMBEDDING_PROVIDER != "local" or config.DOCUMENT_EMBEDDING_MODEL != config.EMBEDDING_MODEL:
            raise EmbeddingCompatibilityError(
                "로컬 질의 모델과 저장 문서 벡터 provenance가 일치하지 않습니다."
            )
    else:
        raise NvidiaEmbeddingConfigurationError(
            f"지원하지 않는 EMBEDDING_PROVIDER={config.EMBEDDING_PROVIDER!r}"
        )


def embed_documents(texts: Sequence[str]) -> np.ndarray:
    """기존 색인용 E5 문서 임베딩. 자동 재색인은 하지 않는다."""
    prefixed = [f"passage: {text}" for text in texts]
    vectors = get_model().encode(
        prefixed,
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=len(prefixed) > 10,
    )
    array = np.asarray(vectors, dtype=np.float32)
    if array.ndim != 2 or array.shape[1] != config.EMBEDDING_DIMENSION:
        raise EmbeddingCompatibilityError(
            f"문서 임베딩 차원이 설정과 다릅니다: 반환={getattr(array, 'shape', None)}, "
            f"설정={config.EMBEDDING_DIMENSION}"
        )
    return array


def _embed_query_nvidia(query: str, expected_dimension: int) -> np.ndarray:
    if not config.NVIDIA_API_KEY:
        raise NvidiaEmbeddingConfigurationError("NVIDIA_API_KEY가 설정되지 않았습니다.")
    if not config.NVIDIA_EMBEDDING_MODEL:
        raise NvidiaEmbeddingConfigurationError("NVIDIA_EMBEDDING_MODEL이 설정되지 않았습니다.")
    try:
        from openai import APIConnectionError, APITimeoutError, AuthenticationError, OpenAI
        from openai import APIStatusError

        client = OpenAI(
            api_key=config.NVIDIA_API_KEY,
            base_url=config.NVIDIA_BASE_URL,
            timeout=config.NVIDIA_TIMEOUT_SECONDS,
            max_retries=config.NVIDIA_MAX_RETRIES,
        )
        response = client.embeddings.create(
            model=config.NVIDIA_EMBEDDING_MODEL,
            input=[query],
            # NVIDIA NIM 전용 필드. E5의 query: 접두사는 붙이지 않는다.
            extra_body={"input_type": "query"},
        )
        if not response.data or not response.data[0].embedding:
            raise NvidiaEmbeddingAPIError("NVIDIA 응답에 embedding 데이터가 없습니다.")
        return _validate_vector(response.data[0].embedding, expected_dimension, "NVIDIA 질의")
    except AuthenticationError as exc:
        raise NvidiaEmbeddingAuthenticationError("NVIDIA 인증 실패(401/403)입니다.") from exc
    except APITimeoutError as exc:
        raise NvidiaEmbeddingTimeoutError("NVIDIA 임베딩 요청 시간이 초과되었습니다.") from exc
    except APIConnectionError as exc:
        raise NvidiaEmbeddingAPIError("NVIDIA 임베딩 endpoint에 연결할 수 없습니다.") from exc
    except APIStatusError as exc:
        if getattr(exc, "status_code", None) in {401, 403}:
            raise NvidiaEmbeddingAuthenticationError("NVIDIA 인증 실패(401/403)입니다.") from exc
        raise NvidiaEmbeddingAPIError(
            f"NVIDIA 임베딩 API 오류(status={getattr(exc, 'status_code', 'unknown')})"
        ) from exc


@lru_cache(maxsize=128)
def embed_query(query: str, expected_dimension: int | None = None) -> np.ndarray:
    """질의 벡터를 캐시한다. NVIDIA 사용 시 query 접두사를 붙이지 않는다."""
    if not query.strip():
        raise ValueError("임베딩할 질문이 비어 있습니다.")
    if expected_dimension is not None:
        validate_query_configuration(expected_dimension)
    if config.EMBEDDING_PROVIDER == "nvidia":
        if expected_dimension is None:
            raise EmbeddingCompatibilityError("NVIDIA 질의 임베딩에는 저장 벡터 차원이 필요합니다.")
        return _embed_query_nvidia(query, expected_dimension)
    vector = get_model().encode(
        f"query: {query}",
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=False,
    )
    return _validate_vector(vector, expected_dimension or config.EMBEDDING_DIMENSION, "로컬 질의")
