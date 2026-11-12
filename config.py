"""애플리케이션 공통 설정."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "legal_news")
POSTGRES_USER = os.getenv("POSTGRES_USER", "legal_news")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "legal_news_password")

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ELASTICSEARCH_INDEX = "legal-news"

EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "local").lower()
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-base")
EMBEDDING_DIMENSION = int(os.getenv("EMBEDDING_DIMENSION", "768"))
# 이 값은 저장된 문서 벡터의 provenance를 명시한다. 차원만으로 모델을 추정하지 않는다.
DOCUMENT_EMBEDDING_PROVIDER = os.getenv("DOCUMENT_EMBEDDING_PROVIDER", "local").lower()
DOCUMENT_EMBEDDING_MODEL = os.getenv("DOCUMENT_EMBEDDING_MODEL", EMBEDDING_MODEL)
DOCUMENT_EMBEDDING_DIMENSION = int(os.getenv("DOCUMENT_EMBEDDING_DIMENSION", str(EMBEDDING_DIMENSION)))
NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY")
NVIDIA_BASE_URL = os.getenv("NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1")
NVIDIA_EMBEDDING_MODEL = os.getenv("NVIDIA_EMBEDDING_MODEL")
NVIDIA_TIMEOUT_SECONDS = float(os.getenv("NVIDIA_TIMEOUT_SECONDS", "20"))
NVIDIA_MAX_RETRIES = int(os.getenv("NVIDIA_MAX_RETRIES", "0"))
CHUNK_SIZE = 700
CHUNK_OVERLAP = 80
RRF_K = 60

# 답변 생성은 명시적으로 설정한 경우에만 활성화한다. 검색 전용 실행에는
# API 키나 모델 설정이 필요하지 않다.
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL")
OPENAI_QUERY_MODEL = os.getenv("OPENAI_QUERY_MODEL") or OPENAI_MODEL
OPENAI_ANSWER_MODEL = os.getenv("OPENAI_ANSWER_MODEL") or OPENAI_MODEL
OPENAI_TIMEOUT_SECONDS = float(os.getenv("OPENAI_TIMEOUT_SECONDS", "20"))
OPENAI_MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "0"))
ANSWER_INCLUDE_VIRTUAL = os.getenv("ANSWER_INCLUDE_VIRTUAL", "false").lower() in {
    "1", "true", "yes", "y"
}
ANSWER_MAX_CHARS = int(os.getenv("ANSWER_MAX_CHARS", "800"))
ANSWER_OUTPUT_MAX_CHARS = int(os.getenv("ANSWER_OUTPUT_MAX_CHARS", "1800"))

SAMPLE_ARTICLES_PATH = BASE_DIR / "sample_articles.json"
