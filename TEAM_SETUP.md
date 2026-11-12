# 팀원용 실행 튜토리얼

Windows + PowerShell + Python 3.13 기준입니다.

## 1. 압축 해제와 가상환경

ZIP을 압축 해제한 프로젝트 폴더에서 PowerShell을 엽니다.

```powershell
py -3.13 --version
py -3.13 -m venv .venv313
.\.venv313\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

PyCharm 인터프리터는 `.venv313\Scripts\python.exe`를 선택합니다.

## 2. 환경변수

`.env`의 PostgreSQL·Elasticsearch 값을 확인합니다. OpenAI를 사용할 때만 다음을 설정합니다.

```env
OPENAI_API_KEY=로컬에서만 설정
OPENAI_QUERY_MODEL=사용할 질문 변환 모델
OPENAI_ANSWER_MODEL=사용할 답변 모델
```

NVIDIA 질문 임베딩을 사용할 경우 저장 문서 벡터의 모델·차원을 먼저 확인한 뒤 설정합니다.

```env
EMBEDDING_PROVIDER=nvidia
NVIDIA_API_KEY=로컬에서만 설정
NVIDIA_BASE_URL=https://integrate.api.nvidia.com/v1
NVIDIA_EMBEDDING_MODEL=실제 저장 문서 벡터 모델
DOCUMENT_EMBEDDING_PROVIDER=nvidia
DOCUMENT_EMBEDDING_MODEL=실제 저장 문서 벡터 모델
DOCUMENT_EMBEDDING_DIMENSION=실제 저장 vector 차원
```

현재 기본 데이터는 E5 768차원입니다. 확인되지 않은 모델을 임의로 지정하지 않습니다.

## 3. Docker와 데이터 준비

Docker Desktop을 먼저 실행합니다.

```powershell
docker compose up -d
docker compose ps
.\scripts\check_services.ps1
.\.venv313\Scripts\python.exe scripts\inspect_embeddings.py
```

샘플 데이터 색인:

```powershell
.\.venv313\Scripts\python.exe indexer.py
```

외부 JSON 색인:

```powershell
.\.venv313\Scripts\python.exe indexer.py "C:\path\to\articles.json"
```

`legal_data.dump`는 ZIP에 포함되지 않습니다. 별도 DB로 복원하고 모델·차원·입력 설정을 확인한 뒤 연결합니다.

## 4. 실행

```powershell
# 검색 전용
.\.venv313\Scripts\python.exe main.py

# 질문 변환 + 근거 기반 답변
.\.venv313\Scripts\python.exe main.py --answer

# NVIDIA 연결 점검
.\.venv313\Scripts\python.exe scripts\test_nvidia_embedding.py
```

## 5. 문제 해결

- Docker 오류: Docker Desktop 실행 후 `docker compose ps` 재확인
- 모듈 오류: `.venv313\Scripts\python.exe`로 실행
- `EmbeddingCompatibilityError`: 저장 벡터 모델·차원과 `.env`가 불일치
- OpenAI/NVIDIA 인증 오류: 키를 로컬 `.env`에만 설정
- Nori 오류: `docker compose up -d --build` 후 점검 스크립트 실행

`docker compose down -v`는 DB와 색인을 삭제하므로 실행하지 않습니다.
