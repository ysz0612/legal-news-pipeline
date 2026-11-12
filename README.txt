프로젝트 공유 메모
==================

이 폴더는 Google News RSS 기사 수집 및 법률 분야 분류 프로젝트입니다.

필요한 것
----------
- Python 3.11 권장
- requirements.txt 설치
- .env.example을 복사해 .env 생성 후 팀 환경에 맞는 값 입력
- Kafka와 PostgreSQL을 사용할 경우 Docker Desktop 및 compose.yaml
- 실행 방법과 제한사항은 README.md 참고

기본 준비
----------
1. python -m venv .venv
2. .\.venv\Scripts\python.exe -m pip install -r requirements.txt
3. Copy-Item .env.example .env
4. .\.venv\Scripts\python.exe -m unittest -v

정리한 항목
------------
- 기존 실행 중 생성된 RSS 원본, 로그, Elasticsearch 데이터
- 데이터베이스 덤프와 복구 백업
- 구형 샘플 프로젝트 복사본
- 개인용 .env 및 로컬 IDE/임시 파일

주의
----
- .env에는 API 키, 토큰, 비밀번호를 저장하며 Git에 올리지 않습니다.
- 공유 전에 기존 키와 토큰은 폐기하고 새 값으로 발급하세요.
- data 폴더의 JSON은 분류 설정/검토 자료이므로 필요 없으면 별도로 제외하세요.
- Docker 볼륨과 운영 데이터베이스는 이 소스 폴더와 별도로 관리합니다.
