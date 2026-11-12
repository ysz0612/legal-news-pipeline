# 구글 뉴스 RSS → Kafka → 법률 필터 → 분야 분류 → 저장

네이버 API 버전 대신 이 새 프로젝트를 사용하세요. 네이버 키는 필요 없습니다.
기존 작업 폴더에 덮어쓰지 말고 새 폴더로 압축을 풀어주세요.

## 구현 범위

- 15개 뉴스사, 요청한 1차 단어 27개, 세부 분야 30개/화면용 대분류 10개.
- 원본 RSS XML 압축 보관, RSS 기사 데이터 PostgreSQL 저장, 전송 대기 목록(outbox).
- 선배님 파일 구조와 raw-news → legal-news → categorized-news 토픽 유지.
- 전송 성공 확인 후 offset 동기 커밋, 잘못된 메시지 별도 실패 토픽/DB 저장.
- 최종 DB 저장 consumer 추가. Kafka 중복 전달은 event_id/문서 ID로 흡수.
- 필터 코드는 공통 함수로 공유. Kafka 없이 로컬 필터 검토 가능.
- 기사 전문, 임베딩, PostgreSQL/Neo4j/OpenSearch, Airflow 배포는 이번 패키지에 없습니다.

## 기사 데이터의 범위 (반드시 팀과 공유)

Google RSS 제목·링크·출처·시각·설명을 수집합니다. RSS가 기사 전문을 제공하지 않습니다.
설명이 제목/출처 반복이면 content를 비우고 content_type=title_only로 저장합니다.
그 외 설명은 rss_description으로 표시하며 기사 본문이라고 부르지 않습니다.
description에 다른 관련 기사 목록이 들어 있으면 필터 오염을 막기 위해 content에는 넣지 않습니다.
body=null, body_status=not_collected. 임베딩용 전문 확보는 별도 후속 작업입니다.
url은 대부분 Google News 중계 링크입니다. 이를 언론사 원문 링크로 오인하지 않도록
url_type=google_news_redirect, original_url=null을 명시합니다. 원문 URL 디코딩/전문 추출은 구현하지 않았습니다.
현재 두 필터는 제목+사용 가능한 RSS 설명에 적용합니다. 본문에만 있는 키워드는 놓칠 수 있습니다.

## 1. 실행 환경

Windows PowerShell, Python 3.11 권장. 프로젝트 최상위(main.py가 있는 곳)에서 실행하세요.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

venv 활성화 없이 실행하므로 PowerShell 실행정책 변경이 필요 없습니다.
python 명령이 없으면 py -3.11 -m venv .venv로 실행하세요.
수집/로컬 필터 자체는 Python 표준 라이브러리만 사용합니다. confluent-kafka는 Kafka 연결에 필요합니다.
폴더 이름 kafka는 프로젝트 모듈입니다. 별도의 kafka-python 패키지는 설치하지 마세요.

## 2. Kafka 없이 기사부터 수집

```powershell
.\.venv\Scripts\python.exe main.py collect --publisher 법률신문 --days 7
.\.venv\Scripts\python.exe main.py stats
.\.venv\Scripts\python.exe main.py filter-local
.\.venv\Scripts\python.exe main.py export --include-unclassified
```

필터 전 RSS 저장부터 시작합니다. collect는 자체적으로 1/2차 필터를 실행하지 않습니다.
filter-local은 검토용이며 Kafka consumer와 동일한 함수로 분류합니다.

15개 뉴스사 모두 수집:

```powershell
.\.venv\Scripts\python.exe main.py collect --days 7
```

대량 후보를 보완하는 검색어별 수집:

```powershell
.\.venv\Scripts\python.exe main.py collect --mode expanded --days 7 --max-feeds 100
```

마지막에 표시되는 --offset 값을 같은 옵션에 추가해서 나머지를 진행하세요.
예: --offset 100, 200 ... . offset은 RSS 페이지 번호가 아니라 검색 쿼리 목록의 위치입니다.
특정 날짜 검색은 --date 2026-09-08, 특정 매체는 --publisher 법조신문처럼 지정합니다.
--publisher는 여러 번 지정할 수 있습니다. --delay 기본 2초, 최소 1초입니다.

Google RSS에 검색어당 무제한 페이지가 있는 것은 아닙니다. site/when/after/before는 검색 힌트이며
결과의 완전성·날짜 경계·개수·장기 유지가 보장되지 않습니다. 날짜별 검색도 완전한 과거 백필이 아닙니다.
필요 시 날짜별 작은 창과 검색어별 결과를 합칩니다. 사용자의 첫 필터 단어가 검색어가 되는 expanded 모드는
이미 법률 성격의 후보를 요청하지만 실제 1차 필터 판정은 consumer에서 다시 수행합니다.
Google 색인에 없는 매체/기사는 받을 수 없습니다. 15개 매체의 전량 수집 보장이 아닙니다.
실제 source 요소의 URL 도메인을 허용 목록과 비교합니다. 사이트 검색 조건만 믿지 않습니다.
누락 매체는 도메인/피드 source 구조를 확인해 settings/filters.json에서 보완하세요.
0건은 해당 뉴스사에 법률 기사가 없다는 뜻이 아닙니다.
403/429 시 자동 우회 없이 중단합니다. 서버/학교 네트워크 정책을 확인하고 나중에 재시도하세요.
일시 오류는 최대 3회 시도하고, 실패 피드는 DB news_feed_logs 테이블에 기록합니다.

## 3. 저장 위치

- data/rss_raw/*.xml.gz: 수집한 RSS 원본
- PostgreSQL: news_articles, kafka_outbox, news_processing_failures, news_feed_logs
- data/exports/*.jsonl: 내보낸 기사 (한 줄에 기사 한 개, UTF-8)
- export: 분류된 기사만 / export --include-unclassified: 미분류도 포함
- export --raw: 필터 전 RSS 기사 내보내기
- synthetic 테스트 기사는 일반 export에서 제외합니다.

동일 RSS GUID(없으면 링크) + 매체명으로 문서 ID를 정합니다.
수집시각이 바뀌어도 제목/내용 등이 같으면 중복 전송 작업을 만들지 않습니다.
다른 Google 링크로 들어온 같은 원문, 전재 기사, 동일 사건의 여러 보도까지 완벽히 병합하지 않습니다.
RSS만으로 동일성 판단이 불확실하므로 다른 자료를 무리하게 삭제하지 않습니다.

## 4. Kafka 실행 (이미 선배님 서버가 있으면 그대로 사용)

새 서버가 필요한 경우 Docker Desktop 실행 후:

```powershell
docker compose up -d
docker compose ps
docker compose logs --tail 80 kafka
```

healthy가 된 뒤:

```powershell
.\.venv\Scripts\python.exe -m kafka.init_topics
```

이미 Kafka가 9092에 있으면 새 Compose를 실행하지 말고 기존 서버에 토픽만 만드세요.
Windows에서 모두 실행할 때 .env 기본 localhost:9092를 씁니다.
다른 PC에서 접속 시 KAFKA_HOST=공용PC_IP, KAFKA_BOOTSTRAP_SERVERS=공용PC_IP:9092로 바꾸세요.
Docker 내 클라이언트는 같은 네트워크에서 KAFKA_BOOTSTRAP_SERVERS=kafka:29092를 사용합니다.
Kafka 광고 주소 변경 후 docker compose up -d를 다시 실행합니다.
실습용 단일 브로커/복제수1/보관7일/PLAINTEXT 구성입니다. 외부 공개하지 말고 팀 PC만 접근 허용하세요.
메시지 보관은 무기한이 아니며 단일 서버/디스크 고장에 대한 고가용성 보장이 없습니다.
down -v/볼륨 삭제 금지. Docker 볼륨과 PostgreSQL 데이터는 별도 백업이 필요합니다.

## 5. 선배님 코드와 같은 실행 구조

프로젝트 폴더에서 PowerShell 창 3개를 열어 각각 실행하세요.

창 1 — 1차 법률 필터:

```powershell
.\.venv\Scripts\python.exe -m kafka.legal_filter_consumer
```

창 2 — 2차 분야 분류:

```powershell
.\.venv\Scripts\python.exe -m kafka.category_consumer
```

창 3 — 최종 PostgreSQL 저장:

```powershell
.\.venv\Scripts\python.exe -m kafka.storage_consumer
```

창 4 — 저장된 원본을 Kafka로 전송:

```powershell
.\.venv\Scripts\python.exe main.py publish
```

수집 후 바로 전송하려면:

```powershell
.\.venv\Scripts\python.exe main.py collect --days 1 --publish
```

네트워크 없이 테스트 메시지 만들기:

```powershell
.\.venv\Scripts\python.exe main.py sample
.\.venv\Scripts\python.exe main.py publish
```

consumer 창에 제목/통과상태가 나오면:

```powershell
.\.venv\Scripts\python.exe main.py stats
.\.venv\Scripts\python.exe main.py export --include-unclassified
```

DataGrip에서는 PostgreSQL 데이터소스로 `law_db`에 접속하면 됩니다.
선배님과 다른 점: main guard, 패키지 import, metadata 보존, 30개 분류 설정 분리,
전송 확인 후 commit, DLQ, 미분류 보관, 최종 저장 consumer를 추가했습니다.

## 6. 실패 및 재분류

전송 장애: publish 오류 후 로컬 outbox는 남습니다. 연결 복구 뒤 publish 재실행.
consumer 전송/DB 장애: 프로세스가 중단되고 현재 offset은 커밋하지 않습니다. 원인 해결 후 재시작.
잘못된 JSON/필수필드: failed-news + news_processing_failures 테이블에 기록한 뒤 진행합니다.
failures의 failure_id를 이용하여 1회 재전송할 수 있습니다.

```powershell
.\.venv\Scripts\python.exe main.py replay-failure "legal-filter:raw-news:0:5"
```

입력 자체가 잘못되었으면 --corrected-file corrected_article.json을 붙여 수정된 JSON을 전송하세요.
실패 토픽의 순환 자동 재전송은 하지 않습니다.
브로커 재전송으로 이벤트가 중복 전달될 수 있습니다. at-least-once + 저장 중복 제거 방식입니다.
Kafka consumer들은 offset 저장 전 전송 결과/DB 커밋을 확인합니다. publish 중간에 끊겨도 재실행할 수 있습니다.
특히 원본 저장과 kafka_outbox 생성은 같은 PostgreSQL 트랜잭션으로 처리합니다.

필터 수정 후 즉시 검토: main.py filter-local
Kafka로 전체 재분류: consumer 모두 재시작하여 새 설정 로드 → main.py requeue → main.py publish
재분류는 DB에 남아 있는 원본만 대상입니다. 사라진 RSS 기사를 다시 가져오는 것은 아닙니다.
Kafka에서 과거 이벤트를 다시 받는 경우 document collected_at으로 더 오래된 결과의 덮어쓰기를 막습니다.
같은 기사/같은 시각에서 여러 필터 버전을 동시에 처리하지 마세요. 재분류 시 기존 backlog를 먼저 소진하고 작업을 시작하세요.

## 7. Airflow 연결 위치

현재는 수동 수집 실행까지입니다. Airflow DAG/서버는 이번 패키지에 설치하지 않습니다.
다음 단계에서 Airflow가 `main.py collect --days 1`을 실행한 다음 `main.py publish`를 실행하게 연결하면 됩니다.
두 filter consumer와 storage consumer는 상시 프로세스이며 DAG 안에서 무한루프로 실행하면 안 됩니다.
Airflow가 Docker에서 실행되면 코드/저장 폴더 마운트와 kafka:29092 연결을 별도로 설정해야 합니다.
PostgreSQL은 Docker 볼륨을 통해 운영하고, PC 간 파일 공유 방식으로 데이터베이스를 공유하지 마세요.

## 8. 검증과 제한

```powershell
.\.venv\Scripts\python.exe -m unittest -v
```

테스트는 RSS XML 파싱, 원문/요약 구분, source 검증, 30개 설정, 다중분류,
민사 판결의 형사 오분류 방지, 미분류, Kafka 전송 실패 시 대기 보존, 저장 중복 방지를 확인합니다.
제작 환경의 네트워크 제한으로 Google RSS 실접속이 완료되지 않았습니다.
Docker가 없어 실제 Kafka 기동·종단 송수신도 미검증입니다. 학교 PC에서 위 명령으로 확인하세요.
임베딩 전에 본문 확보와 이용 범위, 법률 근거 대조가 필요합니다. 키워드 분류 결과가 법적 판단/팩트체크는 아닙니다.

공식 Kafka 참고:
- https://kafka.apache.org/41/getting-started/docker/
- https://docs.confluent.io/kafka-clients/python/current/overview.html
RSS 엔드포인트: https://news.google.com/rss/search
