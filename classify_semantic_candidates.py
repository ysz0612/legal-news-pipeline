import json
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI


# ============================================================
# PATH / ENV
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

INPUT_FILE = DATA_DIR / "legal_semantic_candidates.json"
OUTPUT_FILE = DATA_DIR / "legal_semantic_categorized_080.json"

THRESHOLD = 0.80
MODEL = "gpt-5.6-luna"

MAX_RETRIES = 3
SAVE_EVERY = 10


# ============================================================
# E DRIVE CACHE / TEMP
# ============================================================

E_TEMP = Path(r"E:\temp")
E_HF = Path(r"E:\huggingface")
E_TORCH = Path(r"E:\torch-cache")

for directory in [
    DATA_DIR,
    E_TEMP,
    E_HF,
    E_TORCH,
]:
    directory.mkdir(
        parents=True,
        exist_ok=True
    )

os.environ["TEMP"] = str(E_TEMP)
os.environ["TMP"] = str(E_TEMP)

os.environ.setdefault(
    "HF_HOME",
    str(E_HF)
)

os.environ.setdefault(
    "TRANSFORMERS_CACHE",
    str(E_HF / "transformers")
)

os.environ.setdefault(
    "TORCH_HOME",
    str(E_TORCH)
)


# ============================================================
# .env LOAD
# ============================================================

ENV_FILE = BASE_DIR / ".env"

load_dotenv(
    ENV_FILE,
    override=False
)


# ============================================================
# CATEGORY
# ============================================================

CATEGORIES = [
    ("C01", "형사"),
    ("C02", "노동"),
    ("C03", "민사·채권"),
    ("C04", "부동산·임대차"),
    ("C05", "금융·도산"),
    ("C06", "개인정보·IT"),
    ("C07", "지식재산"),
    ("C08", "가족·가사"),
    ("C09", "상속"),
    ("C10", "회사·상법"),
    ("C11", "행정"),
    ("C12", "조세"),
    ("C13", "교통"),
    ("C14", "의료"),
    ("C15", "건설·건축"),
    ("C16", "소비자·전자상거래"),
    ("C17", "보험"),
    ("C18", "교육"),
    ("C19", "공정거래"),
    ("C20", "집행·보전"),
    ("C21", "헌법·기본권"),
    ("C22", "수사·형사절차"),
    ("C23", "환경·토지"),
    ("C24", "출입국·국적"),
    ("C25", "공직·선거"),
    ("C26", "병역·군사"),
    ("C27", "산업안전·중대재해"),
    ("C28", "복지·연금"),
    ("C29", "식품·의약품"),
    ("C30", "가상자산·전자금융"),
    ("C31", "기타"),
]

CATEGORY_MAP = {
    category_id: name
    for category_id, name in CATEGORIES
}

CATEGORY_TEXT = "\n".join(
    f"{category_id}: {name}"
    for category_id, name in CATEGORIES
)


# ============================================================
# OPENAI CLIENT
# ============================================================

def get_client():
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        raise RuntimeError(
            "\nOPENAI_API_KEY를 찾을 수 없습니다.\n"
            f".env 위치: {ENV_FILE}\n"
        )

    return OpenAI(
        api_key=api_key
    )


# ============================================================
# JSON LOAD
# ============================================================

def load_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(
            f"파일이 없습니다:\n{path}"
        )

    with path.open(
        "r",
        encoding="utf-8"
    ) as file:
        return json.load(file)


# ============================================================
# LOAD CANDIDATES >= 0.80
# ============================================================

def load_candidates():
    raw_items = load_json(
        INPUT_FILE
    )

    candidates = []

    skipped_no_id = 0
    skipped_low_score = 0

    for item in raw_items:
        try:
            similarity = float(
                item.get(
                    "max_similarity",
                    0
                )
            )
        except Exception:
            similarity = 0.0

        if similarity < THRESHOLD:
            skipped_low_score += 1
            continue

        document_id = item.get(
            "candidate_document_id"
        )

        if not document_id:
            skipped_no_id += 1
            continue

        candidate = {
            "document_id":
                str(document_id),

            "title":
                (
                    item.get(
                        "candidate_title"
                    )
                    or ""
                ).strip(),

            "description":
                (
                    item.get(
                        "candidate_description"
                    )
                    or ""
                ).strip(),

            "filter_status":
                item.get(
                    "candidate_filter_status"
                ),

            "max_similarity":
                similarity,

            "avg_top_k_similarity":
                item.get(
                    "avg_top_k_similarity"
                ),

            "matched_legal_articles":
                item.get(
                    "matched_legal_articles",
                    []
                ),
        }

        candidates.append(
            candidate
        )

    candidates.sort(
        key=lambda item:
            item["max_similarity"],
        reverse=True
    )

    print(
        f"[LOAD] raw candidates      : "
        f"{len(raw_items):,}"
    )

    print(
        f"[LOAD] below threshold     : "
        f"{skipped_low_score:,}"
    )

    print(
        f"[LOAD] missing candidate id: "
        f"{skipped_no_id:,}"
    )

    return candidates


# ============================================================
# EXISTING RESULT LOAD
# ============================================================

def load_existing_results():
    if not OUTPUT_FILE.exists():
        return []

    try:
        with OUTPUT_FILE.open(
            "r",
            encoding="utf-8"
        ) as file:
            data = json.load(file)

        if isinstance(
            data,
            list
        ):
            return data

    except Exception as exc:
        print(
            f"[WARN] 기존 결과 파일 읽기 실패: {exc}"
        )

    return []


# ============================================================
# SAVE
# ============================================================

def save_results(results):
    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    temp_file = OUTPUT_FILE.with_suffix(
        ".tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            results,
            file,
            ensure_ascii=False,
            indent=2
        )

    temp_file.replace(
        OUTPUT_FILE
    )


# ============================================================
# CATEGORY NORMALIZE
# ============================================================

def normalize_category(category):
    if not isinstance(
        category,
        dict
    ):
        return None

    category_id = (
        category.get("id")
        or category.get(
            "category_id"
        )
    )

    if category_id not in CATEGORY_MAP:
        return None

    return {
        "id":
            category_id,

        "name":
            CATEGORY_MAP[
                category_id
            ],
    }


def normalize_classification(result):
    if not isinstance(
        result,
        dict
    ):
        raise ValueError(
            "GPT 응답이 JSON 객체가 아닙니다."
        )

    primary = normalize_category(
        result.get(
            "primary_category"
        )
    )

    if primary is None:
        raise ValueError(
            "primary_category가 올바르지 않습니다."
        )

    raw_secondaries = result.get(
        "secondary_categories",
        []
    )

    secondaries = []

    seen = {
        primary["id"]
    }

    if isinstance(
        raw_secondaries,
        list
    ):
        for category in raw_secondaries:
            parsed = normalize_category(
                category
            )

            if not parsed:
                continue

            if parsed["id"] in seen:
                continue

            seen.add(
                parsed["id"]
            )

            secondaries.append(
                parsed
            )

            if len(
                secondaries
            ) >= 2:
                break

    try:
        confidence = float(
            result.get(
                "confidence",
                0
            )
        )
    except Exception:
        confidence = 0.0

    confidence = max(
        0.0,
        min(
            confidence,
            1.0
        )
    )

    reason = str(
        result.get(
            "reason",
            ""
        )
    ).strip()

    return {
        "primary_category":
            primary,

        "secondary_categories":
            secondaries,

        "confidence":
            round(
                confidence,
                4
            ),

        "reason":
            reason,
    }


# ============================================================
# REFERENCE TITLES
# ============================================================

def build_reference_text(article):
    matched_articles = article.get(
        "matched_legal_articles",
        []
    )

    lines = []

    for matched in matched_articles[:5]:
        title = (
            matched.get(
                "title"
            )
            or ""
        ).strip()

        similarity = matched.get(
            "similarity"
        )

        if not title:
            continue

        if similarity is None:
            lines.append(
                f"- {title}"
            )
        else:
            try:
                lines.append(
                    f"- {title} "
                    f"(유사도 {float(similarity):.4f})"
                )
            except Exception:
                lines.append(
                    f"- {title}"
                )

    if not lines:
        return "없음"

    return "\n".join(
        lines
    )


# ============================================================
# GPT CLASSIFICATION
# ============================================================

def classify_article(
    client,
    article
):
    title = (
        article.get(
            "title"
        )
        or ""
    ).strip()

    description = (
        article.get(
            "description"
        )
        or ""
    ).strip()

    reference_text = (
        build_reference_text(
            article
        )
    )

    prompt = f"""
당신은 대한민국 법률 뉴스 전문 분류기입니다.

현재 기사는 기존 법률 뉴스와 Nemotron 임베딩 유사도가
0.80 이상인 기사입니다.

다만 유사도가 높다는 사실만 보고 카테고리를 결정하지 말고,
현재 기사 자체의 핵심 법률 쟁점을 가장 중요하게 판단하세요.

아래 카테고리 중 primary_category를 정확히 1개 선택하세요.

{CATEGORY_TEXT}

분류 규칙:

1. primary_category는 반드시 정확히 1개 선택합니다.

2. secondary_categories는 필요한 경우에만 0~2개 선택합니다.

3. 제목에 경찰, 검찰, 법원, 판사 등의 단어가 있다는 이유만으로
   특정 카테고리를 선택하면 안 됩니다.

4. 기사에서 실제 핵심으로 다루는 법적 쟁점을 기준으로 판단합니다.

5. 범죄의 성립, 범죄행위, 형벌 또는 처벌 자체가 핵심이면
   C01 형사입니다.

6. 압수수색, 영장, 체포, 구속, 송치, 기소,
   수사기관의 수사 과정 또는 형사절차가 핵심이면
   C22 수사·형사절차입니다.

7. 선거법, 선거운동, 후보자, 공직자의 선거 관련 법적 쟁점이면
   C25 공직·선거입니다.

8. 헌법상 권리, 기본권, 표현의 자유, 평등권,
   차별 문제 등이 핵심이면
   C21 헌법·기본권입니다.

9. 정부 또는 행정기관의 허가, 처분, 제재,
   행정소송 등이 핵심이면
   C11 행정입니다.

10. 회생, 파산, 채무조정, 금융기관의 법률문제 등이 핵심이면
    C05 금융·도산입니다.

11. 노동조합, 해고, 임금, 근로계약,
    노동관계법 문제가 핵심이면
    C02 노동입니다.

12. 개인정보보호, 정보통신, 데이터,
    IT 규제 문제가 핵심이면
    C06 개인정보·IT입니다.

13. 기업결합, 시장지배력, 담합,
    경쟁제한 문제 등이 핵심이면
    C19 공정거래입니다.

14. 어느 카테고리에도 적절히 들어가지 않는 경우에만
    C31 기타를 선택합니다.

15. 아래 유사 기사는 참고만 하십시오.
    유사 기사의 카테고리를 그대로 따라가서는 안 됩니다.

16. 반드시 JSON 형식으로만 응답합니다.

현재 기사 제목:
{title}

현재 기사 설명:
{description}

유사도가 높은 기존 법률 기사:
{reference_text}

출력 JSON 형식:

{{
  "primary_category": {{
    "id": "C22",
    "name": "수사·형사절차"
  }},
  "secondary_categories": [],
  "confidence": 0.95,
  "reason": "수사 절차가 기사의 핵심 내용이기 때문"
}}
""".strip()

    last_error = None

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):
        try:
            response = (
                client
                .chat
                .completions
                .create(
                    model=MODEL,

                    messages=[
                        {
                            "role":
                                "system",

                            "content":
                                (
                                    "대한민국 법률 뉴스를 "
                                    "지정된 법률 카테고리로 "
                                    "분류하세요. "
                                    "반드시 JSON만 반환하세요."
                                ),
                        },
                        {
                            "role":
                                "user",

                            "content":
                                prompt,
                        },
                    ],

                    response_format={
                        "type":
                            "json_object"
                    },

                    # 중요:
                    # gpt-5.6-luna는 temperature=0 미지원
                    # 기본값을 사용하기 위해 temperature 파라미터 자체를 넣지 않음
                )
            )

            content = (
                response
                .choices[0]
                .message
                .content
            )

            if not content:
                raise ValueError(
                    "OpenAI 응답이 비어 있습니다."
                )

            parsed = json.loads(
                content
            )

            normalized = (
                normalize_classification(
                    parsed
                )
            )

            return normalized

        except Exception as exc:
            last_error = exc

            print(
                f"    [RETRY "
                f"{attempt}/"
                f"{MAX_RETRIES}] "
                f"{exc}"
            )

            if attempt < MAX_RETRIES:
                time.sleep(
                    attempt * 2
                )

    raise RuntimeError(
        f"OpenAI 분류 실패: {last_error}"
    )


# ============================================================
# RESULT SUMMARY
# ============================================================

def print_category_summary(results):
    counts = {}

    for result in results:
        try:
            primary = (
                result[
                    "classification"
                ][
                    "primary_category"
                ]
            )

            category_id = (
                primary["id"]
            )

            category_name = (
                primary["name"]
            )

            key = (
                f"{category_id} "
                f"{category_name}"
            )

            counts[key] = (
                counts.get(
                    key,
                    0
                )
                + 1
            )

        except Exception:
            continue

    print()
    print(
        "CATEGORY COUNTS"
    )

    print(
        "-" * 80
    )

    for key, count in sorted(
        counts.items(),
        key=lambda item: (
            -item[1],
            item[0]
        )
    ):
        print(
            f"{key:<30} "
            f"{count:>6,}"
        )


# ============================================================
# MAIN
# ============================================================

def main():
    print()
    print("=" * 80)
    print(
        "SEMANTIC >= 0.80 "
        "CATEGORY CLASSIFICATION"
    )
    print("=" * 80)

    print(
        f"PROJECT  : {BASE_DIR}"
    )

    print(
        f"INPUT    : {INPUT_FILE}"
    )

    print(
        f"OUTPUT   : {OUTPUT_FILE}"
    )

    print(
        f"TEMP     : {E_TEMP}"
    )

    print(
        f"THRESHOLD: {THRESHOLD}"
    )

    print(
        f"MODEL    : {MODEL}"
    )

    print()

    # --------------------------------------------------------
    # OpenAI
    # --------------------------------------------------------

    client = get_client()

    print(
        "[OK] OPENAI_API_KEY loaded"
    )

    # --------------------------------------------------------
    # Candidate load
    # --------------------------------------------------------

    candidates = (
        load_candidates()
    )

    print(
        f"[LOAD] semantic >= "
        f"{THRESHOLD:.2f}: "
        f"{len(candidates):,}"
    )

    if not candidates:
        print()
        print(
            "[DONE] 분류할 후보가 없습니다."
        )
        return

    # --------------------------------------------------------
    # Resume
    # --------------------------------------------------------

    existing_results = (
        load_existing_results()
    )

    completed_ids = set()

    for item in existing_results:
        document_id = (
            item.get(
                "document_id"
            )
        )

        if document_id:
            completed_ids.add(
                str(document_id)
            )

    remaining = [
        candidate
        for candidate in candidates
        if candidate[
            "document_id"
        ]
        not in completed_ids
    ]

    print(
        f"[LOAD] already classified: "
        f"{len(completed_ids):,}"
    )

    print(
        f"[LOAD] remaining         : "
        f"{len(remaining):,}"
    )

    print()

    results = list(
        existing_results
    )

    success = 0
    failed = 0

    total = len(
        remaining
    )

    # --------------------------------------------------------
    # Classification
    # --------------------------------------------------------

    for index, article in enumerate(
        remaining,
        start=1
    ):
        document_id = (
            article[
                "document_id"
            ]
        )

        similarity = float(
            article[
                "max_similarity"
            ]
        )

        title = (
            article[
                "title"
            ]
        )

        try:
            classification = (
                classify_article(
                    client,
                    article
                )
            )

            primary = (
                classification[
                    "primary_category"
                ]
            )

            result = {
                "document_id":
                    document_id,

                "title":
                    title,

                "description":
                    article[
                        "description"
                    ],

                "filter_status":
                    article[
                        "filter_status"
                    ],

                "max_similarity":
                    round(
                        similarity,
                        6
                    ),

                "avg_top_k_similarity":
                    article.get(
                        "avg_top_k_similarity"
                    ),

                "classification":
                    classification,
            }

            results.append(
                result
            )

            completed_ids.add(
                document_id
            )

            success += 1

            print(
                f"[{index}/{total}] "
                f"sim={similarity:.4f} "
                f"→ "
                f"{primary['id']} "
                f"{primary['name']} "
                f"conf="
                f"{classification['confidence']:.2f}"
            )

            if title:
                print(
                    f"    {title[:100]}"
                )

        except KeyboardInterrupt:
            print()
            print(
                "[STOP] 사용자가 중단했습니다."
            )

            print(
                "[SAVE] 현재까지 결과 저장 중..."
            )

            save_results(
                results
            )

            print(
                f"[SAVE] {len(results):,}건 저장 완료"
            )

            return

        except Exception as exc:
            failed += 1

            print(
                f"[{index}/{total}] "
                f"[ERROR] "
                f"{document_id}"
            )

            print(
                f"    {exc}"
            )

        # ----------------------------------------------------
        # Intermediate save
        # ----------------------------------------------------

        if (
            index % SAVE_EVERY
            == 0
        ):
            save_results(
                results
            )

            print(
                f"    [SAVE] "
                f"{len(results):,} "
                f"results"
            )

    # --------------------------------------------------------
    # Final save
    # --------------------------------------------------------

    save_results(
        results
    )

    # --------------------------------------------------------
    # Final result
    # --------------------------------------------------------

    print()
    print("=" * 80)
    print(
        "RESULT"
    )
    print("=" * 80)

    print(
        f"TARGET TOTAL       : "
        f"{len(candidates):,}"
    )

    print(
        f"ALREADY CLASSIFIED : "
        f"{len(candidates) - len(remaining):,}"
    )

    print(
        f"NEW SUCCESS        : "
        f"{success:,}"
    )

    print(
        f"FAILED             : "
        f"{failed:,}"
    )

    print(
        f"SAVED TOTAL        : "
        f"{len(results):,}"
    )

    print_category_summary(
        results
    )

    print()
    print(
        f"OUTPUT:"
    )

    print(
        OUTPUT_FILE
    )

    print()
    print(
        "DB MODIFIED: NO"
    )


if __name__ == "__main__":
    main()