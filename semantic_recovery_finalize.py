import json
from collections import Counter
from pathlib import Path


INPUT_FILE = Path(
    "data/legal_semantic_refined.json"
)

OUTPUT_FILE = Path(
    "data/legal_semantic_final_review.json"
)


# ============================================================
# 프로젝트 고정 카테고리
# ============================================================

CATEGORY_ID_MAP = {
    "형사": "C01",
    "노동": "C02",
    "민사·채권": "C03",
    "부동산·임대차": "C04",
    "금융·도산": "C05",
    "개인정보·IT": "C06",
    "지식재산": "C07",
    "가족·가사": "C08",
    "상속": "C09",
    "회사·상법": "C10",
    "행정": "C11",
    "조세": "C12",
    "교통": "C13",
    "의료": "C14",
    "건설·건축": "C15",
    "소비자·전자상거래": "C16",
    "보험": "C17",
    "교육": "C18",
    "공정거래": "C19",
    "집행·보전": "C20",
    "헌법·기본권": "C21",
    "수사·형사절차": "C22",
    "환경·토지": "C23",
    "출입국·국적": "C24",
    "공직·선거": "C25",
    "병역·군사": "C26",
    "산업안전·중대재해": "C27",
    "복지·연금": "C28",
    "식품·의약품": "C29",
    "가상자산·전자금융": "C30",
    "기타": "C31",
}


# ============================================================
# 카테고리 보정
# ============================================================

def fix_category(category):
    if not category:
        return None

    name = category.get("name")

    if not name:
        return None

    category_id = (
        category.get("id")
        or CATEGORY_ID_MAP.get(name)
    )

    return {
        "id": category_id,
        "name": name,
        "role": "primary",
    }


# ============================================================
# MATCH 카테고리도 보정
# ============================================================

def fix_matches(matches):
    result = []

    for match in matches or []:
        copied = dict(match)

        copied["primary_category"] = (
            fix_category(
                match.get(
                    "primary_category"
                )
            )
        )

        result.append(copied)

    return result


# ============================================================
# 최종 판정
# ============================================================

def classify(item):
    similarity = float(
        item.get(
            "max_similarity",
            0
        )
    )

    avg_top_k = float(
        item.get(
            "avg_top_k_similarity",
            0
        )
    )

    vote_count = int(
        item.get(
            "category_vote_count",
            0
        )
    )

    vote_ratio = float(
        item.get(
            "category_vote_ratio",
            0
        )
    )

    confidence = float(
        item.get(
            "confidence",
            0
        )
    )

    # --------------------------------------------------------
    # AUTO
    # --------------------------------------------------------

    if (
        similarity >= 0.90
        and vote_count >= 4
        and vote_ratio >= 0.80
        and avg_top_k >= 0.80
        and confidence >= 0.80
    ):
        return "AUTO"

    # --------------------------------------------------------
    # REVIEW
    # --------------------------------------------------------

    if (
        similarity >= 0.85
        and vote_count >= 3
        and vote_ratio >= 0.60
        and avg_top_k >= 0.76
        and confidence >= 0.75
    ):
        return "REVIEW"

    return "HOLD"


# ============================================================
# 판정 이유
# ============================================================

def build_reason(item, decision):
    sim = float(
        item.get(
            "max_similarity",
            0
        )
    )

    avg = float(
        item.get(
            "avg_top_k_similarity",
            0
        )
    )

    votes = int(
        item.get(
            "category_vote_count",
            0
        )
    )

    confidence = float(
        item.get(
            "confidence",
            0
        )
    )

    duplicate = bool(
        item.get(
            "likely_duplicate",
            False
        )
    )

    reasons = [
        f"top1={sim:.4f}",
        f"top5_avg={avg:.4f}",
        f"category_vote={votes}/5",
        f"confidence={confidence:.4f}",
    ]

    if duplicate:
        reasons.append(
            "similar_title=True"
        )

    if decision == "AUTO":
        reasons.append(
            "strong_semantic_match"
        )

    elif decision == "REVIEW":
        reasons.append(
            "manual_review_recommended"
        )

    else:
        reasons.append(
            "insufficient_evidence"
        )

    return ", ".join(reasons)


# ============================================================
# 실행
# ============================================================

def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"파일이 없습니다: "
            f"{INPUT_FILE.resolve()}"
        )

    with INPUT_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:
        items = json.load(file)

    results = []

    category_counter = Counter()

    missing_category = 0

    for item in items:
        fixed_category = (
            fix_category(
                item.get(
                    "suggested_category"
                )
            )
        )

        if fixed_category is None:
            missing_category += 1

        elif fixed_category.get("id") is None:
            missing_category += 1

        else:
            category_counter[
                fixed_category["name"]
            ] += 1

        fixed_matches = fix_matches(
            item.get("matches")
        )

        working = dict(item)

        working[
            "suggested_category"
        ] = fixed_category

        working[
            "matches"
        ] = fixed_matches

        decision = classify(
            working
        )

        working[
            "final_decision"
        ] = decision

        working[
            "decision_reason"
        ] = build_reason(
            working,
            decision
        )

        results.append(
            working
        )

    # AUTO -> REVIEW -> HOLD 순
    decision_order = {
        "AUTO": 0,
        "REVIEW": 1,
        "HOLD": 2,
    }

    results.sort(
        key=lambda x: (
            decision_order.get(
                x[
                    "final_decision"
                ],
                9
            ),
            -float(
                x.get(
                    "confidence",
                    0
                )
            ),
            -float(
                x.get(
                    "max_similarity",
                    0
                )
            ),
        )
    )

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with OUTPUT_FILE.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            results,
            file,
            ensure_ascii=False,
            indent=2
        )

    auto_items = [
        x
        for x in results
        if x["final_decision"]
        == "AUTO"
    ]

    review_items = [
        x
        for x in results
        if x["final_decision"]
        == "REVIEW"
    ]

    hold_items = [
        x
        for x in results
        if x["final_decision"]
        == "HOLD"
    ]

    print()
    print("=" * 80)
    print("SEMANTIC RECOVERY FINAL DRY-RUN")
    print("=" * 80)

    print(
        f"TOTAL  : "
        f"{len(results):,}"
    )

    print(
        f"AUTO   : "
        f"{len(auto_items):,}"
    )

    print(
        f"REVIEW : "
        f"{len(review_items):,}"
    )

    print(
        f"HOLD   : "
        f"{len(hold_items):,}"
    )

    print(
        f"MISSING CATEGORY ID: "
        f"{missing_category:,}"
    )

    print()
    print("=" * 80)
    print("CATEGORY COUNTS")
    print("=" * 80)

    for name, count in (
        category_counter
        .most_common()
    ):
        category_id = (
            CATEGORY_ID_MAP.get(
                name
            )
        )

        print(
            f"{category_id:4} "
            f"{name:20} "
            f"{count:5,}"
        )

    print()
    print("=" * 80)
    print("AUTO SAMPLE")
    print("=" * 80)

    for index, item in enumerate(
        auto_items[:20],
        start=1
    ):
        category = item.get(
            "suggested_category"
        )

        print()
        print(
            f"[{index}] "
            f"sim="
            f"{item['max_similarity']:.4f} "
            f"avg="
            f"{item['avg_top_k_similarity']:.4f} "
            f"conf="
            f"{item['confidence']:.4f}"
        )

        print(
            f"title: "
            f"{item['title']}"
        )

        print(
            f"category: "
            f"{category}"
        )

        print(
            f"vote: "
            f"{item['category_vote_count']}/5"
        )

        print(
            f"reason: "
            f"{item['decision_reason']}"
        )

    print()
    print("=" * 80)
    print("REVIEW SAMPLE")
    print("=" * 80)

    for index, item in enumerate(
        review_items[:20],
        start=1
    ):
        category = item.get(
            "suggested_category"
        )

        print()
        print(
            f"[{index}] "
            f"sim="
            f"{item['max_similarity']:.4f} "
            f"avg="
            f"{item['avg_top_k_similarity']:.4f} "
            f"conf="
            f"{item['confidence']:.4f}"
        )

        print(
            f"title: "
            f"{item['title']}"
        )

        print(
            f"category: "
            f"{category}"
        )

        print(
            f"vote: "
            f"{item['category_vote_count']}/5"
        )

    print()
    print("=" * 80)
    print("OUTPUT")
    print("=" * 80)

    print(
        OUTPUT_FILE.resolve()
    )

    print()
    print(
        "DB MODIFIED: NO"
    )


if __name__ == "__main__":
    main()