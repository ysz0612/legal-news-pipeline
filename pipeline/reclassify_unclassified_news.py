"""Safe historical legal-news reclassification.

Default mode is read-only dry-run.  ``--write-staging`` is the only option
that creates/updates the review table; it never updates news_articles.
``--live-openai`` is opt-in and is limited to clear candidates.
"""
import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from psycopg.types.json import Jsonb

from common.database import get_connection
from common.rules import categorize, legal_filter
from common.settings import filters

SEMANTIC_THRESHOLD = 0.80


def _article(row):
    return {
        "document_id": row["document_id"],
        "title": row["title"] or "",
        "content": row["description"] or "",
        "source": row["source"] or "",
        "url": row["url"] or "",
        "published_at": row["published_at"].isoformat() if row["published_at"] else None,
        "collected_at": row["collected_at"].isoformat() if row["collected_at"] else None,
        "content_type": "stored_description",
        "legal_keywords": row["legal_keywords"] or [],
        "categories": row["categories"] or [],
        "category_candidates": row["category_candidates"] or [],
        "article_types": row["article_types"] or [],
        "filter_status": row["filter_status"],
        "filter_version": row["filter_version"],
    }


def _semantic_scores(db, rows):
    """Return max reference similarity by document_id without changing DB."""
    usable = [r for r in rows if r["nemotron_embedding"] is not None]
    if not usable:
        return {}
    try:
        import numpy as np
        from common.nemotron_embedding import parse_vector
        from semantic_recovery_fast import load_references, normalize_matrix, calculate_top_k
    except (ImportError, OSError) as exc:
        print(f"SEMANTIC_SCORING_UNAVAILABLE: {exc}")
        return {}
    refs, refmat = load_references(db)
    refmat = normalize_matrix(refmat.astype(np.float32))
    vectors = normalize_matrix(np.vstack([parse_vector(r["nemotron_embedding"]) for r in usable]).astype(np.float32))
    _, scores = calculate_top_k(vectors, refmat, 1)
    return {r["document_id"]: float(score[0]) for r, score in zip(usable, scores)}


def _initial_decision(row, semantic_score):
    config = filters()
    article = legal_filter(_article(row), config)
    if article["legal_keywords"]:
        return "LEGAL", "existing legal_filter signal", article
    article = categorize(article, config)
    if article["categories"]:
        return "LEGAL", "existing category signal", article
    source = (row["source"] or "").strip()
    if source in {"법률신문", "법조신문"}:
        return "LEGAL_CANDIDATE", "legal-specialist publisher; classifier review required", article
    if article["category_candidates"]:
        return "LEGAL_CANDIDATE", "weak category signal", article
    if semantic_score is not None and semantic_score >= SEMANTIC_THRESHOLD:
        return "LEGAL_CANDIDATE", f"semantic similarity {semantic_score:.4f}", article
    # No automatic deletion/rejection: uncertain historical records remain for review.
    return "REVIEW", "no decisive keyword/category/semantic signal", article


def _ensure_staging(db):
    with db.transaction():
        db.execute("""
            CREATE TABLE IF NOT EXISTS news_legal_reclassification (
                id BIGSERIAL PRIMARY KEY,
                document_id TEXT NOT NULL UNIQUE,
                old_filter_status TEXT,
                old_categories JSONB NOT NULL DEFAULT '[]'::jsonb,
                old_category_candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
                semantic_score DOUBLE PRECISION,
                classifier_result JSONB NOT NULL DEFAULT '{}'::jsonb,
                final_status TEXT NOT NULL,
                reason TEXT,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)


def run(limit, write_staging, live_openai):
    db = get_connection()
    try:
        rows = db.execute("""
            SELECT document_id,title,description,source,url,published_at,collected_at,
                   legal_keywords,categories,category_candidates,article_types,
                   filter_status,filter_version,nemotron_embedding
            FROM news_articles
            WHERE legal_keywords='[]'::jsonb
              AND categories='[]'::jsonb
              AND category_candidates='[]'::jsonb
            ORDER BY id
            LIMIT %s
        """, (limit,)).fetchall()
        scores = _semantic_scores(db, rows)
        results = []
        for row in rows:
            status, reason, article = _initial_decision(row, scores.get(row["document_id"]))
            result = {"is_legal": status in {"LEGAL", "LEGAL_CANDIDATE"},
                      "status": status, "reason": reason,
                      "categories": article.get("categories", []),
                      "category_candidates": article.get("category_candidates", [])}
            # OpenAI is intentionally opt-in and only sees clear candidates.
            if live_openai and status in {"LEGAL_CANDIDATE", "REVIEW"}:
                from common.openai_classifier import classify_article
                result["openai"] = classify_article({"title": row["title"] or "", "content": row["description"] or ""})
                result["status"] = "LEGAL" if result["openai"].get("is_legal") else "REVIEW"
            results.append((row, scores.get(row["document_id"]), result))
        if write_staging:
            _ensure_staging(db)
            with db.transaction():
                for row, score, result in results:
                    db.execute("""
                        INSERT INTO news_legal_reclassification
                        (document_id,old_filter_status,old_categories,old_category_candidates,
                         semantic_score,classifier_result,final_status,reason)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (document_id) DO UPDATE SET
                          semantic_score=EXCLUDED.semantic_score,
                          classifier_result=EXCLUDED.classifier_result,
                          final_status=EXCLUDED.final_status,
                          reason=EXCLUDED.reason,
                          processed_at=CURRENT_TIMESTAMP
                    """, (row["document_id"], row["filter_status"], Jsonb(row["categories"] or []),
                          Jsonb(row["category_candidates"] or []), score, Jsonb(result),
                          result["status"], result["reason"]))
        counts = {key: sum(1 for _, _, result in results if result["status"] == key)
                  for key in ("LEGAL", "LEGAL_CANDIDATE", "NON_LEGAL", "REVIEW")}
        print(json.dumps({"TOTAL_TESTED": len(results), **counts, "FAILED": 0,
                          "WRITE_STAGING": write_staging, "LIVE_OPENAI": live_openai}, ensure_ascii=False))
        for status in ("LEGAL", "LEGAL_CANDIDATE", "NON_LEGAL", "REVIEW"):
            print(f"[{status}]")
            for row, _, result in [x for x in results if x[2]["status"] == status][:10]:
                print(row["title"] or "(untitled)")
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--write-staging", action="store_true")
    parser.add_argument("--live-openai", action="store_true")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be >= 1")
    run(args.limit, args.write_staging, args.live_openai)


if __name__ == "__main__":
    main()
