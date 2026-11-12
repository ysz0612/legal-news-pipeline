import hashlib
import json
import os
import statistics
from pathlib import Path

import numpy as np
import psycopg
from dotenv import load_dotenv
from neo4j import GraphDatabase
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


BASE = Path(__file__).resolve().parent
load_dotenv(BASE / ".env")
INPUT = BASE / "data" / "legal_semantic_categorized_080.json"
MODEL = "nvidia/nemotron-3-embed-1b"

PG = dict(host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
          port=int(os.getenv("POSTGRES_PORT", "55432")),
          user=os.getenv("POSTGRES_USER", "postgres"),
          password=os.getenv("POSTGRES_PASSWORD", "1234"))
NEO_URI = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
NEO_USER = os.getenv("NEO4J_USER", "neo4j")
NEO_PASSWORD = os.getenv("NEO4J_PASSWORD")


def vec(value):
    if isinstance(value, str):
        return np.asarray([float(x) for x in value.strip("[]").split(",") if x], dtype=np.float32)
    return np.asarray(value, dtype=np.float32)


def mapping():
    data = json.loads((BASE / "settings" / "filters.json").read_text(encoding="utf-8"))
    items = data["categories"]
    by_id = {str(x["id"]): str(x["name"]) for x in items}
    if len(by_id) != len(items):
        raise RuntimeError("duplicate category IDs in filters.json")
    return by_id


def category_json(item, source="semantic_recovery"):
    cls = item.get("classification") or {}
    confidence = cls.get("confidence")
    result = []
    primary = cls.get("primary_category") or {}
    all_items = [(primary, "primary")] + [(x, "secondary") for x in (cls.get("secondary_categories") or [])]
    names = mapping()
    for category, role in all_items:
        cid = str(category.get("id") or category.get("category_id") or "")
        name = str(category.get("name") or category.get("category") or names.get(cid) or "")
        if cid not in names or not name:
            raise RuntimeError(f"unknown category for {item['document_id']}: {cid!r}/{name!r}")
        result.append({"id": cid, "name": name, "category_id": cid, "category": name,
                       "role": role, "source": source, "confidence": confidence})
    if not result:
        raise RuntimeError(f"missing primary category: {item['document_id']}")
    return result


def open_pg(db):
    return psycopg.connect(dbname=db, row_factory=dict_row, **PG)


def graph_counts(driver):
    qs = {
        "news": "MATCH (n:News) RETURN count(n) AS n",
        "related": "MATCH ()-[r:RELATED_TO]->() RETURN count(r) AS n",
        "part": "MATCH ()-[r:PART_OF]->() RETURN count(r) AS n",
        "category": "MATCH (c:Category) RETURN count(c) AS n",
        "belongs": "MATCH ()-[r:BELONGS_TO]->() RETURN count(r) AS n",
    }
    with driver.session() as s:
        return {k: s.run(q).single()["n"] for k, q in qs.items()}


def main():
    items = json.loads(INPUT.read_text(encoding="utf-8"))
    if not isinstance(items, list) or len(items) != 1343:
        raise RuntimeError(f"expected 1343 input rows, got {len(items) if isinstance(items, list) else 'non-list'}")
    print("INPUT JSON:", len(items))
    print("JSON FIELDS:", sorted(items[0]))
    print("CLASSIFICATION FIELDS:", sorted((items[0].get("classification") or {})))
    if not NEO_PASSWORD:
        raise RuntimeError("NEO4J_PASSWORD is missing")

    news_db = open_pg("law_db")
    legal_db = open_pg("legal_shared")
    driver = GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PASSWORD))
    try:
        driver.verify_connectivity()
        cols = news_db.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='news_articles' ORDER BY ordinal_position").fetchall()
        print("NEWS COLUMNS:", [x["column_name"] for x in cols])
        before = graph_counts(driver)

        ids = [str(x["document_id"]) for x in items]
        if len(set(ids)) != len(ids):
            raise RuntimeError("duplicate document_id in input")
        existing = {str(x["document_id"]) for x in news_db.execute("SELECT document_id FROM news_articles WHERE document_id = ANY(%s)", (ids,)).fetchall()}
        inserted = updated = already = failed = 0
        for item in items:
            try:
                doc_id = str(item["document_id"])
                categories = category_json(item)
                title = item.get("title") or ""
                description = item.get("description") or item.get("content") or ""
                if doc_id in existing:
                    news_db.execute("UPDATE news_articles SET categories=%s, category_candidates='[]'::jsonb, article_types='[]'::jsonb, filter_status='categorized', updated_at=CURRENT_TIMESTAMP WHERE document_id=%s", (Jsonb(categories), doc_id))
                    already += 1
                else:
                    event_id = hashlib.sha256(("semantic-recovery:" + doc_id).encode()).hexdigest()
                    news_db.execute("INSERT INTO news_articles (document_id,event_id,title,description,source,url,published_at,collected_at,legal_keywords,categories,category_candidates,article_types,filter_status,filter_version,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,'[]'::jsonb,%s,'[]'::jsonb,'[]'::jsonb,'categorized','semantic-recovery',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", (doc_id, event_id, title, description, item.get("source") or "semantic_recovery", item.get("url") or item.get("link") or "", item.get("published_at"), Jsonb(categories)))
                    inserted += 1
            except Exception:
                news_db.rollback()
                failed += 1
                raise
        news_db.commit()
        updated = already
        print("DB INSERTED:", inserted)
        print("DB UPDATED:", updated)
        print("ALREADY EXISTS:", already)
        print("FAILED:", failed)
        total = news_db.execute("SELECT count(*) AS n FROM news_articles").fetchone()["n"]
        print("DB TOTAL LEGAL NEWS:", total)

        emb_rows = news_db.execute("SELECT document_id,nemotron_embedding FROM news_articles WHERE document_id = ANY(%s)", (ids,)).fetchall()
        missing = [str(x["document_id"]) for x in emb_rows if x["nemotron_embedding"] is None]
        print("NEW NEWS TOTAL:", len(emb_rows))
        print("NEW NEWS WITH NEMOTRON:", len(emb_rows) - len(missing))
        print("NEW NEWS WITHOUT NEMOTRON:", len(missing))
        if missing:
            print("MISSING DOCUMENT IDS:", json.dumps(missing, ensure_ascii=False))
            raise RuntimeError("Nemotron embedding missing; stopped before relation creation")

        legal_rows = legal_db.execute("SELECT id,document_id,nemotron_embedding FROM public.legal_chunks WHERE nemotron_embedding_status='COMPLETED' AND nemotron_embedding IS NOT NULL ORDER BY id").fetchall()
        legal_matrix = np.asarray([vec(x["nemotron_embedding"]) for x in legal_rows], dtype=np.float32)
        if legal_matrix.shape[1] != 2048:
            raise RuntimeError(f"legal Nemotron dimension is {legal_matrix.shape[1]}")
        legal_matrix /= np.maximum(np.linalg.norm(legal_matrix, axis=1, keepdims=True), 1e-12)
        news_vectors = {str(x["document_id"]): vec(x["nemotron_embedding"]) for x in emb_rows}
        rel_rows = []
        scores = []
        for doc_id in ids:
            q = news_vectors[doc_id]
            q /= max(float(np.linalg.norm(q)), 1e-12)
            sim = q @ legal_matrix.T
            idx = np.argsort(-sim)[:5]
            scores.append(float(sim[idx[0]]))
            for rank, pos in enumerate(idx, 1):
                legal = legal_rows[int(pos)]
                rel_rows.append((doc_id, int(legal["id"]), int(legal["document_id"]), float(sim[pos]), rank))
        existing_rel = {
            (x["news_document_id"], x["legal_chunk_id"], x["rank"])
            for x in news_db.execute(
                "SELECT news_document_id,legal_chunk_id,rank FROM news_legal_relations WHERE news_document_id = ANY(%s)",
                (ids,),
            ).fetchall()
        }
        created = skipped = 0
        for row in rel_rows:
            key = (row[0], row[1], row[4])
            if key in existing_rel:
                skipped += 1
                continue
            news_db.execute("INSERT INTO news_legal_relations (news_document_id,legal_chunk_id,legal_document_id,similarity_score,rank,relation_type,embedding_model) VALUES (%s,%s,%s,%s,%s,'SEMANTIC_SIMILARITY',%s) ON CONFLICT (news_document_id,legal_chunk_id,embedding_model) DO UPDATE SET similarity_score=EXCLUDED.similarity_score,rank=EXCLUDED.rank,legal_document_id=EXCLUDED.legal_document_id,created_at=NOW()", (*row[:5], MODEL))
            created += 1
        news_db.commit()
        print("NEW NEWS LINK TARGET:", len(ids))
        print("RELATIONS CREATED:", created)
        print("RELATIONS SKIPPED:", skipped)
        print("FAILED:", 0)
        print("AVG TOP1:", statistics.mean(scores))
        print("P50 TOP1:", float(np.percentile(scores, 50)))
        print("P90 TOP1:", float(np.percentile(scores, 90)))
        print("P95 TOP1:", float(np.percentile(scores, 95)))

        with driver.session() as s:
            s.run("CREATE CONSTRAINT category_id_unique IF NOT EXISTS FOR (c:Category) REQUIRE c.category_id IS UNIQUE").consume()
            s.run("UNWIND $rows AS row MERGE (c:Category {category_id:row.id}) SET c.name=row.name", rows=[{"id": cid, "name": name} for cid, name in mapping().items()]).consume()
            news_payload = []
            for item in items:
                cats = category_json(item)
                news_payload.append({
                    "document_id": str(item["document_id"]),
                    "title": item.get("title") or "",
                    "description": item.get("description") or item.get("content") or "",
                    "categories": cats,
                })
            for start in range(0, len(news_payload), 500):
                batch = news_payload[start:start+500]
                s.run("UNWIND $rows AS row MERGE (n:News {document_id:row.document_id}) ON CREATE SET n.title=row.title,n.description=row.description", rows=batch).consume()
                s.run("UNWIND $rows AS row MATCH (n:News {document_id:row.document_id}) UNWIND row.categories AS cat MATCH (c:Category {category_id:cat.id}) MERGE (n)-[r:BELONGS_TO {role:cat.role}]->(c) SET r.name=cat.name", rows=batch).consume()
            graph_rels = news_db.execute("SELECT news_document_id,legal_chunk_id,legal_document_id,similarity_score,rank FROM news_legal_relations WHERE news_document_id = ANY(%s) ORDER BY news_document_id,rank", (ids,)).fetchall()
            for start in range(0, len(graph_rels), 500):
                s.run("UNWIND $rows AS row MATCH (n:News {document_id:row.news_document_id}) MATCH (c:LegalChunk {chunk_id:row.legal_chunk_id}) MERGE (n)-[r:RELATED_TO]->(c) SET r.similarity=row.similarity_score,r.rank=row.rank,r.model=$model", rows=[dict(x) for x in graph_rels[start:start+500]], model=MODEL).consume()
        after = graph_counts(driver)
        with driver.session() as s:
            cat_rel = s.run("MATCH (n:News)-[r:BELONGS_TO]->(c:Category) WHERE n.document_id IN $ids RETURN r.role AS role,count(*) AS n ORDER BY role", ids=ids).data()
            new_rel = s.run("MATCH (n:News)-[:RELATED_TO]->(c:LegalChunk) WHERE n.document_id IN $ids RETURN count(*) AS n", ids=ids).single()["n"]
            print("NEW NEWS NODES:", s.run("MATCH (n:News) WHERE n.document_id IN $ids RETURN count(n) AS n", ids=ids).single()["n"])
            print("NEW RELATED_TO:", new_rel)
            print("NEW CATEGORY RELATIONS:", sum(x["n"] for x in cat_rel))
            print("PRIMARY RELATIONS:", sum(x["n"] for x in cat_rel if x["role"] == "primary"))
            print("SECONDARY RELATIONS:", sum(x["n"] for x in cat_rel if x["role"] == "secondary"))
        print("NEWS NODES TOTAL:", after["news"])
        print("RELATED_TO TOTAL:", after["related"])
        print("PART_OF TOTAL:", after["part"])
        print("CATEGORY NODES TOTAL:", after["category"])
        print("BELONGS_TO TOTAL:", after["belongs"])
        if after["related"] < before["related"] or after["part"] < before["part"]:
            raise RuntimeError("existing graph relation count decreased")
        print("STATUS: SUCCESS")
        print("SEMANTIC NEWS -> DB -> LEGAL TOP5 -> NEO4J -> CATEGORY")
        print("COMPLETE")
    finally:
        news_db.close()
        legal_db.close()
        driver.close()


if __name__ == "__main__":
    main()
