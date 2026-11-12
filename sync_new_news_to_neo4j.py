import json
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from neo4j import GraphDatabase
from psycopg.rows import dict_row

BASE = Path(__file__).resolve().parent
load_dotenv(BASE / ".env")
INPUT = BASE / "data" / "legal_semantic_categorized_080.json"
PG = dict(host=os.getenv("POSTGRES_HOST", "127.0.0.1"), port=int(os.getenv("POSTGRES_PORT", "55432")), user=os.getenv("POSTGRES_USER", "postgres"), password=os.getenv("POSTGRES_PASSWORD", "1234"))
URI = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD")
MODEL = "nvidia/nemotron-3-embed-1b"


def main():
    items = json.loads(INPUT.read_text(encoding="utf-8"))
    ids = [str(x["document_id"]) for x in items]
    config = json.loads((BASE / "settings" / "filters.json").read_text(encoding="utf-8"))
    categories = {str(x["id"]): str(x["name"]) for x in config["categories"]}
    if not PASSWORD:
        raise RuntimeError("NEO4J_PASSWORD is missing")
    news = psycopg.connect(dbname="law_db", row_factory=dict_row, **PG)
    legal = psycopg.connect(dbname="legal_shared", row_factory=dict_row, **PG)
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    try:
        driver.verify_connectivity()
        relation_rows = news.execute("SELECT news_document_id,legal_chunk_id,legal_document_id,similarity_score,rank FROM news_legal_relations WHERE news_document_id = ANY(%s) ORDER BY news_document_id,rank", (ids,)).fetchall()
        if len(relation_rows) < len(ids) * 5:
            raise RuntimeError(f"expected at least {len(ids) * 5} stored relations, got {len(relation_rows)}")
        chunk_ids = list({int(x["legal_chunk_id"]) for x in relation_rows})
        chunks = legal.execute("SELECT id,document_id,chunk_key,chunk_index,chunk_type,content FROM public.legal_chunks WHERE id=ANY(%s)", (chunk_ids,)).fetchall()
        doc_ids = list({int(x["document_id"]) for x in chunks})
        docs = legal.execute("SELECT id,title,document_type,case_number FROM public.legal_documents WHERE id=ANY(%s)", (doc_ids,)).fetchall()
        with driver.session() as s:
            s.run("CREATE CONSTRAINT category_id_unique IF NOT EXISTS FOR (c:Category) REQUIRE c.category_id IS UNIQUE").consume()
            s.run("UNWIND $rows AS row MERGE (c:Category {category_id:row.id}) SET c.name=row.name", rows=[{"id": k, "name": v} for k, v in categories.items()]).consume()
            payload = []
            for item in items:
                cls = item.get("classification") or {}
                cats = []
                primary = cls.get("primary_category") or {}
                if primary:
                    cats.append({"id": str(primary.get("id") or primary.get("category_id")), "role": "primary"})
                cats.extend({"id": str(x.get("id") or x.get("category_id")), "role": "secondary"} for x in (cls.get("secondary_categories") or []))
                payload.append({"id": str(item["document_id"]), "title": item.get("title") or "", "description": item.get("description") or "", "categories": cats})
            for start in range(0, len(payload), 500):
                batch = payload[start:start + 500]
                s.run("UNWIND $rows AS row MERGE (n:News {document_id:row.id}) ON CREATE SET n.title=row.title,n.description=row.description", rows=batch).consume()
                s.run("UNWIND $rows AS row MATCH (n:News {document_id:row.id}) UNWIND row.categories AS cat MATCH (c:Category {category_id:cat.id}) MERGE (n)-[r:BELONGS_TO {role:cat.role}]->(c)", rows=batch).consume()
            s.run("UNWIND $rows AS row MERGE (d:LegalDocument {document_id:row.id}) ON CREATE SET d.title=row.title,d.document_type=row.document_type,d.document_number=row.case_number", rows=[{"id": int(x["id"]), "title": x["title"] or "", "document_type": x["document_type"], "case_number": x["case_number"]} for x in docs]).consume()
            s.run("UNWIND $rows AS row MERGE (c:LegalChunk {chunk_id:row.id}) ON CREATE SET c.document_id=row.document_id,c.chunk_key=row.chunk_key,c.chunk_index=row.chunk_index,c.chunk_type=row.chunk_type,c.content=row.content", rows=[{"id": int(x["id"]), "document_id": int(x["document_id"]), "chunk_key": x["chunk_key"], "chunk_index": x["chunk_index"], "chunk_type": x["chunk_type"], "content": x["content"] or ""} for x in chunks]).consume()
            s.run("UNWIND $rows AS row MATCH (c:LegalChunk {chunk_id:row.chunk_id}) MATCH (d:LegalDocument {document_id:row.document_id}) MERGE (c)-[:PART_OF]->(d)", rows=[{"chunk_id": int(x["id"]), "document_id": int(x["document_id"])} for x in chunks]).consume()
            for start in range(0, len(relation_rows), 500):
                s.run("UNWIND $rows AS row MATCH (n:News {document_id:row.news_document_id}) MATCH (c:LegalChunk {chunk_id:row.legal_chunk_id}) MERGE (n)-[r:RELATED_TO]->(c) SET r.similarity=row.similarity_score,r.rank=row.rank,r.model=$model", rows=[dict(x) for x in relation_rows[start:start + 500]], model=MODEL).consume()
            print("NEW NEWS NODES:", s.run("MATCH (n:News) WHERE n.document_id IN $ids RETURN count(n) AS n", ids=ids).single()["n"])
            print("NEW RELATED_TO:", s.run("MATCH (n:News)-[:RELATED_TO]->(c:LegalChunk) WHERE n.document_id IN $ids RETURN count(*) AS n", ids=ids).single()["n"])
            print("NEW CATEGORY RELATIONS:", s.run("MATCH (n:News)-[:BELONGS_TO]->(c:Category) WHERE n.document_id IN $ids RETURN count(*) AS n", ids=ids).single()["n"])
        print("SYNC STATUS: SUCCESS")
    finally:
        news.close(); legal.close(); driver.close()


if __name__ == "__main__":
    main()
