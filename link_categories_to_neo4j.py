import json
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from neo4j import GraphDatabase
from psycopg.rows import dict_row


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

PG_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("POSTGRES_PORT", "55432"))
PG_DB = os.getenv("POSTGRES_DB", "law_db")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "1234")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")


def load_mapping():
    config = json.loads((BASE_DIR / "settings" / "filters.json").read_text(encoding="utf-8"))
    categories = config.get("categories", [])
    by_id = {str(item["id"]): str(item["name"]) for item in categories}
    by_name = {str(item["name"]): str(item["id"]) for item in categories}
    if len(by_id) != len(categories) or len(by_name) != len(categories):
        raise RuntimeError("filters.json category IDs or names are not unique")
    return by_id, by_name


def connect_postgres():
    return psycopg.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER,
        password=PG_PASSWORD, row_factory=dict_row,
    )


def category_value(item):
    if not isinstance(item, dict):
        return None, None, None
    category_id = item.get("id", item.get("category_id"))
    name = item.get("name", item.get("category"))
    role = str(item.get("role", "")).lower() or None
    return (str(category_id) if category_id is not None else None,
            str(name) if name is not None else None, role)


def parse_categories(value, by_id, by_name):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return [], "categories is not valid JSON"
    if not value:
        return [], "categories is null or empty"

    items = []
    if isinstance(value, dict):
        primary = value.get("primary_category")
        secondary = value.get("secondary_categories", [])
        if isinstance(primary, dict):
            items.append((primary, "primary"))
        if isinstance(secondary, list):
            items.extend((item, "secondary") for item in secondary)
        if not items:
            items.append((value, value.get("role")))
    elif isinstance(value, list):
        items = [(item, None) for item in value]
    else:
        return [], "categories has unsupported JSON type"

    result = []
    for index, (item, forced_role) in enumerate(items):
        category_id, name, role = category_value(item)
        role = forced_role or role or ("primary" if index == 0 else "secondary")
        if role not in {"primary", "secondary"}:
            role = "secondary" if index else "primary"
        resolved_id = category_id if category_id in by_id else by_name.get(name)
        resolved_name = by_id.get(resolved_id) if resolved_id else None
        if not resolved_id or not resolved_name:
            return [], f"category not found in filters.json: id={category_id!r}, name={name!r}"
        result.append({"category_id": resolved_id, "name": resolved_name, "role": role})

    unique = {(row["category_id"], row["role"]): row for row in result}
    primary = [row for row in unique.values() if row["role"] == "primary"][:1]
    secondary = [row for row in unique.values() if row["role"] == "secondary"][:2]
    return primary + secondary, None


def cypher_counts(driver):
    queries = {
        "NEWS NODES": "MATCH (n:News) RETURN count(n) AS n",
        "RELATED_TO": "MATCH ()-[r:RELATED_TO]->() RETURN count(r) AS n",
        "PART_OF": "MATCH ()-[r:PART_OF]->() RETURN count(r) AS n",
    }
    with driver.session() as session:
        return {label: session.run(query).single()["n"] for label, query in queries.items()}


def main():
    by_id, by_name = load_mapping()
    print(f"POSTGRES : {PG_HOST}:{PG_PORT}")
    print(f"NEWS DB  : {PG_DB}")
    print(f"NEO4J   : {NEO4J_URI}")
    print("CATEGORY MAPPING:", json.dumps(by_id, ensure_ascii=False, sort_keys=True))

    if not NEO4J_PASSWORD:
        raise RuntimeError("NEO4J_PASSWORD is missing")

    pg = connect_postgres()
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        driver.verify_connectivity()
        category_columns = pg.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='news_articles' "
            "AND (column_name ILIKE '%categor%' OR column_name='document_id') "
            "ORDER BY ordinal_position"
        ).fetchall()
        print("CATEGORY COLUMNS:", [row["column_name"] for row in category_columns])
        rows = pg.execute("SELECT document_id, categories FROM public.news_articles ORDER BY id").fetchall()
        parsed = []
        skipped = []
        for row in rows:
            categories, reason = parse_categories(row["categories"], by_id, by_name)
            if categories:
                parsed.append({"document_id": str(row["document_id"]), "categories": categories})
            else:
                skipped.append((str(row["document_id"]), reason))

        before = cypher_counts(driver)
        with driver.session() as session:
            session.run(
                "CREATE CONSTRAINT category_id_unique IF NOT EXISTS "
                "FOR (c:Category) REQUIRE c.category_id IS UNIQUE"
            ).consume()
            session.run(
                "UNWIND $rows AS row MERGE (c:Category {category_id: row.category_id}) "
                "SET c.name = row.name",
                rows=[{"category_id": cid, "name": name} for cid, name in by_id.items()],
            ).consume()
            linked = {"primary": 0, "secondary": 0}
            for start in range(0, len(parsed), 500):
                batch = parsed[start:start + 500]
                result = session.run(
                    "UNWIND $rows AS row MATCH (n:News {document_id: row.document_id}) "
                    "UNWIND row.categories AS cat MATCH (c:Category {category_id: cat.category_id}) "
                    "MERGE (n)-[r:BELONGS_TO {role: cat.role}]->(c) "
                    "SET r.name = cat.name RETURN count(*) AS matched",
                    rows=batch,
                ).single()
                if result:
                    for item in batch:
                        for cat in item["categories"]:
                            linked[cat["role"]] += 1
            print("LINKED FROM POSTGRES:", linked)

        after = cypher_counts(driver)
        with driver.session() as session:
            counts = session.run(
                "MATCH (n:News)-[r:BELONGS_TO]->(c:Category) "
                "RETURN r.role AS role, count(*) AS n ORDER BY role"
            ).data()
            missing = session.run(
                "MATCH (n:News) WHERE NOT (n)-[:BELONGS_TO]->(:Category) "
                "RETURN n.document_id AS document_id ORDER BY n.document_id"
            ).data()
            category_counts = session.run(
                "MATCH (n:News)-[:BELONGS_TO]->(c:Category) "
                "RETURN c.category_id AS id, c.name AS name, count(DISTINCT n) AS n "
                "ORDER BY n DESC, id"
            ).data()
            category_nodes = session.run("MATCH (c:Category) RETURN count(c) AS n").single()["n"]
            duplicate_nodes = session.run(
                "MATCH (c:Category) WITH c.category_id AS id, count(*) AS n "
                "WHERE n > 1 RETURN count(*) AS n"
            ).single()["n"]

        print("CATEGORY NODES:", category_nodes)
        print("PRIMARY RELATIONS:", sum(row["n"] for row in counts if row["role"] == "primary"))
        print("SECONDARY RELATIONS:", sum(row["n"] for row in counts if row["role"] == "secondary"))
        print("NEWS WITHOUT CATEGORY:", len(missing))
        print("CATEGORY NODE DUPLICATE GROUPS:", duplicate_nodes)
        print("NEWS NODES:", after["NEWS NODES"])
        print("RELATED_TO BEFORE:", before["RELATED_TO"])
        print("RELATED_TO AFTER:", after["RELATED_TO"])
        print("PART_OF BEFORE:", before["PART_OF"])
        print("PART_OF AFTER:", after["PART_OF"])
        print("CATEGORY COUNTS:", json.dumps(category_counts, ensure_ascii=False))
        if missing:
            print("NEWS WITHOUT CATEGORY DETAILS:", json.dumps(missing, ensure_ascii=False))
        if skipped:
            print("SKIPPED CATEGORY DETAILS:", json.dumps(
                [{"document_id": doc, "reason": reason} for doc, reason in skipped],
                ensure_ascii=False,
            ))
    finally:
        pg.close()
        driver.close()


if __name__ == "__main__":
    main()
