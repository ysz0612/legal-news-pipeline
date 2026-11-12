import os
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase


BASE_DIR = Path(__file__).resolve().parent

load_dotenv(BASE_DIR / ".env")


NEO4J_URI = os.getenv(
    "NEO4J_URI",
    "bolt://localhost:7687"
)

NEO4J_USER = os.getenv(
    "NEO4J_USER",
    "neo4j"
)

NEO4J_PASSWORD = os.getenv(
    "NEO4J_PASSWORD"
)


CONSTRAINTS = [
    """
    CREATE CONSTRAINT news_document_id_unique
    IF NOT EXISTS
    FOR (n:News)
    REQUIRE n.document_id IS UNIQUE
    """,

    """
    CREATE CONSTRAINT legal_document_id_unique
    IF NOT EXISTS
    FOR (d:LegalDocument)
    REQUIRE d.document_id IS UNIQUE
    """,

    """
    CREATE CONSTRAINT legal_chunk_id_unique
    IF NOT EXISTS
    FOR (c:LegalChunk)
    REQUIRE c.chunk_id IS UNIQUE
    """,

    """
    CREATE INDEX news_primary_category_index
    IF NOT EXISTS
    FOR (n:News)
    ON (n.primary_category_id)
    """,

    """
    CREATE INDEX legal_chunk_document_index
    IF NOT EXISTS
    FOR (c:LegalChunk)
    ON (c.document_id)
    """
]


def main():
    print()
    print("=" * 70)
    print("NEO4J SETUP")
    print("=" * 70)

    if not NEO4J_PASSWORD:
        raise RuntimeError(
            "NEO4J_PASSWORD가 .env에 없습니다."
        )

    print(f"URI : {NEO4J_URI}")
    print(f"USER: {NEO4J_USER}")

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(
            NEO4J_USER,
            NEO4J_PASSWORD
        )
    )

    try:
        driver.verify_connectivity()

        print()
        print("[OK] Neo4j connected")
        print()

        with driver.session() as session:

            for index, query in enumerate(
                CONSTRAINTS,
                start=1
            ):
                session.run(
                    query
                ).consume()

                print(
                    f"[{index}/{len(CONSTRAINTS)}] OK"
                )

        print()
        print(
            "[DONE] Neo4j constraints/indexes ready"
        )

    finally:
        driver.close()


if __name__ == "__main__":
    main()