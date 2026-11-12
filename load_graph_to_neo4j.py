import json
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from neo4j import GraphDatabase


# ============================================================
# BASE
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


# ============================================================
# POSTGRES
# ============================================================

PG_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("POSTGRES_PORT", "55432"))
PG_USER = os.getenv("POSTGRES_USER", "postgres")

PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "1234")

NEWS_DB = os.getenv(
    "POSTGRES_DB",
    "law_db"
)

LEGAL_DB = os.getenv(
    "LEGAL_DB_NAME",
    "legal_shared"
)


# ============================================================
# NEO4J
# ============================================================

NEO4J_URI = os.getenv(
    "NEO4J_URI",
    "bolt://127.0.0.1:7687"
)

NEO4J_USER = os.getenv(
    "NEO4J_USER",
    "neo4j"
)

NEO4J_PASSWORD = os.getenv(
    "NEO4J_PASSWORD"
)


# ============================================================
# SETTINGS
# ============================================================

BATCH_SIZE = 500


# ============================================================
# DB CONNECT
# ============================================================

def connect_postgres(dbname):
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=dbname,
    )


# ============================================================
# COLUMN HELPERS
# ============================================================

def get_columns(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )

        return {
            row[0]
            for row in cur.fetchall()
        }


def find_column(
    columns,
    candidates,
    required=True,
):
    for candidate in candidates:
        if candidate in columns:
            return candidate

    if required:
        raise RuntimeError(
            "컬럼을 찾을 수 없습니다.\n"
            f"찾은 컬럼: {sorted(columns)}\n"
            f"후보: {candidates}"
        )

    return None


# ============================================================
# JSON SAFE
# ============================================================

def safe_json(value):
    if value is None:
        return None

    if isinstance(
        value,
        (dict, list)
    ):
        return value

    try:
        return json.loads(value)
    except Exception:
        return None


def extract_primary_category(categories):
    data = safe_json(categories)

    if not data:
        return None, None

    # 배열 형태
    if isinstance(data, list):

        for item in data:
            if not isinstance(item, dict):
                continue

            role = str(
                item.get("role", "")
            ).lower()

            if role == "primary":
                return (
                    item.get("id"),
                    item.get("name"),
                )

        if data and isinstance(
            data[0],
            dict
        ):
            return (
                data[0].get("id"),
                data[0].get("name"),
            )

    # 객체 형태
    if isinstance(data, dict):

        primary = data.get(
            "primary_category"
        )

        if isinstance(
            primary,
            dict
        ):
            return (
                primary.get("id"),
                primary.get("name"),
            )

    return None, None


# ============================================================
# RELATION SCHEMA
# ============================================================

def resolve_relation_schema(conn):
    columns = get_columns(
        conn,
        "news_legal_relations"
    )

    print()
    print(
        "[RELATION TABLE COLUMNS]"
    )

    print(
        ", ".join(
            sorted(columns)
        )
    )

    news_col = find_column(
        columns,
        [
            "news_document_id",
            "news_id",
            "document_id",
        ],
    )

    chunk_col = find_column(
        columns,
        [
            "legal_chunk_id",
            "chunk_id",
        ],
    )

    legal_document_col = find_column(
        columns,
        [
            "legal_document_id",
        ],
        required=False,
    )

    score_col = find_column(
        columns,
        [
            "similarity_score",
            "similarity",
            "score",
        ],
    )

    rank_col = find_column(
        columns,
        [
            "rank",
            "similarity_rank",
        ],
    )

    return {
        "news":
            news_col,

        "chunk":
            chunk_col,

        "legal_document":
            legal_document_col,

        "score":
            score_col,

        "rank":
            rank_col,
    }


# ============================================================
# NEWS LOAD
# ============================================================

def load_news(
    conn,
    relation_schema,
):
    news_columns = get_columns(
        conn,
        "news_articles"
    )

    id_col = find_column(
        news_columns,
        [
            "document_id"
        ]
    )

    title_col = find_column(
        news_columns,
        [
            "title"
        ],
        required=False,
    )

    description_col = find_column(
        news_columns,
        [
            "description",
            "summary",
        ],
        required=False,
    )

    categories_col = find_column(
        news_columns,
        [
            "categories"
        ],
        required=False,
    )

    published_col = find_column(
        news_columns,
        [
            "published_at",
            "published",
            "pub_date",
        ],
        required=False,
    )

    source_col = find_column(
        news_columns,
        [
            "source",
            "source_name",
        ],
        required=False,
    )

    fields = [
        f"n.{id_col}"
    ]

    fields.append(
        (
            f"n.{title_col}"
            if title_col
            else "NULL"
        )
    )

    fields.append(
        (
            f"n.{description_col}"
            if description_col
            else "NULL"
        )
    )

    fields.append(
        (
            f"n.{categories_col}"
            if categories_col
            else "NULL"
        )
    )

    fields.append(
        (
            f"n.{published_col}"
            if published_col
            else "NULL"
        )
    )

    fields.append(
        (
            f"n.{source_col}"
            if source_col
            else "NULL"
        )
    )

    query = f"""
        SELECT DISTINCT
            {", ".join(fields)}
        FROM news_articles n
        INNER JOIN news_legal_relations r
            ON r.{relation_schema["news"]}
             = n.{id_col}
        ORDER BY n.{id_col}
    """

    results = []

    with conn.cursor() as cur:
        cur.execute(query)

        for row in cur:
            (
                document_id,
                title,
                description,
                categories,
                published_at,
                source,
            ) = row

            category_id, category_name = (
                extract_primary_category(
                    categories
                )
            )

            results.append({
                "document_id":
                    str(document_id),

                "title":
                    title or "",

                "description":
                    description or "",

                "primary_category_id":
                    category_id,

                "primary_category_name":
                    category_name,

                "published_at":
                    (
                        published_at.isoformat()
                        if hasattr(
                            published_at,
                            "isoformat"
                        )
                        else (
                            str(published_at)
                            if published_at
                            else None
                        )
                    ),

                "source":
                    str(source)
                    if source is not None
                    else None,
            })

    return results


# ============================================================
# RELATIONS LOAD
# ============================================================

def load_relations(
    conn,
    relation_schema,
):
    legal_doc_expr = (
        relation_schema[
            "legal_document"
        ]
    )

    if legal_doc_expr:
        legal_doc_select = (
            f"r.{legal_doc_expr}"
        )
    else:
        legal_doc_select = "NULL"

    query = f"""
        SELECT
            r.{relation_schema["news"]},
            r.{relation_schema["chunk"]},
            {legal_doc_select},
            r.{relation_schema["score"]},
            r.{relation_schema["rank"]}
        FROM news_legal_relations r
        ORDER BY
            r.{relation_schema["news"]},
            r.{relation_schema["rank"]}
    """

    results = []

    with conn.cursor() as cur:
        cur.execute(query)

        for row in cur:
            (
                news_id,
                chunk_id,
                legal_document_id,
                similarity,
                rank,
            ) = row

            results.append({
                "news_document_id":
                    str(news_id),

                "legal_chunk_id":
                    int(chunk_id),

                "legal_document_id":
                    (
                        int(
                            legal_document_id
                        )
                        if legal_document_id
                           is not None
                        else None
                    ),

                "similarity":
                    float(similarity),

                "rank":
                    int(rank),
            })

    return results


# ============================================================
# LEGAL CHUNKS
# ============================================================

def load_legal_chunks(
    conn,
    chunk_ids,
):
    if not chunk_ids:
        return []

    results = []

    chunk_ids = list(
        set(chunk_ids)
    )

    for start in range(
        0,
        len(chunk_ids),
        5000
    ):
        batch = chunk_ids[
            start:start + 5000
        ]

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    document_id,
                    chunk_key,
                    chunk_index,
                    chunk_type,
                    content
                FROM legal_chunks
                WHERE id = ANY(%s)
                """,
                (batch,),
            )

            for row in cur:
                results.append({
                    "chunk_id":
                        int(row[0]),

                    "document_id":
                        int(row[1]),

                    "chunk_key":
                        row[2],

                    "chunk_index":
                        row[3],

                    "chunk_type":
                        row[4],

                    "content":
                        row[5] or "",
                })

    return results


# ============================================================
# LEGAL DOCUMENTS
# ============================================================

def load_legal_documents(
    conn,
    document_ids,
):
    if not document_ids:
        return []

    columns = get_columns(
        conn,
        "legal_documents"
    )

    print()
    print(
        "[LEGAL DOCUMENT COLUMNS]"
    )

    print(
        ", ".join(
            sorted(columns)
        )
    )

    id_col = find_column(
        columns,
        ["id"]
    )

    title_col = find_column(
        columns,
        [
            "title",
            "document_title",
            "case_name",
            "name",
        ],
        required=False,
    )

    type_col = find_column(
        columns,
        [
            "document_type",
            "doc_type",
            "type",
            "source_type",
        ],
        required=False,
    )

    number_col = find_column(
        columns,
        [
            "case_number",
            "document_number",
            "law_number",
        ],
        required=False,
    )

    fields = [
        id_col
    ]

    fields.append(
        title_col
        if title_col
        else "NULL"
    )

    fields.append(
        type_col
        if type_col
        else "NULL"
    )

    fields.append(
        number_col
        if number_col
        else "NULL"
    )

    results = []

    document_ids = list(
        set(document_ids)
    )

    for start in range(
        0,
        len(document_ids),
        5000
    ):
        batch = document_ids[
            start:start + 5000
        ]

        query = f"""
            SELECT
                {", ".join(fields)}
            FROM legal_documents
            WHERE {id_col} = ANY(%s)
        """

        with conn.cursor() as cur:
            cur.execute(
                query,
                (batch,),
            )

            for row in cur:
                results.append({
                    "document_id":
                        int(row[0]),

                    "title":
                        row[1] or "",

                    "document_type":
                        (
                            str(row[2])
                            if row[2] is not None
                            else None
                        ),

                    "document_number":
                        (
                            str(row[3])
                            if row[3] is not None
                            else None
                        ),
                })

    return results


# ============================================================
# BATCH
# ============================================================

def batches(
    items,
    size,
):
    for i in range(
        0,
        len(items),
        size
    ):
        yield items[
            i:i + size
        ]


# ============================================================
# NEO4J NEWS
# ============================================================

def write_news(
    session,
    items,
):
    query = """
    UNWIND $rows AS row

    MERGE (n:News {
        document_id: row.document_id
    })

    SET
        n.title =
            row.title,

        n.description =
            row.description,

        n.primary_category_id =
            row.primary_category_id,

        n.primary_category_name =
            row.primary_category_name,

        n.published_at =
            row.published_at,

        n.source =
            row.source
    """

    session.run(
        query,
        rows=items
    ).consume()


# ============================================================
# NEO4J LEGAL DOCUMENT
# ============================================================

def write_legal_documents(
    session,
    items,
):
    query = """
    UNWIND $rows AS row

    MERGE (d:LegalDocument {
        document_id: row.document_id
    })

    SET
        d.title =
            row.title,

        d.document_type =
            row.document_type,

        d.document_number =
            row.document_number
    """

    session.run(
        query,
        rows=items
    ).consume()


# ============================================================
# NEO4J LEGAL CHUNK
# ============================================================

def write_legal_chunks(
    session,
    items,
):
    query = """
    UNWIND $rows AS row

    MERGE (c:LegalChunk {
        chunk_id: row.chunk_id
    })

    SET
        c.document_id =
            row.document_id,

        c.chunk_key =
            row.chunk_key,

        c.chunk_index =
            row.chunk_index,

        c.chunk_type =
            row.chunk_type,

        c.content =
            row.content
    """

    session.run(
        query,
        rows=items
    ).consume()


# ============================================================
# PART_OF
# ============================================================

def write_part_of(
    session,
    chunks,
):
    query = """
    UNWIND $rows AS row

    MATCH (c:LegalChunk {
        chunk_id: row.chunk_id
    })

    MATCH (d:LegalDocument {
        document_id: row.document_id
    })

    MERGE (c)-[:PART_OF]->(d)
    """

    session.run(
        query,
        rows=chunks
    ).consume()


# ============================================================
# RELATED_TO
# ============================================================

def write_related_to(
    session,
    relations,
):
    query = """
    UNWIND $rows AS row

    MATCH (n:News {
        document_id:
            row.news_document_id
    })

    MATCH (c:LegalChunk {
        chunk_id:
            row.legal_chunk_id
    })

    MERGE (n)-[r:RELATED_TO]->(c)

    SET
        r.similarity =
            row.similarity,

        r.rank =
            row.rank,

        r.model =
            "nvidia/nemotron-3-embed-1b"
    """

    session.run(
        query,
        rows=relations
    ).consume()


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 80)
    print(
        "POSTGRESQL -> NEO4J GRAPH LOADER"
    )
    print("=" * 80)

    print(
        f"POSTGRES : {PG_HOST}:{PG_PORT}"
    )

    print(
        f"NEWS DB : {NEWS_DB}"
    )

    print(
        f"LEGAL DB: {LEGAL_DB}"
    )

    print(
        f"NEO4J   : {NEO4J_URI}"
    )

    if not NEO4J_PASSWORD:
        raise RuntimeError(
            "NEO4J_PASSWORD가 .env에 없습니다."
        )

    # --------------------------------------------------------
    # PostgreSQL
    # --------------------------------------------------------

    news_conn = connect_postgres(
        NEWS_DB
    )

    legal_conn = connect_postgres(
        LEGAL_DB
    )

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
        print(
            "[OK] PostgreSQL + Neo4j connected"
        )

        # ----------------------------------------------------
        # Relations
        # ----------------------------------------------------

        relation_schema = (
            resolve_relation_schema(
                news_conn
            )
        )

        relations = load_relations(
            news_conn,
            relation_schema
        )

        print()
        print(
            f"[LOAD] relations: "
            f"{len(relations):,}"
        )

        # ----------------------------------------------------
        # News
        # ----------------------------------------------------

        news = load_news(
            news_conn,
            relation_schema
        )

        print(
            f"[LOAD] news: "
            f"{len(news):,}"
        )

        # ----------------------------------------------------
        # Chunk IDs
        # ----------------------------------------------------

        chunk_ids = [
            row[
                "legal_chunk_id"
            ]
            for row in relations
        ]

        legal_chunks = (
            load_legal_chunks(
                legal_conn,
                chunk_ids
            )
        )

        print(
            f"[LOAD] legal chunks: "
            f"{len(legal_chunks):,}"
        )

        # ----------------------------------------------------
        # Documents
        # ----------------------------------------------------

        document_ids = [
            row[
                "document_id"
            ]
            for row in legal_chunks
        ]

        legal_documents = (
            load_legal_documents(
                legal_conn,
                document_ids
            )
        )

        print(
            f"[LOAD] legal documents: "
            f"{len(legal_documents):,}"
        )

        # ----------------------------------------------------
        # Write Neo4j
        # ----------------------------------------------------

        with driver.session() as session:

            print()
            print(
                "[WRITE] News"
            )

            completed = 0

            for batch in batches(
                news,
                BATCH_SIZE
            ):
                write_news(
                    session,
                    batch
                )

                completed += len(
                    batch
                )

                print(
                    f"  {completed:,}/"
                    f"{len(news):,}"
                )

            # ------------------------------------------------

            print()
            print(
                "[WRITE] LegalDocument"
            )

            completed = 0

            for batch in batches(
                legal_documents,
                BATCH_SIZE
            ):
                write_legal_documents(
                    session,
                    batch
                )

                completed += len(
                    batch
                )

                print(
                    f"  {completed:,}/"
                    f"{len(legal_documents):,}"
                )

            # ------------------------------------------------

            print()
            print(
                "[WRITE] LegalChunk"
            )

            completed = 0

            for batch in batches(
                legal_chunks,
                BATCH_SIZE
            ):
                write_legal_chunks(
                    session,
                    batch
                )

                completed += len(
                    batch
                )

                print(
                    f"  {completed:,}/"
                    f"{len(legal_chunks):,}"
                )

            # ------------------------------------------------

            print()
            print(
                "[WRITE] PART_OF"
            )

            completed = 0

            for batch in batches(
                legal_chunks,
                BATCH_SIZE
            ):
                write_part_of(
                    session,
                    batch
                )

                completed += len(
                    batch
                )

                print(
                    f"  {completed:,}/"
                    f"{len(legal_chunks):,}"
                )

            # ------------------------------------------------

            print()
            print(
                "[WRITE] RELATED_TO"
            )

            completed = 0

            for batch in batches(
                relations,
                BATCH_SIZE
            ):
                write_related_to(
                    session,
                    batch
                )

                completed += len(
                    batch
                )

                print(
                    f"  {completed:,}/"
                    f"{len(relations):,}"
                )

        print()
        print("=" * 80)
        print("DONE")
        print("=" * 80)

        print(
            f"News          : "
            f"{len(news):,}"
        )

        print(
            f"LegalDocument : "
            f"{len(legal_documents):,}"
        )

        print(
            f"LegalChunk    : "
            f"{len(legal_chunks):,}"
        )

        print(
            f"RELATED_TO    : "
            f"{len(relations):,}"
        )

        print()
        print(
            "Neo4j graph load completed."
        )

    finally:

        news_conn.close()
        legal_conn.close()
        driver.close()


if __name__ == "__main__":
    main()
