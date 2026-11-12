from pathlib import Path

from common.database import get_connection
from common.settings import ROOT


def create_tables():
    sql_path = ROOT / "database" / "create_tables.sql"
    sql = sql_path.read_text(encoding="utf-8")

    with get_connection() as connection:
        connection.execute(sql)

    print("PostgreSQL 테이블 생성 완료")


if __name__ == "__main__":
    create_tables()