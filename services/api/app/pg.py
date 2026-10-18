from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import get_settings


def query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    s = get_settings()
    with (
        psycopg.connect(
            host=s.postgres_host,
            port=s.postgres_port,
            dbname=s.postgres_db,
            user=s.postgres_user,
            password=s.postgres_password,
            row_factory=dict_row,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(sql, params)
        return cur.fetchall()
