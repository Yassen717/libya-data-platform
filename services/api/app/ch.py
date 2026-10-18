from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from app.config import get_settings


@lru_cache
def get_client() -> Client:
    s = get_settings()
    url = urlparse(s.clickhouse_url)
    secure = url.scheme == "https"
    return clickhouse_connect.get_client(
        host=url.hostname or "localhost",
        port=url.port or (8443 if secure else 8123),
        username=s.clickhouse_user,
        password=s.clickhouse_password,
        database=s.clickhouse_db,
        interface="https" if secure else "http",
    )


def query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    result = get_client().query(sql, parameters=params or {})
    return [dict(zip(result.column_names, row, strict=True)) for row in result.result_rows]
