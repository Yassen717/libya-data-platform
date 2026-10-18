from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    clickhouse_url: str = "http://localhost:8123"
    clickhouse_db: str = "libya"
    clickhouse_user: str = "ly"
    clickhouse_password: str = "devpass"

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "libya"
    postgres_user: str = "ly"
    postgres_password: str = "devpass"


@lru_cache
def get_settings() -> Settings:
    return Settings()
