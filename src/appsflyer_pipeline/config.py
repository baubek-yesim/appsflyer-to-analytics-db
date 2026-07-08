"""Typed runtime configuration, loaded from the environment (and `.env` locally)."""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def _split_csv(value: object) -> object:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return value


# NoDecode: by default pydantic-settings JSON-decodes env values for list-typed
# fields before validators run, which rejects a plain CSV string outright.
# NoDecode skips that so `_parse_csv_fields` sees the raw "a,b,c" string.
CsvList = Annotated[list[str], NoDecode]


class Settings(BaseSettings):
    """Field names map case-insensitively to env vars, e.g. `db_host` <- `DB_HOST`."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Analytics database (MariaDB/MySQL)
    db_host: str
    db_port: int = 3306
    db_user: str
    db_password: str
    db_name: str
    db_table: str

    # AppsFlyer Pull API
    appsflyer_api_token: str
    # min_length=1 (issue #9): an empty value (e.g. a truncated line in the server's
    # hand-edited EnvironmentFile) must fail startup loudly — an empty app list is a
    # silent no-op run that exits 0, and an empty event list actively wipes windows
    # (transform re-filters with is_in([]) and the loader then delete-then-inserts nothing).
    appsflyer_app_ids: Annotated[CsvList, Field(min_length=1)]

    # Run parameters — defaulted to the BAF-2 acceptance criteria, overridable via env.
    appsflyer_media_source: str = "Facebook Ads"
    appsflyer_event_names: Annotated[CsvList, Field(min_length=1)] = [  # noqa: RUF012
        "af_purchase",
        "af_purchase_YC",
    ]

    @field_validator("appsflyer_app_ids", "appsflyer_event_names", mode="before")
    @classmethod
    def _parse_csv_fields(cls, value: object) -> object:
        return _split_csv(value)


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]  # fields are populated from the environment
