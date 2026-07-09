"""Typed runtime configuration, loaded from the environment (and `.env` locally)."""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from pydantic import BeforeValidator, Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def _split_csv(value: object) -> object:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return value


def _strip_scalar(value: object) -> object:
    if isinstance(value, str):
        return value.strip()
    return value


# min_length=1 on required scalars (issue #29, same rationale as #9's list
# fields): a truncated line in the server's hand-edited EnvironmentFile must
# fail startup loudly, not degrade at runtime. Stripping runs BEFORE the
# length check so whitespace-only values are rejected too -- and so edge
# whitespace can't silently break the exact-match media-source filter.
# db_password is deliberately exempt: an empty DB password is legitimate
# (CI's mysql:8 service container uses one).
RequiredStr = Annotated[str, BeforeValidator(_strip_scalar), Field(min_length=1)]

# NoDecode: by default pydantic-settings JSON-decodes env values for list-typed
# fields before validators run, which rejects a plain CSV string outright.
# NoDecode skips that so `_parse_csv_fields` sees the raw "a,b,c" string.
CsvList = Annotated[list[str], NoDecode]


class Settings(BaseSettings):
    """Field names map case-insensitively to env vars, e.g. `db_host` <- `DB_HOST`."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Analytics database (MariaDB/MySQL)
    db_host: RequiredStr
    db_port: int = 3306
    db_user: RequiredStr
    db_password: str
    db_name: RequiredStr
    db_table: RequiredStr

    # AppsFlyer Pull API
    appsflyer_api_token: RequiredStr
    # Canonical production roster, defaulted (issue #48) so environments without
    # APPSFLYER_APP_IDS just work — mirrors the reference scripts' hardcoded list
    # (which also carries id6753973280 / id1525236866 commented out as future
    # candidates). Setting the env var overrides the default. min_length=1 stays
    # (issue #9): an EXPLICITLY empty value — e.g. a truncated line in the
    # server's hand-edited EnvironmentFile — must still fail startup loudly
    # instead of degrading to a silent no-op run that exits 0 (an empty event
    # list would even actively wipe windows via is_in([])).
    appsflyer_app_ids: Annotated[CsvList, Field(min_length=1)] = [
        "com.yesimmobile",
        "id1458505230",
    ]

    # Run parameters — defaulted to the BAF-2 acceptance criteria, overridable via env.
    appsflyer_media_source: RequiredStr = "Facebook Ads"
    appsflyer_event_names: Annotated[CsvList, Field(min_length=1)] = [
        "af_purchase",
        "af_purchase_YC",
    ]

    # Daily trailing-window depth (issue #8): the scheduled `daily` run pulls
    # [yesterday - (N-1), yesterday]. Default 1 preserves the original single-day
    # pull; deeper windows re-capture AppsFlyer late/offline-cached events (the
    # 05:00 +03 timer fires exactly at AppsFlyer's 02:00 UTC late-event boundary)
    # and cost no extra API quota at N <= 31 (one report download per
    # app/attribution regardless of range length). Upper bound = the Pull API's
    # ~90-day retention (appsflyer_client.MAX_RETENTION_DAYS; literal here to
    # keep config free of package imports).
    appsflyer_daily_lookback_days: Annotated[int, Field(ge=1, le=90)] = 1

    @field_validator("appsflyer_app_ids", "appsflyer_event_names", mode="before")
    @classmethod
    def _parse_csv_fields(cls, value: object) -> object:
        return _split_csv(value)


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]  # fields are populated from the environment
