"""Typed runtime configuration, loaded from the environment (and `.env` locally)."""

from __future__ import annotations

import datetime
import zoneinfo
from functools import lru_cache
from typing import Annotated

from pydantic import BeforeValidator, Field, field_validator, model_validator
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

    # Config-driven event-time window (issue #50, Mark's suggestion): the Pull
    # API's from/to params filter on EVENT TIME server-side — these map onto
    # them directly, like the reference script's from_date/to_date arguments.
    # When FROM is set, the flagless `daily` run pulls exactly [FROM, TO or
    # yesterday] instead of the lookback window; an explicit --date still wins
    # over both. CAUTION: a standing FROM that ages past the API's ~35-day
    # availability floor meets valid-header EMPTY responses, and the idempotent
    # delete-then-insert would wipe already-loaded rows (issue #45) — the run
    # warns (#28) but proceeds; the hard clamp is issue #49's scope.
    appsflyer_event_time_from: datetime.date | None = None
    appsflyer_event_time_to: datetime.date | None = None

    # Pull API `timezone` request param (issue #53): AppsFlyer returns report
    # times in UTC unless the request names the app's configured timezone —
    # then event times AND the from/to day boundaries follow that zone,
    # matching the analytics team's Europe/Riga reference exports. Unset (the
    # default) keeps today's UTC behavior. The value must match the app-level
    # timezone setting in AppsFlyer exactly; a malformed zone name fails
    # startup loudly, but a valid-but-wrong one cannot be caught client-side.
    appsflyer_timezone: RequiredStr | None = None

    @field_validator("appsflyer_app_ids", "appsflyer_event_names", mode="before")
    @classmethod
    def _parse_csv_fields(cls, value: object) -> object:
        return _split_csv(value)

    @field_validator("appsflyer_timezone")
    @classmethod
    def _validate_timezone_is_iana(cls, value: str | None) -> str | None:
        if value is not None:
            try:
                zoneinfo.ZoneInfo(value)
            except (zoneinfo.ZoneInfoNotFoundError, ValueError) as exc:
                raise ValueError(f"not a valid IANA time zone name: {value!r}") from exc
        return value

    @model_validator(mode="after")
    def _validate_event_time_window(self) -> Settings:
        if self.appsflyer_event_time_to is not None and self.appsflyer_event_time_from is None:
            raise ValueError("APPSFLYER_EVENT_TIME_TO requires APPSFLYER_EVENT_TIME_FROM")
        if (
            self.appsflyer_event_time_from is not None
            and self.appsflyer_event_time_to is not None
            and self.appsflyer_event_time_from > self.appsflyer_event_time_to
        ):
            raise ValueError("APPSFLYER_EVENT_TIME_FROM is after APPSFLYER_EVENT_TIME_TO")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]  # fields are populated from the environment
