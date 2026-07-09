"""Maps raw AppsFlyer rows to the target table schema (Stage 4).

Mirrors Mark Malovichko's TARGET_COLUMNS shape (BAF-2 comment 62293), but fails
loudly on schema drift instead of silently filling missing columns with None —
per docs/design-spec.md's risk mitigation for that scenario.
"""

from __future__ import annotations

import datetime
import logging
from decimal import Decimal, InvalidOperation
from typing import Any

import polars as pl

from appsflyer_pipeline.appsflyer_client import AttributionType

logger = logging.getLogger(__name__)

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# Raw AppsFlyer column -> target table column. Confirmed against a live API
# response (81 raw columns) during Stage 4 — everything else AppsFlyer returns
# (geo, device, contributors, cost, ...) is intentionally dropped.
_COLUMN_MAP: dict[str, str] = {
    "Event Time": "event_time",
    "Install Time": "install_time",
    "Attributed Touch Time": "attributed_touch_time",
    "Event Name": "event_name",
    "Event Revenue": "event_revenue",
    "Media Source": "media_source",
    "Channel": "channel",
    "Campaign": "campaign",
    "Campaign ID": "campaign_id",
    "Adset": "adset",
    "Adset ID": "adset_id",
    "Ad": "ad",
    "Ad ID": "ad_id",
    "AppsFlyer ID": "appsflyer_id",
    "Customer User ID": "customer_user_id",
}

_TIMESTAMP_COLUMNS = ("event_time", "install_time", "attributed_touch_time")
_REQUIRED_NOT_NULL = ("event_time", "event_name", "appsflyer_id")

# Dual attribution (issue #7): a standard column of the v5 export (present in
# the live 81-column response; must NOT be requested via additional_fields —
# see appsflyer_client._fetch_csv). Not loaded into the table; used to drop
# secondary copies of retargeting-attributed events from the UA pull.
_PRIMARY_ATTRIBUTION_COLUMN = "Is Primary Attribution"


class TransformError(RuntimeError):
    """Raised when raw AppsFlyer data doesn't match the expected shape."""


def _parse_timestamp(value: str | None) -> datetime.datetime | None:
    if value is None or value.strip() == "":
        return None
    try:
        return datetime.datetime.strptime(value, _TIMESTAMP_FORMAT)
    except ValueError as exc:
        raise TransformError(f"Unexpected timestamp format: {value!r}") from exc


def _parse_revenue(value: str | None) -> Decimal | None:
    if value is None or value.strip() == "":
        return None
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise TransformError(f"Unexpected event_revenue value: {value!r}") from exc


def _dedupe_rows(
    rows: list[dict[str, Any]], *, attribution_type: AttributionType, app_id: str
) -> list[dict[str, Any]]:
    """Collapse exact duplicate rows sharing (event_time, event_name, appsflyer_id).

    `attribution_type`/`app_id` are constant across one transform_events call, so
    this 3-column key is covariant with Mark's full 4-column dedup key (BAF-2
    comment 62585) — neither column can differ within a single call. Rows
    sharing the key but disagreeing on any other field raise: that means the
    key's uniqueness assumption doesn't hold for this data and needs a human,
    not a silent pick. A raised error fails only the current
    (app_id, attribution_type, chunk) window via _process_window's isolation —
    not the whole run — but that window's load is skipped entirely until the
    conflict is resolved, same as any other TransformError.
    """
    seen: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    duplicate_count = 0
    for row in rows:
        key = (row["event_time"], row["event_name"], row["appsflyer_id"])
        existing = seen.get(key)
        if existing is None:
            seen[key] = row
        elif existing == row:
            duplicate_count += 1
        else:
            raise TransformError(
                f"Conflicting duplicate rows for key {key!r} "
                f"(attribution_type={attribution_type}, app_id={app_id}): {existing} vs {row}"
            )
    if duplicate_count:
        logger.warning(
            "collapsed %d exact-duplicate row(s): attribution_type=%s app_id=%s",
            duplicate_count,
            attribution_type,
            app_id,
        )
    return list(seen.values())


def transform_events(
    df: pl.DataFrame,
    *,
    attribution_type: AttributionType,
    app_id: str,
    media_source_filter: str,
    event_names_filter: list[str],
) -> list[dict[str, Any]]:
    """Map one raw AppsFlyer chunk to typed rows matching the target schema.

    Re-applies the media-source/event-name filters client-side (defense in
    depth on top of the API's own `media_source`/`event_name` request params)
    and adds `attribution_type`/`app_id`, which AppsFlyer's export doesn't know.

    For `non_organic` chunks, also drops rows where `Is Primary Attribution`
    is false: those are secondary copies of retargeting-attributed events that
    the retargeting pull already delivers as primary (issue #7 — loading both
    double-counts revenue).
    """
    required_raw = list(_COLUMN_MAP)
    if attribution_type == "non_organic":
        required_raw.append(_PRIMARY_ATTRIBUTION_COLUMN)
    missing = [raw for raw in required_raw if raw not in df.columns]
    if missing:
        raise TransformError(
            f"AppsFlyer response is missing expected column(s): {missing} "
            f"(attribution_type={attribution_type}, app_id={app_id})"
        )

    # Issue #26: this early-return must stay BELOW the column check. Only a
    # schema-valid empty (expected headers, zero rows -- the shape a genuinely
    # quiet window returns, live-verified 2026-07-09) may yield []; an
    # error-text body or a drifted header set parses to a 0-row frame too,
    # and returning [] for those would wipe the window downstream at exit 0.
    if df.is_empty():
        return []

    if attribution_type == "non_organic":
        # Drop secondary copies of retargeting-attributed events (dual
        # attribution, issue #7) — they arrive as primary rows in the
        # retargeting report, so keeping them here double-counts revenue.
        flag = pl.col(_PRIMARY_ATTRIBUTION_COLUMN).str.strip_chars().str.to_lowercase()
        invalid = df.filter(flag.is_null() | ~flag.is_in(["true", "false"]))
        if invalid.height:
            bad_values = invalid.get_column(_PRIMARY_ATTRIBUTION_COLUMN).unique().head(5).to_list()
            raise TransformError(
                f"Unexpected {_PRIMARY_ATTRIBUTION_COLUMN!r} value(s): {bad_values} "
                f"(attribution_type={attribution_type}, app_id={app_id})"
            )
        df = df.filter(flag.eq("true"))

    filtered = df.filter(
        pl.col("Media Source").eq(media_source_filter)
        & pl.col("Event Name").is_in(event_names_filter)
    )

    rows: list[dict[str, Any]] = []
    for raw_row in filtered.select(list(_COLUMN_MAP)).iter_rows(named=True):
        row: dict[str, Any] = {target: raw_row[raw] for raw, target in _COLUMN_MAP.items()}
        for ts_col in _TIMESTAMP_COLUMNS:
            row[ts_col] = _parse_timestamp(row[ts_col])
        row["event_revenue"] = _parse_revenue(row["event_revenue"])
        row["attribution_type"] = attribution_type
        row["app_id"] = app_id

        for required in _REQUIRED_NOT_NULL:
            if not row[required]:
                raise TransformError(f"Row has NULL/blank required field {required!r}: {row}")

        rows.append(row)

    return _dedupe_rows(rows, attribution_type=attribution_type, app_id=app_id)
