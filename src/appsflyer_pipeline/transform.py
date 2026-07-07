"""Maps raw AppsFlyer rows to the target table schema (Stage 4).

Mirrors Mark Malovichko's TARGET_COLUMNS shape (BAF-2 comment 62293), but fails
loudly on schema drift instead of silently filling missing columns with None —
per docs/design-spec.md's risk mitigation for that scenario.
"""

from __future__ import annotations

import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import polars as pl

from appsflyer_pipeline.appsflyer_client import AttributionType

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
    """
    if df.is_empty():
        return []

    missing = [raw for raw in _COLUMN_MAP if raw not in df.columns]
    if missing:
        raise TransformError(
            f"AppsFlyer response is missing expected column(s): {missing} "
            f"(attribution_type={attribution_type}, app_id={app_id})"
        )

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

    return rows
