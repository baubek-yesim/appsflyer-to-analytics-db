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
    comment 62585) — neither column can differ within a single call.

    Rows sharing the key but disagreeing on another field are **kept, both of
    them**, with a WARNING. They are two genuinely distinct events that the key
    cannot separate: AppsFlyer timestamps are second-granular, so one user
    making two purchases of different amounts inside the same second produces
    exactly this shape. Measured live on 2026-08-13 while filling
    2026-07-15..07-26 — one such pair (3 vs 4 EUR at 2026-07-19 04:26:43) out
    of 242 rows, and under the previous behavior (raise) it cost the entire
    window: `_process_window` isolates a TransformError to its own
    (app_id, attribution_type, chunk), but that window then loads nothing at
    all. Dropping 242 rows to avoid guessing about 2 is the wrong trade, and
    Mark's comment specifies the dedup key, not the failure mode — the raise
    was this repo's own guard (issue #23).

    Exact duplicates still collapse: identical bytes carry no information that
    picking one of them could lose.
    """
    seen: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = {}
    kept: list[dict[str, Any]] = []
    duplicate_count = 0
    conflict_count = 0
    first_conflict: tuple[Any, Any, Any] | None = None
    for row in rows:
        key = (row["event_time"], row["event_name"], row["appsflyer_id"])
        bucket = seen.setdefault(key, [])
        if any(existing == row for existing in bucket):
            duplicate_count += 1
            continue
        if bucket:
            conflict_count += 1
            if first_conflict is None:
                first_conflict = key
        bucket.append(row)
        kept.append(row)
    if duplicate_count:
        logger.warning(
            "collapsed %d exact-duplicate row(s): attribution_type=%s app_id=%s",
            duplicate_count,
            attribution_type,
            app_id,
        )
    if conflict_count:
        logger.warning(
            "kept %d conflicting row(s) sharing a dedup key (distinct events the key "
            "cannot separate; first: %r): attribution_type=%s app_id=%s",
            conflict_count,
            first_conflict,
            attribution_type,
            app_id,
        )
    return kept


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

    ALL rows are loaded regardless of `Is Primary Attribution` (issue #47,
    data-analytics decision on #46, reversing #7's filter): dedup follows
    Mark's key (event_time, event_name, appsflyer_id, attribution_type) via
    `_dedupe_rows`, and since `attribution_type` is part of that key, a
    dual-attributed purchase legitimately appears once per report —
    attribution_type is a dimension, and cross-attribution sums count such
    purchases in both dimensions by design.
    """
    missing = [raw for raw in _COLUMN_MAP if raw not in df.columns]
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
