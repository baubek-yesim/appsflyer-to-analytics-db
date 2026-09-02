"""Maps raw AppsFlyer rows to the target table schema (Stage 4).

Mirrors Mark Malovichko's TARGET_COLUMNS shape (BAF-2 comment 62293), but fails
loudly on schema drift instead of silently filling missing columns with None —
per docs/design-spec.md's risk mitigation for that scenario.
"""

from __future__ import annotations

import datetime
import logging
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

import polars as pl

from appsflyer_pipeline.appsflyer_client import AttributionType

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from appsflyer_pipeline.reports import ReportSpec

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# Raw-only column read for dedup, never persisted (BAF-11 stage 2, Q6): on the
# unfiltered full-array stream, two real events can share
# (event_time, event_name, appsflyer_id) to the second and still be genuinely
# different rows -- e.g. two screen_plans_cInternet events from one device in
# one second with a different Event Value (measured live 2026-08-13: 5 such
# rows in one conflict group). Folding this into the dedupe key stops those
# from resolving to a single survivor. It is deliberately NOT added to
# _COLUMN_MAP: ticket requirement #2 keeps in-app-events on the same 17-column
# table, so this value is stripped back off every row before transform_events
# returns (see the `del` in transform_events below).
_DEDUPE_DISCRIMINATOR_RAW_COLUMN = "Event Value"
# BAF-11 stage 4: exported (no leading underscore) -- reports.py's in-app-events
# dedupe_key function needs the exact same internal row key transform_events()
# writes it under, so the two stay in sync by construction instead of by two
# separately-maintained string literals.
DEDUPE_DISCRIMINATOR_ROW_KEY = "__dedupe_event_value"

# BAF-11 stage 4 (installs' full pass-through mode, ReportSpec.column_map=None):
# raw AppsFlyer header -> target snake_case column name, with no per-report
# hand-written dict. Reproduces every existing _IN_APP_EVENTS_COLUMN_MAP entry
# exactly (pinned by test_transform.py's
# test_normalize_matches_every_in_app_events_column_map_entry) -- the one
# override below exists ONLY to dodge a real name collision (see the comment
# on it), not to change behavior for anything already mapped by hand.
_RAW_COLUMN_NAME_OVERRIDES: dict[str, str] = {
    "App ID": "appsflyer_app_id",
}


def normalize_column_name(raw: str) -> str:
    """Raw AppsFlyer CSV header -> target snake_case column name."""
    if raw in _RAW_COLUMN_NAME_OVERRIDES:
        return _RAW_COLUMN_NAME_OVERRIDES[raw]
    return raw.strip().lower().replace(" ", "_")


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


def _install_time_rank(row: dict[str, Any]) -> tuple[int, datetime.datetime]:
    """Sort key making a NULL `install_time` lose to any real one.

    Mirrors MariaDB's `ORDER BY install_time DESC`, which sorts NULLs last. The
    leading flag keeps None out of the datetime comparison rather than mapping it
    onto `datetime.min`, which a (pathological) real timestamp could collide with.
    """
    install_time = row["install_time"]
    if install_time is None:
        return (0, datetime.datetime.min)
    return (1, install_time)


def _dedupe_rows(
    rows: list[dict[str, Any]], *, attribution_type: AttributionType, app_id: str
) -> list[dict[str, Any]]:
    """Keep exactly ONE row per (event_time, event_name, appsflyer_id,
    Event Value) key: the one with the latest `install_time`.

    `attribution_type`/`app_id` are constant across one transform_events call, so
    this 3-column key is covariant with Mark's full 4-column dedup key (BAF-2
    comment 62585) — neither column can differ within a single call. Note the
    consequence: this function can only ever compare rows *within* one
    (app_id, attribution_type) report, so it does NOT collapse the
    dual-attribution twins (issues #7/#46/#47), which arrive in two separate
    reports and two separate load windows. Only a post-load SQL pass spanning the
    whole table can do that — see
    sql/migrations/2026-08-14-dedupe-keep-latest-install-time.sql.

    Conflict handling changed on 2026-08-14 (data-analytics decision): rows
    sharing the key but disagreeing on another field are no longer both kept —
    the latest `install_time` wins and the loser is DROPPED, matching the SQL
    rewrite above so a scheduled run cannot reintroduce what that rewrite
    removed. Both prior behaviours are recorded in docs/design-spec.md: raising
    (until 2026-08-13) cost the entire window, keeping both (2026-08-13..08-14)
    kept every row.

    Event Value joined the key on BAF-11 stage 2 (2026-08-25): on the
    unfiltered full-array stream, rows sharing the original 3-column key can
    be genuinely distinct events rather than conflicting duplicates -- see
    test_transform_keeps_both_rows_when_event_value_differs. The known
    production conflict this function was built for (two purchases of
    different amounts in the same second, distinguished by `Event Revenue`,
    NOT `Event Value`) is unaffected by this change and still resolves via
    the install_time tiebreak described below.

    The known cost, logged loudly: the one conflict actually measured in
    production (2026-08-13, 3 vs 4 EUR at 2026-07-19 04:26:43 in a 242-row
    window) is two distinct purchases by one user in the same second. They share
    an appsflyer_id, hence an install_time, so the tiebreak is report order, not
    data — one real purchase is discarded. The WARNING reports how many such ties
    occurred and how much `event_revenue` went with them.

    Exact duplicates still collapse separately: identical bytes carry no
    information that picking one of them could lose.
    """
    slot_of_key: dict[tuple[Any, Any, Any, Any], int] = {}
    kept: list[dict[str, Any]] = []
    duplicate_count = 0
    conflict_count = 0
    tie_count = 0
    discarded_revenue = Decimal(0)
    first_conflict: tuple[Any, Any, Any, Any] | None = None

    for row in rows:
        key = (
            row["event_time"],
            row["event_name"],
            row["appsflyer_id"],
            row[DEDUPE_DISCRIMINATOR_ROW_KEY],
        )
        slot = slot_of_key.get(key)
        if slot is None:
            slot_of_key[key] = len(kept)
            kept.append(row)
            continue

        incumbent = kept[slot]
        if row == incumbent:
            duplicate_count += 1
            continue

        conflict_count += 1
        if first_conflict is None:
            first_conflict = key
        challenger_rank = _install_time_rank(row)
        incumbent_rank = _install_time_rank(incumbent)
        if challenger_rank == incumbent_rank:
            # No data to choose on. Last row in report order wins, matching the
            # SQL rewrite's `id DESC` tiebreak (ids follow insertion = report
            # order), which at least makes repeated runs agree with each other.
            tie_count += 1
        if challenger_rank >= incumbent_rank:
            kept[slot] = row
            discarded = incumbent
        else:
            discarded = row
        discarded_revenue += discarded["event_revenue"] or Decimal(0)

    if duplicate_count:
        logger.warning(
            "collapsed %d exact-duplicate row(s): attribution_type=%s app_id=%s",
            duplicate_count,
            attribution_type,
            app_id,
        )
    if conflict_count:
        logger.warning(
            "dropped %d conflicting row(s) sharing a dedup key (kept the latest "
            "install_time; first: %r; discarded event_revenue: %s): "
            "attribution_type=%s app_id=%s",
            conflict_count,
            first_conflict,
            discarded_revenue,
            attribution_type,
            app_id,
        )
    if tie_count:
        logger.warning(
            "%d of those conflict(s) had identical install_time — the surviving row "
            "was picked by report order, not by data: attribution_type=%s app_id=%s",
            tie_count,
            attribution_type,
            app_id,
        )
    return kept


def transform_events(
    df: pl.DataFrame,
    *,
    spec: ReportSpec,
    app_id: str,
    media_source_filter: str | None,
    event_names_filter: list[str] | None,
) -> list[dict[str, Any]]:
    """Map one raw AppsFlyer chunk to typed rows matching the target schema.

    Re-applies the media-source/event-name filters client-side (defense in
    depth on top of the API's own `media_source`/`event_name` request params)
    and adds `attribution_type`/`app_id`, which AppsFlyer's export doesn't know.

    Either filter may be None (BAF-11 stage 1), meaning it was not requested
    from the API either — so there is nothing to re-apply and every row is kept.

    ALL rows are loaded regardless of `Is Primary Attribution` (issue #47,
    data-analytics decision on #46, reversing #7's filter): dedup follows
    Mark's key (event_time, event_name, appsflyer_id, attribution_type) via
    `_dedupe_rows`, and since `attribution_type` is part of that key, a
    dual-attributed purchase legitimately appears once per report —
    attribution_type is a dimension, and cross-attribution sums count such
    purchases in both dimensions by design. Within one report, a key collision
    now resolves to a single row (latest `install_time`) rather than keeping
    both — see `_dedupe_rows`.
    """
    attribution_type = spec.attribution_type
    column_map = spec.column_map
    missing = [
        raw for raw in (*column_map, _DEDUPE_DISCRIMINATOR_RAW_COLUMN) if raw not in df.columns
    ]
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

    # BAF-11 stage 1: an unset filter drops its predicate entirely rather than
    # comparing against None. This is not cosmetic — organic rows carry an EMPTY
    # Media Source, so any predicate built from a missing filter would discard
    # exactly the rows the full raw export exists to capture.
    predicates: list[pl.Expr] = []
    if media_source_filter is not None:
        predicates.append(pl.col("Media Source").eq(media_source_filter))
    if event_names_filter is not None:
        predicates.append(pl.col("Event Name").is_in(event_names_filter))

    filtered = df
    for predicate in predicates:
        filtered = filtered.filter(predicate)

    rows: list[dict[str, Any]] = []
    skipped_missing_required = 0
    select_columns = [*column_map, _DEDUPE_DISCRIMINATOR_RAW_COLUMN]
    for raw_row in filtered.select(select_columns).iter_rows(named=True):
        row: dict[str, Any] = {target: raw_row[raw] for raw, target in column_map.items()}
        row[DEDUPE_DISCRIMINATOR_ROW_KEY] = raw_row[_DEDUPE_DISCRIMINATOR_RAW_COLUMN]
        for ts_col in spec.timestamp_columns:
            row[ts_col] = _parse_timestamp(row[ts_col])
        row["event_revenue"] = _parse_revenue(row["event_revenue"])
        row["attribution_type"] = attribution_type
        row["app_id"] = app_id

        if any(not row[required] for required in spec.required_not_null):
            skipped_missing_required += 1
            continue

        rows.append(row)

    if skipped_missing_required:
        logger.warning(
            "skipped %d row(s) missing a required field (%s): attribution_type=%s app_id=%s",
            skipped_missing_required,
            ", ".join(spec.required_not_null),
            attribution_type,
            app_id,
        )

    deduped = _dedupe_rows(rows, attribution_type=attribution_type, app_id=app_id)
    for row in deduped:
        del row[DEDUPE_DISCRIMINATOR_ROW_KEY]
    return deduped
