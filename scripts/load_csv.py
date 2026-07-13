"""One-off / occasional loader: a CSV already in `appsflyer_events_fb` shape -> the DB.

For CSVs that already carry the target table's 17 snake_case columns (see
`references/appsflayer_add_raw.csv` for the expected shape) rather than a raw
AppsFlyer Pull API export. `transform.transform_events` expects the raw,
title-cased AppsFlyer column names ("Event Time", "AppsFlyer ID", ...) and
stamps `attribution_type`/`app_id` on itself, so it cannot consume this shape
-- this script builds typed row dicts directly instead.

Safety model -- `appsflyer_events_fb` is production-critical, so this script
is deliberately conservative, unlike `loader.load_events`'s
delete-then-insert idempotency model (appropriate for a scheduled fetch that
owns its whole window; inappropriate for a CSV of unknown/partial provenance):

  * INSERT-only. Never DELETEs, under any flag.
  * A row is skipped -- not inserted -- if a row with the same natural key
    (event_time, event_name, appsflyer_id) already exists in the table for
    the same (app_id, attribution_type), or already appeared earlier in this
    same CSV. Avoids duplicating rows the scheduled daily/backfill job (or a
    previous run of this script) already loaded for an overlapping window.
  * `campaign_id`/`adset_id`/`ad_id` values mangled into scientific notation
    by Excel (e.g. "1.20E+17") are nulled rather than written verbatim --
    that is not the real 18-digit AppsFlyer ID, and the true value is not
    recoverable from a CSV already saved in that state. Every other column
    on the row (revenue, timestamps, appsflyer_id, campaign/adset/ad
    *names*) still loads normally.
  * Defaults to a dry run: parses, validates, and reports what it would do,
    but writes nothing. Pass --execute to actually write, inside a single
    transaction (rolled back whole on any error).

Usage:
    uv run python scripts/load_csv.py <csv_path>              # dry run
    uv run python scripts/load_csv.py <csv_path> --execute    # write
    uv run python scripts/load_csv.py <csv_path> --execute --table some_other_table
"""

from __future__ import annotations

import argparse
import datetime
import logging
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from appsflyer_pipeline.cli import (
    _format_validation_error,  # same secret-safe rendering (issue #27)
)
from appsflyer_pipeline.config import get_settings
from appsflyer_pipeline.loader import (
    _INSERT_COLUMNS,  # reusing the loader's single source of truth for column order
    PipelineError,
    _validate_identifier,  # same identifier-safety gate loader.py itself uses
    check_connection,
    create_engine,
)

logger = logging.getLogger(__name__)

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"  # mirrors transform._TIMESTAMP_FORMAT
_TIMESTAMP_COLUMNS = ("event_time", "install_time", "attributed_touch_time")
_ID_COLUMNS_TO_SANITIZE = ("campaign_id", "adset_id", "ad_id")
_REQUIRED_NOT_NULL = ("event_time", "event_name", "appsflyer_id", "attribution_type", "app_id")

# Excel silently rewrites long all-digit strings (AppsFlyer's 18-digit
# campaign/adset/ad IDs) into scientific notation when a CSV is opened and
# re-saved, e.g. "120244023026200094" -> "1.2E+17". The true value is gone by
# the time we see it -- this pattern is only good for detecting the damage.
_SCI_NOTATION_RE = re.compile(r"^\d+(\.\d+)?E\+\d+$", re.IGNORECASE)


class LoadError(RuntimeError):
    """Raised for actionable failures in this script (bad CSV, DB error, ...)."""


def _parse_timestamp(value: str | None) -> datetime.datetime | None:
    if value is None or value.strip() == "":
        return None
    try:
        return datetime.datetime.strptime(value, _TIMESTAMP_FORMAT)
    except ValueError as exc:
        raise LoadError(f"Unexpected timestamp format: {value!r}") from exc


def _parse_revenue(value: str | None) -> Decimal | None:
    if value is None or value.strip() == "":
        return None
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise LoadError(f"Unexpected event_revenue value: {value!r}") from exc


def _clean_str(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _sanitize_id(value: str | None) -> tuple[str | None, bool]:
    """Return (cleaned value, True if it was nulled for looking Excel-mangled)."""
    cleaned = _clean_str(value)
    if cleaned is not None and _SCI_NOTATION_RE.match(cleaned):
        return None, True
    return cleaned, False


@dataclass
class ParseStats:
    total_rows: int = 0
    ids_nulled: int = 0


def _read_csv(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise LoadError(f"CSV file not found: {path}")
    # infer_schema=False: read every column as a plain string. We do our own
    # typed parsing below so a stray non-numeric revenue or malformed
    # timestamp raises a clear LoadError instead of a silent polars-inferred
    # null/mismatch.
    df = pl.read_csv(path, infer_schema=False)
    missing = sorted(set(_INSERT_COLUMNS) - set(df.columns))
    extra = sorted(set(df.columns) - set(_INSERT_COLUMNS))
    if missing or extra:
        raise LoadError(
            f"CSV columns don't match the target schema. missing={missing} extra={extra}"
        )
    return df


def _build_row(raw: dict[str, Any], stats: ParseStats) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for ts_col in _TIMESTAMP_COLUMNS:
        row[ts_col] = _parse_timestamp(raw[ts_col])
    row["event_name"] = _clean_str(raw["event_name"])
    row["event_revenue"] = _parse_revenue(raw["event_revenue"])
    for str_col in ("media_source", "channel", "campaign", "adset", "ad", "customer_user_id"):
        row[str_col] = _clean_str(raw[str_col])
    for id_col in _ID_COLUMNS_TO_SANITIZE:
        value, was_nulled = _sanitize_id(raw[id_col])
        row[id_col] = value
        if was_nulled:
            stats.ids_nulled += 1
    row["appsflyer_id"] = _clean_str(raw["appsflyer_id"])
    row["attribution_type"] = _clean_str(raw["attribution_type"])
    row["app_id"] = _clean_str(raw["app_id"])

    for required in _REQUIRED_NOT_NULL:
        if not row[required]:
            raise LoadError(f"Row has NULL/blank required field {required!r}: {raw}")

    return row


def parse_csv(path: Path) -> tuple[list[dict[str, Any]], ParseStats]:
    df = _read_csv(path)
    stats = ParseStats()
    rows: list[dict[str, Any]] = []
    for raw_row in df.iter_rows(named=True):
        stats.total_rows += 1
        rows.append(_build_row(raw_row, stats))
    return rows, stats


def _group_rows(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (row["app_id"], row["attribution_type"])
        groups.setdefault(key, []).append(row)
    return groups


@dataclass
class GroupPlan:
    app_id: str
    attribution_type: str
    total: int
    already_in_table: int
    intra_csv_dup: int
    to_insert: list[dict[str, Any]] = field(default_factory=list)


def _existing_keys(
    engine: Engine,
    table_name: str,
    *,
    app_id: str,
    attribution_type: str,
    window_start: datetime.datetime,
    window_end: datetime.datetime,
) -> set[tuple[Any, Any, Any]]:
    """Read-only lookup of natural keys already present for this group's exact event_time span."""
    stmt = text(
        f"SELECT event_time, event_name, appsflyer_id FROM `{table_name}` "
        "WHERE app_id = :app_id AND attribution_type = :attribution_type "
        "AND event_time BETWEEN :window_start AND :window_end"
    )
    with engine.connect() as conn:
        result = conn.execute(
            stmt,
            {
                "app_id": app_id,
                "attribution_type": attribution_type,
                "window_start": window_start,
                "window_end": window_end,
            },
        )
        return {(row.event_time, row.event_name, row.appsflyer_id) for row in result}


def plan_group(
    engine: Engine, table_name: str, app_id: str, attribution_type: str, rows: list[dict[str, Any]]
) -> GroupPlan:
    event_times: list[datetime.datetime] = [row["event_time"] for row in rows]
    existing = _existing_keys(
        engine,
        table_name,
        app_id=app_id,
        attribution_type=attribution_type,
        window_start=min(event_times),
        window_end=max(event_times),
    )

    seen_this_run: set[tuple[Any, Any, Any]] = set()
    to_insert: list[dict[str, Any]] = []
    already_in_table = 0
    intra_csv_dup = 0
    for row in rows:
        key = (row["event_time"], row["event_name"], row["appsflyer_id"])
        if key in existing:
            already_in_table += 1
        elif key in seen_this_run:
            intra_csv_dup += 1
        else:
            seen_this_run.add(key)
            to_insert.append(row)

    return GroupPlan(
        app_id=app_id,
        attribution_type=attribution_type,
        total=len(rows),
        already_in_table=already_in_table,
        intra_csv_dup=intra_csv_dup,
        to_insert=to_insert,
    )


def build_plan(
    engine: Engine, table_name: str, groups: dict[tuple[str, str], list[dict[str, Any]]]
) -> list[GroupPlan]:
    return [
        plan_group(engine, table_name, app_id, attribution_type, rows)
        for (app_id, attribution_type), rows in sorted(groups.items())
    ]


def execute_inserts(engine: Engine, table_name: str, plans: list[GroupPlan]) -> int:
    rows_to_insert = [row for plan in plans for row in plan.to_insert]
    if not rows_to_insert:
        return 0
    columns_sql = ", ".join(f"`{c}`" for c in _INSERT_COLUMNS)
    placeholders_sql = ", ".join(f":{c}" for c in _INSERT_COLUMNS)
    insert_stmt = text(f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({placeholders_sql})")
    try:
        with engine.begin() as conn:
            conn.execute(insert_stmt, rows_to_insert)
    except SQLAlchemyError as exc:
        raise LoadError(f"Insert failed, transaction rolled back: {exc}") from exc
    return len(rows_to_insert)


def _print_report(
    csv_path: Path, table_name: str, stats: ParseStats, plans: list[GroupPlan]
) -> None:
    print(f"\nCSV: {csv_path}")
    print(f"Target table: `{table_name}`")
    print(f"Parsed rows: {stats.total_rows}")
    print(f"IDs nulled (Excel scientific-notation corruption): {stats.ids_nulled}")
    print("\nPer-group plan (app_id, attribution_type):")
    total_to_insert = 0
    for plan in plans:
        print(
            f"  ({plan.app_id}, {plan.attribution_type}): "
            f"total={plan.total} already_in_table={plan.already_in_table} "
            f"intra_csv_dup={plan.intra_csv_dup} to_insert={len(plan.to_insert)}"
        )
        total_to_insert += len(plan.to_insert)
    print(f"\nTotal rows to insert: {total_to_insert}")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "csv_path", type=Path, help="Path to the CSV, already in target-table column shape."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually write to the database. Omit for a dry run (default): parses, "
        "validates, and reports what would happen, but writes nothing.",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Override the target table name (default: the configured DB_TABLE).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = _parse_args(argv)

    try:
        settings = get_settings()
    except ValidationError as exc:
        # Sanitized (issue #27): raw ValidationError.__str__ embeds the collected
        # settings dict, which would leak db_password/api_token onto stderr.
        print(f"ERROR: {_format_validation_error(exc)}", file=sys.stderr)
        return 1
    table_name = _validate_identifier(args.table or settings.db_table)
    engine = create_engine(settings)

    try:
        before = check_connection(engine, table_name)
    except PipelineError as exc:
        print(f"ERROR: could not connect: {exc}", file=sys.stderr)
        return 1
    if not before.table_exists:
        print(f"ERROR: table `{table_name}` does not exist.", file=sys.stderr)
        return 1
    print(
        f"Connected. server_version={before.server_version} "
        f"table=`{table_name}` current_row_count={before.row_count}"
    )

    try:
        rows, stats = parse_csv(args.csv_path)
        if not rows:
            print("CSV has no data rows -- nothing to do.")
            return 0
        groups = _group_rows(rows)
        plans = build_plan(engine, table_name, groups)
    except LoadError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    _print_report(args.csv_path, table_name, stats, plans)

    if not args.execute:
        print("\nDry run only -- no changes written. Re-run with --execute to write.")
        return 0

    try:
        inserted = execute_inserts(engine, table_name, plans)
    except LoadError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    after = check_connection(engine, table_name)
    print(f"\nInserted {inserted} row(s). row_count: {before.row_count} -> {after.row_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
