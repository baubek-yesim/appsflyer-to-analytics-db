"""Orchestrates fetch -> transform -> load across every (app_id, attribution_type,
date-window) unit for a backfill or daily run (Stage 5).

`run_backfill`/`run_daily` never raise on a single window's failure — each unit
is isolated via `_process_window`, which returns a `WindowResult` instead of
propagating `AppsFlyerAPIError`/`TransformError`/`PipelineError`. That keeps a
bad window (rate limit exhausted, one malformed row, a transient DB blip) from
aborting the rest of a 12-window backfill. A summary is returned; the CLI
layer decides the process exit code from it.

Deliberately sequential (see docs/design-spec.md): AppsFlyer already rate-
limits, and `appsflyer_client` already retries 429/5xx with backoff -- running
these concurrently would only manufacture more 429s. At <=12 units per
backfill / 4 per daily, wall time is dominated by AppsFlyer's own export
generation, not client concurrency. `_process_window` returning a
self-contained result makes a future `ThreadPoolExecutor.map` a drop-in if
that ever changes -- no need to build it now.
"""

from __future__ import annotations

import datetime
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import httpx
from sqlalchemy.engine import Engine

from appsflyer_pipeline.appsflyer_client import (
    MAX_RETENTION_DAYS,
    AppsFlyerAPIError,
    AttributionType,
    chunk_date_range,
    fetch_events,
)
from appsflyer_pipeline.config import Settings, get_settings
from appsflyer_pipeline.loader import PipelineError, check_connection, create_engine, load_events
from appsflyer_pipeline.transform import TransformError, transform_events

logger = logging.getLogger(__name__)

ATTRIBUTION_TYPES: tuple[AttributionType, ...] = ("non_organic", "retargeting")


def _today() -> datetime.date:
    """Seam for tests: monkeypatch this rather than datetime.date.today directly."""
    return datetime.date.today()


@dataclass(frozen=True)
class WindowResult:
    app_id: str
    attribution_type: AttributionType
    start_date: datetime.date
    end_date: datetime.date
    fetched_rows: int
    loaded_rows: int
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


@dataclass(frozen=True)
class RunSummary:
    results: list[WindowResult]
    dry_run: bool

    @property
    def succeeded(self) -> list[WindowResult]:
        return [r for r in self.results if r.succeeded]

    @property
    def failed(self) -> list[WindowResult]:
        return [r for r in self.results if not r.succeeded]

    @property
    def total_fetched(self) -> int:
        return sum(r.fetched_rows for r in self.results)

    @property
    def total_loaded(self) -> int:
        return sum(r.loaded_rows for r in self.results)

    @property
    def all_succeeded(self) -> bool:
        return all(r.succeeded for r in self.results)


def _iter_work_items(
    settings: Settings, start: datetime.date, end: datetime.date
) -> Iterator[tuple[str, AttributionType, datetime.date, datetime.date]]:
    """(app_id x attribution_type x <=31-day chunk) for the [start, end] window.

    Pure -- no HTTP/DB -- so the exact work-item set and chunk boundaries are
    unit-testable without mocking anything.
    """
    for app_id in settings.appsflyer_app_ids:
        for attribution_type in ATTRIBUTION_TYPES:
            for chunk_start, chunk_end in chunk_date_range(start, end):
                yield app_id, attribution_type, chunk_start, chunk_end


def _process_window(
    client: httpx.Client,
    engine: Engine,
    settings: Settings,
    *,
    app_id: str,
    attribution_type: AttributionType,
    start_date: datetime.date,
    end_date: datetime.date,
    dry_run: bool,
) -> WindowResult:
    """Fetch -> transform -> (load unless dry_run) for one unit.

    Catches (AppsFlyerAPIError, TransformError, PipelineError) into
    WindowResult.error rather than raising -- that's the per-unit isolation.
    Deliberately does NOT catch bare Exception: an unexpected bug must crash
    loudly, not get silently absorbed into a result row.
    """
    logger.info(
        "fetching app_id=%s attribution_type=%s window=[%s, %s]",
        app_id,
        attribution_type,
        start_date,
        end_date,
    )
    try:
        raw_df = fetch_events(
            client,
            app_id=app_id,
            attribution_type=attribution_type,
            from_date=start_date,
            to_date=end_date,
            api_token=settings.appsflyer_api_token,
            media_source=settings.appsflyer_media_source,
            event_names=settings.appsflyer_event_names,
        )
        fetched_rows = raw_df.height

        rows: list[dict[str, Any]] = transform_events(
            raw_df,
            attribution_type=attribution_type,
            app_id=app_id,
            media_source_filter=settings.appsflyer_media_source,
            event_names_filter=settings.appsflyer_event_names,
        )

        if dry_run:
            loaded_rows = len(rows)
        else:
            loaded_rows = load_events(
                engine,
                settings.db_table,
                rows,
                app_id=app_id,
                attribution_type=attribution_type,
                start_date=start_date,
                end_date=end_date,
            )
    except (AppsFlyerAPIError, TransformError, PipelineError) as exc:
        logger.error(
            "failed app_id=%s attribution_type=%s window=[%s, %s]: %s",
            app_id,
            attribution_type,
            start_date,
            end_date,
            exc,
        )
        return WindowResult(
            app_id=app_id,
            attribution_type=attribution_type,
            start_date=start_date,
            end_date=end_date,
            fetched_rows=0,
            loaded_rows=0,
            error=f"{type(exc).__name__}: {exc}",
        )

    logger.info(
        "done app_id=%s attribution_type=%s window=[%s, %s] fetched=%d loaded=%d",
        app_id,
        attribution_type,
        start_date,
        end_date,
        fetched_rows,
        loaded_rows,
    )
    return WindowResult(
        app_id=app_id,
        attribution_type=attribution_type,
        start_date=start_date,
        end_date=end_date,
        fetched_rows=fetched_rows,
        loaded_rows=loaded_rows,
    )


def _warn_if_before_retention_floor(day: datetime.date, what: str) -> None:
    """Issue #28: the floor anchors to TODAY (the API retains a trailing
    window), never to a caller-provided end date -- an explicit past
    --end-date used to skip this warning for fully-beyond-retention windows.
    Warn-and-proceed is deliberate (RUNBOOK §9's probes rely on it). This is
    the API's documented/HTTP-400 boundary; the *silent* empty-response
    boundary is shorter -- see issue #45.
    """
    retention_floor = _today() - datetime.timedelta(days=MAX_RETENTION_DAYS)
    if day < retention_floor:
        logger.warning(
            "Requested %s %s is earlier than the AppsFlyer Pull API's ~%d-day "
            "retention floor (%s) — requests before the floor may return empty "
            "data or an error. Proceeding anyway.",
            what,
            day,
            MAX_RETENTION_DAYS,
            retention_floor,
        )


def _run_window(start: datetime.date, end: datetime.date, *, dry_run: bool) -> RunSummary:
    """Shared core for run_backfill/run_daily: preflight, then a sequential loop."""
    settings = get_settings()
    engine = create_engine(settings)

    if not dry_run:
        status = check_connection(engine, settings.db_table)
        if not status.table_exists:
            raise PipelineError(
                f"Target table `{settings.db_table}` does not exist yet — "
                "run `appsflyer-pipeline create-table` first."
            )

    results: list[WindowResult] = []
    with httpx.Client() as client:
        for app_id, attribution_type, chunk_start, chunk_end in _iter_work_items(
            settings, start, end
        ):
            results.append(
                _process_window(
                    client,
                    engine,
                    settings,
                    app_id=app_id,
                    attribution_type=attribution_type,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    dry_run=dry_run,
                )
            )
    return RunSummary(results=results, dry_run=dry_run)


def run_backfill(
    start: datetime.date | None = None,
    end: datetime.date | None = None,
    *,
    dry_run: bool = False,
) -> RunSummary:
    """Historical backfill. Defaults to the full available AppsFlyer window:
    [yesterday - (MAX_RETENTION_DAYS - 1), yesterday].

    If an explicit `start` predates the retention floor (today minus
    MAX_RETENTION_DAYS), this does NOT clamp it — it logs a warning and
    proceeds, so an operator can deliberately probe what AppsFlyer actually
    returns for old dates (see RUNBOOK §9 and issue #45).
    """
    end = end or (_today() - datetime.timedelta(days=1))
    default_start = end - datetime.timedelta(days=MAX_RETENTION_DAYS - 1)
    start = start or default_start

    if start > end:
        raise PipelineError(f"start {start} is after end {end}")
    _warn_if_before_retention_floor(start, "backfill start")

    return _run_window(start, end, dry_run=dry_run)


def run_daily(*, date: datetime.date | None = None, dry_run: bool = False) -> RunSummary:
    """Daily incremental load, sharing run_backfill's fetch/transform/load path.

    The default window is [yesterday - (N-1), yesterday] where N is
    settings.appsflyer_daily_lookback_days — the same days_back shape as the
    backfill, re-pulling recent days on every run so late/offline-cached
    AppsFlyer events get captured (issue #8). N=1 (the default) is the
    original single-day pull. Idempotent delete-then-insert makes the daily
    rewrite of recent days safe by construction.

    Window precedence (issue #50): an explicit `date` (targeted repair,
    exactly [date, date]) wins over everything; otherwise a configured
    APPSFLYER_EVENT_TIME_FROM/TO window wins over the lookback — it maps
    straight onto the API's event-time from/to params, like the reference
    script's from_date/to_date arguments, with TO defaulting to yesterday.
    """
    if date is not None:
        _warn_if_before_retention_floor(date, "daily --date")
        return _run_window(date, date, dry_run=dry_run)

    settings = get_settings()
    if settings.appsflyer_event_time_from is not None:
        start = settings.appsflyer_event_time_from
        end = settings.appsflyer_event_time_to or (_today() - datetime.timedelta(days=1))
        if start > end:
            raise PipelineError(f"APPSFLYER_EVENT_TIME_FROM {start} is after the window end {end}")
        _warn_if_before_retention_floor(start, "APPSFLYER_EVENT_TIME_FROM")
        return _run_window(start, end, dry_run=dry_run)

    end = _today() - datetime.timedelta(days=1)
    start = end - datetime.timedelta(days=settings.appsflyer_daily_lookback_days - 1)
    return _run_window(start, end, dry_run=dry_run)
