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
from appsflyer_pipeline.reports import REPORTS, ReportSpec
from appsflyer_pipeline.transform import TransformError, transform_events

logger = logging.getLogger(__name__)


def _today() -> datetime.date:
    """Seam for tests: monkeypatch this rather than datetime.date.today directly."""
    return datetime.date.today()


def _active_retention_days() -> int:
    """The narrowest retention_days across every registered REPORTS entry.

    BAF-11 stage 4: NOT consumed by run_backfill/run_daily's default-window or
    warn-threshold math -- those use MAX_RETENTION_DAYS instead, since they're
    about in-app-events' own (hard_clamp_retention=False) floor specifically,
    and installs (hard_clamp_retention=True, 60 days) already clamps its own
    effective start inside _iter_work_items regardless of what start/end this
    function's callers would otherwise compute. Kept as a small, still-correct
    diagnostic helper (min(...) over REPORTS, currently 60 now that installs
    is registered) with no production call site -- not dead code to delete,
    per this stage's plan.
    """
    return min(spec.retention_days for spec in REPORTS.values())


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
) -> Iterator[tuple[ReportSpec, str, datetime.date, datetime.date]]:
    """(report x app_id x <=chunk_days chunk) for the [start, end] window.

    Mostly pure -- the one exception (BAF-11 stage 4) is `spec.hard_clamp_retention`
    specs, which read `_today()` to clamp their effective start date up to the
    retention floor; monkeypatch `_today` for a deterministic test, same
    pattern already used elsewhere in this module. Loop order (app_id outer,
    report inner) is unchanged from the pre-ReportSpec app_id/attribution_type
    nesting -- see the Stage 3 plan's Architecture decision 4 for why it
    wasn't reordered to match the master spec's "report x app_id" prose.
    """
    for app_id in settings.appsflyer_app_ids:
        for spec in REPORTS.values():
            spec_start = start
            if spec.hard_clamp_retention:
                # BAF-11 stage 4 (installs): a response past the real
                # retention boundary can come back as a valid, header-only
                # EMPTY report (issue #45's shape) -- the idempotent
                # delete-then-insert would then wipe a window that may have
                # had real data. In-app-events (hard_clamp_retention=False)
                # deliberately keeps warn-and-proceed instead -- see this
                # stage's plan, Architecture decision 3.
                retention_floor = _today() - datetime.timedelta(days=spec.retention_days)
                spec_start = max(start, retention_floor)
                if spec_start > end:
                    logger.warning(
                        "skipping %s for app_id=%s: requested window [%s, %s] is entirely "
                        "before the %d-day retention floor (%s) -- nothing to fetch",
                        spec.name,
                        app_id,
                        start,
                        end,
                        spec.retention_days,
                        retention_floor,
                    )
                    continue
                if spec_start > start:
                    logger.warning(
                        "clamping %s for app_id=%s: requested start %s is before the "
                        "%d-day retention floor -- fetching from %s instead (a response "
                        "for dates before the floor can come back silently empty, and the "
                        "idempotent delete-then-insert would wipe any already-loaded data "
                        "there)",
                        spec.name,
                        app_id,
                        start,
                        spec.retention_days,
                        spec_start,
                    )
            for chunk_start, chunk_end in chunk_date_range(
                spec_start, end, max_days=settings.appsflyer_chunk_days
            ):
                yield spec, app_id, chunk_start, chunk_end


def _process_window(
    client: httpx.Client,
    engine: Engine,
    settings: Settings,
    *,
    spec: ReportSpec,
    app_id: str,
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
    attribution_type = spec.attribution_type
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
            spec=spec,
            from_date=start_date,
            to_date=end_date,
            api_token=settings.appsflyer_api_token,
            media_source=settings.appsflyer_media_source,
            event_names=settings.appsflyer_event_names,
            timezone=settings.appsflyer_timezone,
        )
        fetched_rows = raw_df.height

        rows: list[dict[str, Any]] = transform_events(
            raw_df,
            spec=spec,
            app_id=app_id,
            media_source_filter=settings.appsflyer_media_source,
            event_names_filter=settings.appsflyer_event_names,
        )

        if dry_run:
            loaded_rows = len(rows)
        else:
            loaded_rows = load_events(
                engine,
                spec,
                spec.table(settings),
                rows,
                app_id=app_id,
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


def _warn_if_before_retention_floor(day: datetime.date, what: str, *, retention_days: int) -> None:
    """Issue #28: the floor anchors to TODAY (the API retains a trailing
    window), never to a caller-provided end date -- an explicit past
    --end-date used to skip this warning for fully-beyond-retention windows.
    Warn-and-proceed is deliberate (RUNBOOK §9's probes rely on it). This is
    the API's documented/HTTP-400 boundary; the *silent* empty-response
    boundary is shorter -- see issue #45.

    `retention_days` is per-run (BAF-11 stage 3), resolved by the caller via
    `_active_retention_days()` -- not a module-level constant, since a future
    report can carry a shorter retention than in-app-events' 90 days.
    """
    retention_floor = _today() - datetime.timedelta(days=retention_days)
    if day < retention_floor:
        logger.warning(
            "Requested %s %s is earlier than the AppsFlyer Pull API's ~%d-day "
            "retention floor (%s) — requests before the floor may return empty "
            "data or an error. Proceeding anyway.",
            what,
            day,
            retention_days,
            retention_floor,
        )


def _log_filter_mode(settings: Settings) -> None:
    """Say once per run which rows this run is even eligible to load.

    Since BAF-11 stage 1 that is a config decision, not a constant, and the two
    modes differ by orders of magnitude (7,707 unfiltered event rows/day vs. a
    couple of dozen Meta purchases, measured 2026-08-13). Unfiltered is the
    wide-blast-radius mode and is logged at WARNING deliberately: until stage 5
    routes the full export to its own table, an unnoticed unfiltered run pours
    every media source into BAF-2's `appsflyer_events_fb`. Downgrade this to
    INFO once that routing exists.
    """
    media_source = settings.appsflyer_media_source
    event_names = settings.appsflyer_event_names
    shown_media_source = f"'{media_source}'" if media_source is not None else "<all>"
    shown_event_names = ",".join(event_names) if event_names is not None else "<all>"
    message = f"filter mode: media_source={shown_media_source} event_name={shown_event_names}"
    if media_source is None or event_names is None:
        logger.warning(
            "%s — UNFILTERED pull, every matching row lands in `%s`", message, settings.db_table
        )
    else:
        logger.info(message)


def _run_window(start: datetime.date, end: datetime.date, *, dry_run: bool) -> RunSummary:
    """Shared core for run_backfill/run_daily: preflight, then a sequential loop."""
    settings = get_settings()
    engine = create_engine(settings)
    _log_filter_mode(settings)

    if not dry_run:
        for table in sorted({spec.table(settings) for spec in REPORTS.values()}):
            status = check_connection(engine, table)
            if not status.table_exists:
                raise PipelineError(
                    f"Target table `{table}` does not exist yet — "
                    "run `appsflyer-pipeline create-table` first."
                )

    results: list[WindowResult] = []
    with httpx.Client() as client:
        for spec, app_id, chunk_start, chunk_end in _iter_work_items(settings, start, end):
            results.append(
                _process_window(
                    client,
                    engine,
                    settings,
                    spec=spec,
                    app_id=app_id,
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
    MAX_RETENTION_DAYS, in-app-events' own retention — this run-level default/
    warn threshold is about in-app-events specifically, not the cross-REPORTS
    minimum; see the BAF-11 stage 4 comment below), this does NOT clamp it —
    it logs a warning and proceeds, so an operator can deliberately probe what
    AppsFlyer actually returns for old dates (see RUNBOOK §9 and issue #45).
    installs (hard_clamp_retention=True) is unaffected by this: it clamps its
    own effective start inside `_iter_work_items` regardless of this
    function's `start`/`end`.
    """
    # BAF-11 stage 4: do NOT use _active_retention_days() (the cross-REPORTS
    # minimum) here. Once installs (retention_days=60, hard_clamp_retention=
    # True) is registered, that minimum drops from 90 to 60 -- but installs
    # already clamps its OWN effective start inside _iter_work_items
    # regardless of what start this function resolves to (Architecture
    # decision 3). Keying the run-level default/warn threshold off the global
    # minimum would silently narrow in-app-events' no-args default window
    # from 90 days to 60, contradicting this stage's regression bar
    # (in-app-events' warn-only retention behavior stays byte-for-byte
    # unchanged) -- caught by test_run_backfill_default_window_is_90_days.
    # MAX_RETENTION_DAYS is what this caller-facing default/warn threshold is
    # actually about: the widest retention among hard_clamp_retention=False
    # specs (today, in-app-events only).
    end = end or (_today() - datetime.timedelta(days=1))
    default_start = end - datetime.timedelta(days=MAX_RETENTION_DAYS - 1)
    start = start or default_start

    if start > end:
        raise PipelineError(f"start {start} is after end {end}")
    _warn_if_before_retention_floor(start, "backfill start", retention_days=MAX_RETENTION_DAYS)

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
    # BAF-11 stage 4: same reasoning as run_backfill above -- MAX_RETENTION_DAYS,
    # not _active_retention_days()'s cross-REPORTS minimum. installs clamps
    # itself inside _iter_work_items; these two warn call sites are about
    # in-app-events' own (hard_clamp_retention=False) floor.
    if date is not None:
        _warn_if_before_retention_floor(date, "daily --date", retention_days=MAX_RETENTION_DAYS)
        return _run_window(date, date, dry_run=dry_run)

    settings = get_settings()
    if settings.appsflyer_event_time_from is not None:
        start = settings.appsflyer_event_time_from
        end = settings.appsflyer_event_time_to or (_today() - datetime.timedelta(days=1))
        if start > end:
            raise PipelineError(f"APPSFLYER_EVENT_TIME_FROM {start} is after the window end {end}")
        _warn_if_before_retention_floor(
            start, "APPSFLYER_EVENT_TIME_FROM", retention_days=MAX_RETENTION_DAYS
        )
        return _run_window(start, end, dry_run=dry_run)

    end = _today() - datetime.timedelta(days=1)
    start = end - datetime.timedelta(days=settings.appsflyer_daily_lookback_days - 1)
    return _run_window(start, end, dry_run=dry_run)
