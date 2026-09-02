"""Command-line entrypoint for the AppsFlyer -> analytics DB pipeline.

Commands are added incrementally as the pipeline is built:
  - version           (scaffold)
  - check-connection   Stage 1
  - create-table       Stage 2
  - backfill / daily   Stage 5
"""

from __future__ import annotations

import datetime

import typer
from pydantic import ValidationError

from appsflyer_pipeline import __version__
from appsflyer_pipeline.config import Settings, get_settings
from appsflyer_pipeline.loader import PipelineError, check_connection, create_engine, create_table
from appsflyer_pipeline.logging_config import configure_logging
from appsflyer_pipeline.pipeline import RunSummary, run_backfill, run_daily
from appsflyer_pipeline.reports import REPORTS

app = typer.Typer(
    name="appsflyer-pipeline",
    help="Load AppsFlyer Pull API purchase events into the analytics MariaDB (BAF-2).",
    no_args_is_help=True,
)


@app.callback()
def main() -> None:
    """Load AppsFlyer Pull API purchase events into the analytics MariaDB (BAF-2).

    An explicit callback (even empty) keeps Typer in subcommand-dispatch mode —
    without it, a Typer app with a single command silently "collapses" so the
    command name itself is rejected as a stray argument.
    """
    configure_logging()


@app.command()
def version() -> None:
    """Print the installed package version."""
    typer.echo(__version__)


@app.command(name="check-connection")
def check_connection_command() -> None:
    """Verify connectivity to the analytics MariaDB and report every active
    report's target table status (BAF-11 stage 3: today that's exactly one
    table, appsflyer_events_fb, shared by both registered ReportSpecs).
    """
    settings = _get_settings_or_exit()
    engine = create_engine(settings)
    tables = sorted({spec.table(settings) for spec in REPORTS.values()})
    try:
        statuses = [(table, check_connection(engine, table)) for table in tables]
    except PipelineError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Connected. MariaDB server version: {statuses[0][1].server_version}")
    for table, status in statuses:
        if status.table_exists:
            typer.echo(f"Table `{table}` exists ({status.row_count} rows).")
        else:
            typer.echo(f"Table `{table}` does not exist yet (run `create-table`).")


@app.command(name="create-table")
def create_table_command() -> None:
    """Create every active report's target table if it doesn't already exist
    (idempotent). BAF-11 stage 4: two distinct tables -- in-app-events'
    17-column schema and installs' 128-column one.
    """
    settings = _get_settings_or_exit()
    engine = create_engine(settings)
    tables = sorted({(spec.table(settings), spec.name) for spec in REPORTS.values()})
    try:
        for table, report_name in tables:
            create_table(engine, table, report_name)
    except PipelineError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    for table, _report_name in tables:
        typer.echo(f"Table `{table}` is ready.")


def _parse_optional_date(value: str | None, flag_name: str) -> datetime.date | None:
    """Tightly-scoped date parsing — only catches the fromisoformat ValueError.
    A real config error (e.g. pydantic.ValidationError from get_settings()) is
    a different exception path entirely: it's raised inside
    _get_settings_or_exit, never here.
    """
    if value is None:
        return None
    try:
        return datetime.date.fromisoformat(value)
    except ValueError as exc:
        typer.echo(
            f"FAILED: invalid {flag_name} {value!r}: must be an ISO date (YYYY-MM-DD)", err=True
        )
        raise typer.Exit(code=1) from exc


def _format_validation_error(exc: ValidationError) -> str:
    """Render a config error without echoing collected env values (issue #27):
    pydantic's default str() embeds input_value=<the collected settings dict>,
    which leaks DB-password/API-token material into stderr and journald when
    the EnvironmentFile is truncated -- #9's exact scenario.
    """
    problems = "; ".join(
        f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}"
        for err in exc.errors(include_url=False, include_input=False)
    )
    return f"invalid configuration: {problems}"


def _get_settings_or_exit() -> Settings:
    """Load settings, rendering a bad/missing env var as a clean FAILED message
    (issue #12) instead of an uncaught pydantic ValidationError traceback --
    the same treatment every command already gives a PipelineError.
    """
    try:
        return get_settings()
    except ValidationError as exc:
        typer.echo(f"FAILED: {_format_validation_error(exc)}", err=True)
        raise typer.Exit(code=1) from exc


def _print_summary(summary: RunSummary) -> None:
    for r in summary.results:
        if r.succeeded:
            typer.echo(
                f"  OK   {r.app_id} [{r.attribution_type}] {r.start_date}..{r.end_date}: "
                f"fetched={r.fetched_rows} loaded={r.loaded_rows}"
            )
        else:
            typer.echo(
                f"  FAIL {r.app_id} [{r.attribution_type}] {r.start_date}..{r.end_date}: {r.error}",
                err=True,
            )
    verb = "Would load" if summary.dry_run else "Loaded"
    typer.echo(
        f"{verb} {summary.total_loaded} rows across "
        f"{len(summary.succeeded)}/{len(summary.results)} windows."
    )


@app.command()
def backfill(
    start_date: str | None = typer.Option(
        None,
        "--start-date",
        help=(
            "ISO date (YYYY-MM-DD); defaults to a 90-day window ending at "
            "--end-date (or yesterday)."
        ),
    ),
    end_date: str | None = typer.Option(
        None, "--end-date", help="ISO date (YYYY-MM-DD); defaults to yesterday."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Fetch and transform but don't write to the database."
    ),
) -> None:
    """Historical backfill: pulls the full available AppsFlyer window (up to 90 days)."""
    start = _parse_optional_date(start_date, "--start-date")
    end = _parse_optional_date(end_date, "--end-date")

    try:
        summary = run_backfill(start, end, dry_run=dry_run)
    except ValidationError as exc:
        typer.echo(f"FAILED: {_format_validation_error(exc)}", err=True)
        raise typer.Exit(code=1) from exc
    except PipelineError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    _print_summary(summary)
    if not summary.all_succeeded:
        raise typer.Exit(code=1)


@app.command()
def daily(
    date: str | None = typer.Option(
        None,
        "--date",
        help=(
            "ISO date (YYYY-MM-DD): pull exactly this one day (targeted repair), "
            "ignoring APPSFLYER_DAILY_LOOKBACK_DAYS. Default: the trailing "
            "lookback window ending yesterday."
        ),
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Fetch and transform but don't write to the database."
    ),
) -> None:
    """Daily incremental load: pulls the trailing lookback window (default: yesterday
    only) from both sources."""
    target_date = _parse_optional_date(date, "--date")

    try:
        summary = run_daily(date=target_date, dry_run=dry_run)
    except ValidationError as exc:
        typer.echo(f"FAILED: {_format_validation_error(exc)}", err=True)
        raise typer.Exit(code=1) from exc
    except PipelineError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    _print_summary(summary)
    if not summary.all_succeeded:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
