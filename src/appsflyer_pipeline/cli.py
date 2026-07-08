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

from appsflyer_pipeline import __version__
from appsflyer_pipeline.config import get_settings
from appsflyer_pipeline.loader import PipelineError, check_connection, create_engine, create_table
from appsflyer_pipeline.logging_config import configure_logging
from appsflyer_pipeline.pipeline import RunSummary, run_backfill, run_daily

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
    """Verify connectivity to the analytics MariaDB and report the target table's status."""
    settings = get_settings()
    engine = create_engine(settings)
    try:
        status = check_connection(engine, settings.db_table)
    except PipelineError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Connected. MariaDB server version: {status.server_version}")
    if status.table_exists:
        typer.echo(f"Table `{settings.db_table}` exists ({status.row_count} rows).")
    else:
        typer.echo(f"Table `{settings.db_table}` does not exist yet (run `create-table`).")


@app.command(name="create-table")
def create_table_command() -> None:
    """Create the target table if it doesn't already exist (idempotent)."""
    settings = get_settings()
    engine = create_engine(settings)
    try:
        create_table(engine, settings.db_table)
    except PipelineError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Table `{settings.db_table}` is ready.")


def _parse_optional_date(value: str | None, flag_name: str) -> datetime.date | None:
    """Tightly-scoped date parsing — only catches the fromisoformat ValueError,
    so a real config error (e.g. pydantic.ValidationError from get_settings(),
    which does NOT subclass ValueError) is never accidentally swallowed here.
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
        None, "--start-date", help="ISO date (YYYY-MM-DD); defaults to 90 days before yesterday."
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
    except (PipelineError, ValueError) as exc:
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
    except (PipelineError, ValueError) as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    _print_summary(summary)
    if not summary.all_succeeded:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
