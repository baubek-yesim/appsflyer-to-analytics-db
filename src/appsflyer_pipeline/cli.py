"""Command-line entrypoint for the AppsFlyer -> analytics DB pipeline.

Commands are added incrementally as the pipeline is built:
  - version           (scaffold)
  - check-connection   Stage 1
  - create-table       Stage 2
  - backfill / daily   Stage 5
"""

from __future__ import annotations

import typer

from appsflyer_pipeline import __version__
from appsflyer_pipeline.config import get_settings
from appsflyer_pipeline.loader import PipelineError, check_connection, create_engine, create_table

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


if __name__ == "__main__":
    app()
