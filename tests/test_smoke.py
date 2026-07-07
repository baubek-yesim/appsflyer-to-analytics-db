"""Scaffold smoke test: package imports and the CLI entrypoint is wired up.

Real Stage 1+ tests (config, DB connectivity, transform, chunking, loader
SQL, AppsFlyer client) land as those modules are built.
"""

from typer.testing import CliRunner

from appsflyer_pipeline import __version__
from appsflyer_pipeline.cli import app

runner = CliRunner()


def test_version_is_set() -> None:
    assert __version__


def test_cli_version_command() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout
