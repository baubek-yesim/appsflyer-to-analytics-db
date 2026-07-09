from __future__ import annotations

import datetime

import httpx
import pytest
import respx
from pydantic import ValidationError
from typer.testing import CliRunner

from appsflyer_pipeline import cli
from appsflyer_pipeline.cli import app
from appsflyer_pipeline.config import Settings, get_settings
from appsflyer_pipeline.loader import ConnectionStatus, PipelineError
from appsflyer_pipeline.pipeline import RunSummary

runner = CliRunner()

UNREACHABLE_ENV = {
    "DB_HOST": "127.0.0.1",
    "DB_PORT": "59999",  # nothing listens here -> connection refused, fast
    "DB_USER": "user",
    "DB_PASSWORD": "pw",
    "DB_NAME": "db",
    "DB_TABLE": "some_table",
    "APPSFLYER_API_TOKEN": "token",
    "APPSFLYER_APP_IDS": "id1",
}

# Dry-run backfill/daily never touch the DB (preflight + load are both skipped),
# so a fake, unreachable DB host is fine here too — only the AppsFlyer HTTP
# calls need mocking.
CLI_ENV = {**UNREACHABLE_ENV, "APPSFLYER_APP_IDS": "app1"}

SAMPLE_CSV = (
    "Attributed Touch Time,Install Time,Event Time,Event Name,Event Revenue,"
    "Media Source,Channel,Campaign,Campaign ID,Adset,Adset ID,Ad,Ad ID,"
    "AppsFlyer ID,Customer User ID,Is Primary Attribution\n"
    "2026-05-20 10:00:00,2026-05-19 09:00:00,2026-05-20 10:05:00,af_purchase,9.99,"
    "Facebook Ads,Social,Summer Sale,cmp-1,Adset A,adset-1,Ad A,ad-1,af-id-1,user-1,true\n"
)


def _set_cli_env(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    for key, value in {**CLI_ENV, **overrides}.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()


def _af_url(app_id: str, attribution_type: str) -> str:
    endpoint = (
        "in_app_events_report" if attribution_type == "non_organic" else "in-app-events-retarget"
    )
    return f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/{endpoint}/v5"


def test_check_connection_reports_failure_for_unreachable_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key, value in UNREACHABLE_ENV.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()

    result = runner.invoke(app, ["check-connection"])

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAILED" in result.output


@pytest.mark.parametrize(
    ("table_exists", "row_count", "expected_fragment"),
    [
        (True, 42, "exists (42 rows)"),
        (False, None, "does not exist yet"),
    ],
)
def test_check_connection_reports_status_for_both_branches(
    monkeypatch: pytest.MonkeyPatch,
    table_exists: bool,
    row_count: int | None,
    expected_fragment: str,
) -> None:
    _set_cli_env(monkeypatch)
    monkeypatch.setattr(
        cli,
        "check_connection",
        lambda engine, table_name: ConnectionStatus(
            server_version="8.0.35", table_exists=table_exists, row_count=row_count
        ),
    )

    result = runner.invoke(app, ["check-connection"])

    get_settings.cache_clear()
    assert result.exit_code == 0
    assert expected_fragment in result.output


def test_create_table_success_reports_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_cli_env(monkeypatch)
    monkeypatch.setattr(cli, "create_table", lambda engine, table_name: None)

    result = runner.invoke(app, ["create-table"])

    get_settings.cache_clear()
    assert result.exit_code == 0
    assert "is ready." in result.output


def test_create_table_reports_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_cli_env(monkeypatch)

    def _raise(engine: object, table_name: str) -> None:
        raise PipelineError(f"Could not create table `{table_name}`: boom")

    monkeypatch.setattr(cli, "create_table", _raise)

    result = runner.invoke(app, ["create-table"])

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAILED" in result.output


def test_check_connection_reports_config_error_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    """Issue #12: a bad/missing env var must fail the same clean way as
    backfill/daily (FAILED: ... + exit 1), not an uncaught pydantic
    ValidationError traceback.
    """
    _set_cli_env(monkeypatch, APPSFLYER_APP_IDS="")

    result = runner.invoke(app, ["check-connection"])

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAILED" in result.output


def test_create_table_reports_config_error_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_cli_env(monkeypatch, APPSFLYER_APP_IDS="")

    result = runner.invoke(app, ["create-table"])

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAILED" in result.output


def test_format_validation_error_never_includes_input_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #27: pydantic renders missing-field errors with
    input_value=<the whole collected env dict>, whose repr tail exposes the
    DB password when the EnvironmentFile is truncated right after
    DB_PASSWORD (#9's exact scenario). The formatter must render field
    locations and messages only. Built with _env_file=None so the
    developer's real .env cannot fill the missing fields (see issue #32).
    """
    sentinel = "S3CRET-SENTINEL-VALUE-Tt8Rr"
    for key in (
        "DB_NAME",
        "DB_TABLE",
        "APPSFLYER_API_TOKEN",
        "APPSFLYER_APP_IDS",
        "APPSFLYER_MEDIA_SOURCE",
        "APPSFLYER_EVENT_NAMES",
        "APPSFLYER_DAILY_LOOKBACK_DAYS",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DB_HOST", "db-host")
    monkeypatch.setenv("DB_PORT", "3306")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", sentinel)

    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None)  # type: ignore[call-arg]

    raw = str(excinfo.value)
    clean = cli._format_validation_error(excinfo.value)
    assert "T-SENTINEL-VALUE-Tt8Rr" in raw  # the visible leak portion
    assert sentinel not in clean
    assert "T-SENTINEL-VALUE-Tt8Rr" not in clean  # the truncated tail, the only form the leak takes
    assert clean.startswith("invalid configuration: ")
    assert "db_name" in clean
    assert "input_value" not in clean


def test_check_connection_config_error_renders_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wiring check for _get_settings_or_exit: an empty DB_NAME (issue #29's
    validator -- an explicit env var, so the real .env can't mask it) must
    render through the formatter, without pydantic's input_value= echo.
    """
    _set_cli_env(monkeypatch, DB_NAME="")

    result = runner.invoke(app, ["check-connection"])

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAILED: invalid configuration:" in result.output
    assert "db_name" in result.output
    assert "input_value" not in result.output


def test_backfill_config_error_renders_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wiring check for the backfill except-handler path (get_settings() is
    called inside run_backfill, so the ValidationError surfaces there, not in
    _get_settings_or_exit).
    """
    _set_cli_env(monkeypatch, APPSFLYER_MEDIA_SOURCE="")

    result = runner.invoke(app, ["backfill", "--dry-run"])

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAILED: invalid configuration:" in result.output
    assert "appsflyer_media_source" in result.output
    assert "input_value" not in result.output


@respx.mock
def test_backfill_dry_run_success_exits_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_cli_env(monkeypatch)
    for attribution_type in ("non_organic", "retargeting"):
        respx.get(_af_url("app1", attribution_type)).mock(
            return_value=httpx.Response(200, text=SAMPLE_CSV)
        )

    result = runner.invoke(
        app,
        ["backfill", "--start-date", "2026-05-20", "--end-date", "2026-05-20", "--dry-run"],
    )

    get_settings.cache_clear()
    assert result.exit_code == 0
    assert "Would load" in result.output


@respx.mock
def test_backfill_partial_failure_exits_one_with_fail_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_cli_env(monkeypatch)
    respx.get(_af_url("app1", "non_organic")).mock(
        return_value=httpx.Response(200, text=SAMPLE_CSV)
    )
    respx.get(_af_url("app1", "retargeting")).mock(return_value=httpx.Response(401, text="nope"))

    result = runner.invoke(
        app,
        ["backfill", "--start-date", "2026-05-20", "--end-date", "2026-05-20", "--dry-run"],
    )

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAIL" in result.output


def test_backfill_invalid_start_date_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    """Never touches the network — invalid date is caught before run_backfill()."""
    _set_cli_env(monkeypatch)

    result = runner.invoke(app, ["backfill", "--start-date", "not-a-date"])

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAILED" in result.output
    assert "ISO date" in result.output


@respx.mock
def test_backfill_early_start_is_not_silently_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """The CLI doesn't silently clamp/reject a pre-retention-floor start date --
    it still attempts the window. (The pipeline-level warning log is asserted
    via caplog in test_pipeline.py; raw `logging` output isn't reliably
    capturable through CliRunner's per-invocation stdout redirection once
    configure_logging()'s handler is already bound from an earlier test.)
    """
    _set_cli_env(monkeypatch)
    for attribution_type in ("non_organic", "retargeting"):
        respx.get(_af_url("app1", attribution_type)).mock(
            return_value=httpx.Response(200, text=SAMPLE_CSV)
        )

    result = runner.invoke(
        app,
        ["backfill", "--start-date", "2020-01-01", "--end-date", "2020-01-01", "--dry-run"],
    )

    get_settings.cache_clear()
    assert result.exit_code == 0
    assert "2020-01-01" in result.output


@respx.mock
def test_daily_dry_run_success_exits_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_cli_env(monkeypatch)
    for attribution_type in ("non_organic", "retargeting"):
        respx.get(_af_url("app1", attribution_type)).mock(
            return_value=httpx.Response(200, text=SAMPLE_CSV)
        )

    result = runner.invoke(app, ["daily", "--date", "2026-05-20", "--dry-run"])

    get_settings.cache_clear()
    assert result.exit_code == 0
    assert "Would load" in result.output


@respx.mock
def test_daily_partial_failure_exits_one_with_fail_line(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_cli_env(monkeypatch)
    respx.get(_af_url("app1", "non_organic")).mock(
        return_value=httpx.Response(200, text=SAMPLE_CSV)
    )
    respx.get(_af_url("app1", "retargeting")).mock(return_value=httpx.Response(401, text="nope"))

    result = runner.invoke(app, ["daily", "--date", "2026-05-20", "--dry-run"])

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAIL" in result.output


def test_backfill_start_after_end_reports_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both dates are valid ISO, so CLI-level parsing succeeds; run_backfill()
    itself raises PipelineError("start ... is after end ...") before touching
    the network or DB -- distinct from test_backfill_invalid_start_date_fails_fast,
    which covers the CLI-level date-parsing failure instead.
    """
    _set_cli_env(monkeypatch)

    result = runner.invoke(
        app,
        ["backfill", "--start-date", "2026-05-20", "--end-date", "2026-05-01"],
    )

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAILED" in result.output


def test_daily_reports_failure_when_run_daily_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """run_daily()'s only non-ValueError raise is the missing-table preflight
    PipelineError; isolate it directly rather than standing up a real (fake)
    unreachable DB + non-dry-run path just to reach the same except clause.
    """
    _set_cli_env(monkeypatch)

    def _raise(*, date: datetime.date | None, dry_run: bool) -> RunSummary:
        raise PipelineError("Target table `some_table` does not exist yet")

    monkeypatch.setattr(cli, "run_daily", _raise)

    result = runner.invoke(app, ["daily", "--date", "2026-05-20"])

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAILED" in result.output
