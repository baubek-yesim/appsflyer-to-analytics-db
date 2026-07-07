from __future__ import annotations

import httpx
import pytest
import respx
from typer.testing import CliRunner

from appsflyer_pipeline.cli import app
from appsflyer_pipeline.config import get_settings

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
    "AppsFlyer ID,Customer User ID\n"
    "2026-05-20 10:00:00,2026-05-19 09:00:00,2026-05-20 10:05:00,af_purchase,9.99,"
    "Facebook Ads,Social,Summer Sale,cmp-1,Adset A,adset-1,Ad A,ad-1,af-id-1,user-1\n"
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
