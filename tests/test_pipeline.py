from __future__ import annotations

import datetime
import logging
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
import respx

from appsflyer_pipeline import pipeline
from appsflyer_pipeline.appsflyer_client import MAX_RETENTION_DAYS
from appsflyer_pipeline.config import get_settings
from appsflyer_pipeline.loader import ConnectionStatus, PipelineError
from appsflyer_pipeline.pipeline import _iter_work_items, run_backfill, run_daily

SAMPLE_CSV = (
    "Attributed Touch Time,Install Time,Event Time,Event Name,Event Revenue,"
    "Media Source,Channel,Campaign,Campaign ID,Adset,Adset ID,Ad,Ad ID,"
    "AppsFlyer ID,Customer User ID,Is Primary Attribution\n"
    "2026-05-20 10:00:00,2026-05-19 09:00:00,2026-05-20 10:05:00,af_purchase,9.99,"
    "Facebook Ads,Social,Summer Sale,cmp-1,Adset A,adset-1,Ad A,ad-1,af-id-1,user-1,true\n"
)
MISSING_COLUMN_CSV = "Event Name\naf_purchase\n"

BASE_ENV = {
    "DB_HOST": "db.example.com",
    "DB_PORT": "3306",
    "DB_USER": "user",
    "DB_PASSWORD": "secret",
    "DB_NAME": "statistics",
    "DB_TABLE": "appsflyer_events",
    "APPSFLYER_API_TOKEN": "token",
    "APPSFLYER_APP_IDS": "app1,app2",
}
APP_IDS = ("app1", "app2")
ATTRIBUTION_TYPES = ("non_organic", "retargeting")


def _set_env(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    for key, value in {**BASE_ENV, **overrides}.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()


def _url(app_id: str, attribution_type: str) -> str:
    endpoint = (
        "in_app_events_report" if attribution_type == "non_organic" else "in-app-events-retarget"
    )
    return f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/{endpoint}/v5"


def _mock_all_ok() -> None:
    for app_id in APP_IDS:
        for attribution_type in ATTRIBUTION_TYPES:
            respx.get(_url(app_id, attribution_type)).mock(
                return_value=httpx.Response(200, text=SAMPLE_CSV)
            )


@pytest.fixture(autouse=True)
def _clear_settings_cache_after() -> Iterator[None]:
    yield
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
def _stub_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-dry-run call triggers _run_window's preflight check_connection,
    which would otherwise try a real connection to the fake DB host. Stub it
    to a canned success — create_engine itself is left real: SQLAlchemy's
    create_engine() is lazy and never connects on its own, so it's harmless
    to call with fake credentials as long as nothing actually queries through it.
    """
    monkeypatch.setattr(
        pipeline,
        "check_connection",
        lambda engine, table_name: ConnectionStatus(
            server_version="test", table_exists=True, row_count=0
        ),
    )


@pytest.fixture
def load_spy(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Replaces load_events with a spy that records calls instead of touching
    a real database; returns the list of calls made to it.
    """
    calls: list[dict[str, Any]] = []

    def _fake_load_events(
        engine: object,
        table_name: str,
        rows: list[dict[str, Any]],
        *,
        app_id: str,
        attribution_type: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> int:
        calls.append(
            {
                "app_id": app_id,
                "attribution_type": attribution_type,
                "start_date": start_date,
                "end_date": end_date,
                "rows": rows,
            }
        )
        return len(rows)

    monkeypatch.setattr(pipeline, "load_events", _fake_load_events)
    return calls


def test_iter_work_items_yields_expected_matrix(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch)
    settings = get_settings()
    start = datetime.date(2026, 1, 1)
    end = datetime.date(2026, 3, 31)  # 89 days -> 3 chunks of <=31 days each
    items = list(_iter_work_items(settings, start, end))

    assert {i[0] for i in items} == set(APP_IDS)
    assert {i[1] for i in items} == set(ATTRIBUTION_TYPES)

    one_series = [(s, e) for a, t, s, e in items if a == "app1" and t == "non_organic"]
    assert one_series[0][0] == start
    assert one_series[-1][1] == end
    assert all((e - s).days < 31 for s, e in one_series)
    assert len(items) == len(APP_IDS) * len(ATTRIBUTION_TYPES) * len(one_series)


@respx.mock
def test_run_daily_success_calls_load_for_every_unit(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    _set_env(monkeypatch)
    _mock_all_ok()

    summary = run_daily(date=datetime.date(2026, 5, 20))

    assert summary.all_succeeded
    assert len(summary.results) == len(APP_IDS) * len(ATTRIBUTION_TYPES)
    assert len(load_spy) == len(APP_IDS) * len(ATTRIBUTION_TYPES)
    assert summary.total_loaded == len(APP_IDS) * len(ATTRIBUTION_TYPES)  # 1 row/unit in SAMPLE_CSV
    assert all(call["start_date"] == datetime.date(2026, 5, 20) for call in load_spy)


@respx.mock
def test_run_daily_dry_run_skips_load(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    _set_env(monkeypatch)
    _mock_all_ok()

    summary = run_daily(date=datetime.date(2026, 5, 20), dry_run=True)

    assert summary.dry_run is True
    assert load_spy == []
    assert summary.total_fetched == len(APP_IDS) * len(ATTRIBUTION_TYPES)
    assert summary.total_loaded == len(APP_IDS) * len(ATTRIBUTION_TYPES)
    assert summary.all_succeeded


@respx.mock
def test_run_daily_isolates_appsflyer_api_error(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    _set_env(monkeypatch)
    respx.get(_url("app1", "non_organic")).mock(return_value=httpx.Response(401, text="nope"))
    respx.get(_url("app1", "retargeting")).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))
    respx.get(_url("app2", "non_organic")).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))
    respx.get(_url("app2", "retargeting")).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))

    summary = run_daily(date=datetime.date(2026, 5, 20))

    assert not summary.all_succeeded
    assert len(summary.failed) == 1
    failed = summary.failed[0]
    assert failed.app_id == "app1"
    assert failed.attribution_type == "non_organic"
    assert failed.error is not None
    assert "AppsFlyerAPIError" in failed.error
    assert len(summary.succeeded) == 3
    assert len(load_spy) == 3  # the failed unit never reaches load_events


@respx.mock
def test_run_daily_isolates_transform_error(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    _set_env(monkeypatch)
    respx.get(_url("app1", "non_organic")).mock(
        return_value=httpx.Response(200, text=MISSING_COLUMN_CSV)
    )
    respx.get(_url("app1", "retargeting")).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))
    respx.get(_url("app2", "non_organic")).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))
    respx.get(_url("app2", "retargeting")).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))

    summary = run_daily(date=datetime.date(2026, 5, 20))

    assert not summary.all_succeeded
    failed = summary.failed[0]
    assert failed.app_id == "app1"
    assert failed.attribution_type == "non_organic"
    assert failed.error is not None
    assert "TransformError" in failed.error


def test_run_backfill_default_window_is_90_days(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    _set_env(monkeypatch)
    fixed_today = datetime.date(2026, 7, 7)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    expected_end = fixed_today - datetime.timedelta(days=1)
    expected_start = expected_end - datetime.timedelta(days=MAX_RETENTION_DAYS - 1)

    with respx.mock:
        _mock_all_ok()
        summary = run_backfill(dry_run=True)

    starts = {r.start_date for r in summary.results}
    ends = {r.end_date for r in summary.results}
    assert min(starts) == expected_start
    assert max(ends) == expected_end


def test_run_backfill_rejects_start_after_end(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch)
    with pytest.raises(ValueError, match="after"):
        run_backfill(start=datetime.date(2026, 5, 20), end=datetime.date(2026, 5, 1))


def test_run_backfill_warns_but_does_not_clamp_early_start(
    monkeypatch: pytest.MonkeyPatch,
    load_spy: list[dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_env(monkeypatch)
    fixed_today = datetime.date(2026, 7, 7)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    very_early_start = datetime.date(2025, 1, 1)
    end = fixed_today - datetime.timedelta(days=1)

    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.pipeline"), respx.mock:
        _mock_all_ok()
        summary = run_backfill(start=very_early_start, end=end, dry_run=True)

    assert any("retention floor" in record.message for record in caplog.records)
    starts = {r.start_date for r in summary.results}
    assert min(starts) == very_early_start  # NOT silently clamped


def test_run_daily_defaults_to_yesterday(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    _set_env(monkeypatch)
    fixed_today = datetime.date(2026, 7, 7)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    expected = fixed_today - datetime.timedelta(days=1)

    with respx.mock:
        _mock_all_ok()
        summary = run_daily(dry_run=True)

    assert all(r.start_date == expected and r.end_date == expected for r in summary.results)


def test_run_window_raises_when_table_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-dry-run call preflights the table's existence -- overrides the
    autouse _stub_preflight fixture's table_exists=True default (this
    setattr, being called later, wins; teardown correctly unwinds both).
    """
    _set_env(monkeypatch)
    monkeypatch.setattr(
        pipeline,
        "check_connection",
        lambda engine, table_name: ConnectionStatus(
            server_version="test", table_exists=False, row_count=None
        ),
    )

    with pytest.raises(PipelineError, match="does not exist"):
        run_daily(date=datetime.date(2026, 5, 20), dry_run=False)


def test_today_returns_a_real_date() -> None:
    # Every other test monkeypatches _today(); this exercises its actual body once.
    # Avoid asserting == datetime.date.today() to sidestep midnight-rollover flakiness.
    assert isinstance(pipeline._today(), datetime.date)
