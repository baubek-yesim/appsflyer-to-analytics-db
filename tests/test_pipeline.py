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
from appsflyer_pipeline.config import Settings, get_settings
from appsflyer_pipeline.loader import ConnectionStatus, PipelineError
from appsflyer_pipeline.pipeline import _iter_work_items, run_backfill, run_daily
from appsflyer_pipeline.reports import INSTALLS_RAW_COLUMNS, REPORTS, ReportSpec
from appsflyer_pipeline.transform import transform_events

SAMPLE_CSV = (
    "Attributed Touch Time,Install Time,Event Time,Event Name,Event Value,Event Revenue,"
    "Media Source,Channel,Campaign,Campaign ID,Adset,Adset ID,Ad,Ad ID,"
    "AppsFlyer ID,Customer User ID,Is Primary Attribution\n"
    "2026-05-20 10:00:00,2026-05-19 09:00:00,2026-05-20 10:05:00,af_purchase,af-value-1,9.99,"
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
    "DB_TABLE_INSTALLS": "appsflyer_installs_events",
    "APPSFLYER_API_TOKEN": "token",
    "APPSFLYER_APP_IDS": "app1,app2",
}
APP_IDS = ("app1", "app2")
ATTRIBUTION_TYPES = ("non_organic", "retargeting")
# BAF-11 stage 4: REPORTS holds four specs, but only these two are eligible to
# run unless APPSFLYER_ENABLED_REPORTS says otherwise -- installs must not be
# reachable by the deployed scheduled timer before the Этап 9 cutover.
DEFAULT_ENABLED_REPORTS = ("in_app_events_non_organic", "in_app_events_retargeting")
ALL_REPORTS_ENABLED = ",".join(REPORTS)


# Optional keys with no default (BAF-11 stage 1 made the two filters optional):
# an ambient value from the developer's shell or a CI `env:` block would change
# which rows a run pulls, so clear them unless the test names them.
_OPTIONAL_ENV_KEYS = (
    "APPSFLYER_MEDIA_SOURCE",
    "APPSFLYER_EVENT_NAMES",
    "APPSFLYER_TIMEZONE",
    "APPSFLYER_DAILY_LOOKBACK_DAYS",
    "APPSFLYER_CHUNK_DAYS",
    "APPSFLYER_EVENT_TIME_FROM",
    "APPSFLYER_EVENT_TIME_TO",
    "APPSFLYER_ENABLED_REPORTS",
)


def _set_env(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    for key in _OPTIONAL_ENV_KEYS:
        if key not in overrides:
            monkeypatch.delenv(key, raising=False)
    for key, value in {**BASE_ENV, **overrides}.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()


def _url(app_id: str, attribution_type: str) -> str:
    endpoint = (
        "in_app_events_report" if attribution_type == "non_organic" else "in-app-events-retarget"
    )
    return f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/{endpoint}/v5"


def _url_for_spec(spec: ReportSpec, app_id: str) -> str:
    return f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/{spec.endpoint}/v5"


def _installs_sample_csv() -> str:
    values = dict.fromkeys(INSTALLS_RAW_COLUMNS, "")
    values.update(
        {
            "AppsFlyer ID": "af-installs-1",
            "Install Time": "2026-05-19 09:30:00",
            "Event Time": "2026-05-19 09:30:00",
            "Attributed Touch Time": "2026-05-19 09:00:00",
            "Event Name": "install",
            "Media Source": "Facebook Ads",
        }
    )
    header = ",".join(INSTALLS_RAW_COLUMNS)
    row = ",".join(values[c] for c in INSTALLS_RAW_COLUMNS)
    return f"{header}\n{row}\n"


def _mock_all_ok() -> None:
    installs_csv = _installs_sample_csv()
    for app_id in APP_IDS:
        for spec in REPORTS.values():
            csv_text = SAMPLE_CSV if spec.name == "in_app_events" else installs_csv
            respx.get(_url_for_spec(spec, app_id)).mock(
                return_value=httpx.Response(200, text=csv_text)
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
        spec: ReportSpec,
        table_name: str,
        rows: list[dict[str, Any]],
        *,
        app_id: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> int:
        calls.append(
            {
                "app_id": app_id,
                "attribution_type": spec.attribution_type,
                "start_date": start_date,
                "end_date": end_date,
                "rows": rows,
            }
        )
        return len(rows)

    monkeypatch.setattr(pipeline, "load_events", _fake_load_events)
    return calls


def test_iter_work_items_yields_expected_matrix(monkeypatch: pytest.MonkeyPatch) -> None:
    """BAF-11 stage 4 (Architecture decision 5): REPORTS grew from 2 to 4
    specs (in-app-events non_organic/retargeting + installs non_organic/
    retargeting), so `one_series` must also pin `spec.name` -- otherwise it
    matches both in-app-events' and installs' non_organic specs and the
    per-series chunk count silently doubles. The window is deliberately
    pinned recent (within installs' 60-day hard-clamp floor) so installs'
    clamp is a no-op here and the cross-REPORTS `len(items)` equality holds.
    Every spec is opted in explicitly (APPSFLYER_ENABLED_REPORTS): the default
    enables only the two in-app-events specs, so without this the matrix this
    test is about would never include installs at all.
    """
    _set_env(monkeypatch, APPSFLYER_ENABLED_REPORTS=ALL_REPORTS_ENABLED)
    settings = get_settings()
    fixed_today = datetime.date(2026, 6, 1)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    start = datetime.date(2026, 4, 10)  # within installs' 60-day floor (2026-04-02)
    end = datetime.date(2026, 5, 31)  # 51 days -> 2 chunks of <=31 days each
    items = list(_iter_work_items(settings, start, end))

    assert {app_id for _, app_id, _, _ in items} == set(APP_IDS)
    assert {spec.name for spec, _, _, _ in items} == {"in_app_events", "installs"}
    assert {spec.attribution_type for spec, _, _, _ in items} == set(ATTRIBUTION_TYPES)

    one_series = [
        (s, e)
        for spec, a, s, e in items
        if a == "app1" and spec.name == "in_app_events" and spec.attribution_type == "non_organic"
    ]
    assert one_series[0][0] == start
    assert one_series[-1][1] == end
    assert all((e - s).days < 31 for s, e in one_series)
    assert len(items) == len(APP_IDS) * len(REPORTS) * len(one_series)


def test_iter_work_items_respects_configured_chunk_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """See test_iter_work_items_yields_expected_matrix's docstring -- same
    `spec.name` narrowing, same REPORTS-growth ripple (Architecture decision
    5)."""
    _set_env(monkeypatch, APPSFLYER_CHUNK_DAYS="10")
    settings = get_settings()
    fixed_today = datetime.date(2026, 2, 15)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    start = datetime.date(2026, 1, 1)
    end = datetime.date(2026, 1, 31)  # 31 days -> 4 chunks of <=10 days each

    items = list(_iter_work_items(settings, start, end))

    one_series = [
        (s, e)
        for spec, a, s, e in items
        if a == "app1" and spec.name == "in_app_events" and spec.attribution_type == "non_organic"
    ]
    assert all((e - s).days < 10 for s, e in one_series)
    assert len(one_series) == 4


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
def test_run_daily_omits_filter_params_when_filters_unset(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """BAF-11 stage 1 end-to-end: unset filters must reach the wire as ABSENT
    params, not as empty ones — the config -> client path is where a default
    could silently sneak back in.
    """
    _set_env(monkeypatch)
    _mock_all_ok()

    summary = run_daily(date=datetime.date(2026, 5, 20))

    assert summary.all_succeeded
    for call in respx.calls:
        assert "media_source" not in call.request.url.params
        assert "event_name" not in call.request.url.params


@respx.mock
def test_run_daily_logs_the_effective_filter_mode(
    monkeypatch: pytest.MonkeyPatch,
    load_spy: list[dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Which rows a run is even eligible to load is now a config decision, so the
    run has to say out loud which mode it is in — once per run, before any
    request. Unfiltered is the wide-blast-radius mode and is logged at WARNING:
    until stage 5 routes it to its own table, an unnoticed unfiltered run would
    pour every media source into BAF-2's `appsflyer_events_fb`.
    """
    _set_env(monkeypatch)
    _mock_all_ok()

    with caplog.at_level(logging.INFO, logger="appsflyer_pipeline.pipeline"):
        run_daily(date=datetime.date(2026, 5, 20))

    unfiltered = [r for r in caplog.records if "media_source=<all>" in r.message]
    assert len(unfiltered) == 1
    assert unfiltered[0].levelno == logging.WARNING
    assert "event_name=<all>" in unfiltered[0].message


@respx.mock
def test_run_daily_logs_named_filters_at_info(
    monkeypatch: pytest.MonkeyPatch,
    load_spy: list[dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_env(
        monkeypatch,
        APPSFLYER_MEDIA_SOURCE="Facebook Ads",
        APPSFLYER_EVENT_NAMES="af_purchase,af_purchase_YC",
    )
    _mock_all_ok()

    with caplog.at_level(logging.INFO, logger="appsflyer_pipeline.pipeline"):
        run_daily(date=datetime.date(2026, 5, 20))

    mode = [r for r in caplog.records if "filter mode:" in r.message]
    assert len(mode) == 1
    assert mode[0].levelno == logging.INFO
    assert "media_source='Facebook Ads'" in mode[0].message
    assert "event_name=af_purchase,af_purchase_YC" in mode[0].message


@respx.mock
def test_run_daily_passes_configured_timezone_to_api(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """Issue #53: APPSFLYER_TIMEZONE reaches every report download's query string."""
    _set_env(monkeypatch, APPSFLYER_TIMEZONE="Europe/Riga")
    _mock_all_ok()

    summary = run_daily(date=datetime.date(2026, 5, 20))

    assert summary.all_succeeded
    assert len(respx.calls) == len(APP_IDS) * len(ATTRIBUTION_TYPES)
    for call in respx.calls:
        assert call.request.url.params["timezone"] == "Europe/Riga"


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


@respx.mock
def test_run_daily_isolates_error_text_200_body(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """Issue #26 end-to-end: an HTTP 200 whose body is a one-line error string
    (parses to a 0-row, 1-column frame) fails exactly its own window via
    TransformError -- it must never reach load_events, where rows=[] would
    delete the window's existing data and report success.
    """
    _set_env(monkeypatch)
    respx.get(_url("app1", "non_organic")).mock(
        return_value=httpx.Response(200, text="Subscription package limitation. Contact your CSM")
    )
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
    assert "TransformError" in failed.error
    assert "missing expected column" in failed.error
    assert len(summary.succeeded) == 3
    assert len(load_spy) == 3  # the failed unit never reaches load_events


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
    with pytest.raises(PipelineError, match="after"):
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


def test_run_daily_lookback_widens_default_window(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    _set_env(monkeypatch, APPSFLYER_DAILY_LOOKBACK_DAYS="3")
    fixed_today = datetime.date(2026, 7, 7)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    expected_end = fixed_today - datetime.timedelta(days=1)
    expected_start = expected_end - datetime.timedelta(days=2)

    with respx.mock:
        _mock_all_ok()
        summary = run_daily(dry_run=True)

    assert all(
        r.start_date == expected_start and r.end_date == expected_end for r in summary.results
    )
    # 3 days <= 31 -> still exactly one chunk (one report download) per combo: no extra quota.
    # BAF-11 stage 4: REPORTS grew from 2 to 4 specs (Architecture decision 5),
    # but only the two DEFAULT_ENABLED_REPORTS run without an explicit
    # APPSFLYER_ENABLED_REPORTS opt-in, so a default run is still 2 specs.
    assert len(summary.results) == len(APP_IDS) * len(DEFAULT_ENABLED_REPORTS)


def test_run_daily_explicit_date_ignores_lookback(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """--date is a targeted repair tool: exactly [date, date], lookback or not."""
    _set_env(monkeypatch, APPSFLYER_DAILY_LOOKBACK_DAYS="3")
    target = datetime.date(2026, 5, 20)

    with respx.mock:
        _mock_all_ok()
        summary = run_daily(date=target, dry_run=True)

    assert all(r.start_date == target and r.end_date == target for r in summary.results)


def test_run_backfill_warns_for_fully_beyond_floor_window_with_explicit_past_end(
    monkeypatch: pytest.MonkeyPatch,
    load_spy: list[dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Issue #28: the floor used to be anchored to --end-date (default_start =
    end - 89d), so a request whose WHOLE window lay beyond retention -- e.g. a
    March repair run in July -- warned nothing. The floor must anchor to today.
    """
    _set_env(monkeypatch)
    monkeypatch.setattr(pipeline, "_today", lambda: datetime.date(2026, 7, 9))
    # floor = 2026-04-10; the whole window below is ~6 weeks beyond it

    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.pipeline"), respx.mock:
        _mock_all_ok()
        summary = run_backfill(
            start=datetime.date(2026, 3, 1), end=datetime.date(2026, 3, 31), dry_run=True
        )

    assert any("retention floor" in record.message for record in caplog.records)
    assert min(r.start_date for r in summary.results) == datetime.date(2026, 3, 1)  # not clamped


def test_run_daily_explicit_old_date_warns(
    monkeypatch: pytest.MonkeyPatch,
    load_spy: list[dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Issue #28: daily --date had no floor check at all."""
    _set_env(monkeypatch)
    monkeypatch.setattr(pipeline, "_today", lambda: datetime.date(2026, 7, 9))

    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.pipeline"), respx.mock:
        _mock_all_ok()
        summary = run_daily(date=datetime.date(2026, 2, 1), dry_run=True)

    assert any("retention floor" in record.message for record in caplog.records)
    assert all(r.start_date == datetime.date(2026, 2, 1) for r in summary.results)


def test_run_daily_recent_date_does_not_warn(
    monkeypatch: pytest.MonkeyPatch,
    load_spy: list[dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_env(monkeypatch)
    monkeypatch.setattr(pipeline, "_today", lambda: datetime.date(2026, 7, 9))

    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.pipeline"), respx.mock:
        _mock_all_ok()
        run_daily(date=datetime.date(2026, 7, 5), dry_run=True)

    assert not any("retention floor" in record.message for record in caplog.records)


def test_run_daily_date_exactly_at_floor_does_not_warn(
    monkeypatch: pytest.MonkeyPatch,
    load_spy: list[dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The floor predicate is strict `<`: a date exactly AT the floor (and by
    extension the default 90-day backfill window, whose start equals it) must
    stay silent -- a `<=` regression would spam journald on every scheduled run.

    This date (2026-04-10) is also before installs' own 60-day floor
    (2026-05-10), so `_iter_work_items` would log its own "skipping installs"
    warning for it whenever installs is enabled (BAF-11 stage 4, Architecture
    decision 3) -- an unavoidable consequence of the in-app-events 90-day
    boundary always being earlier than installs' 60-day one, not a regression.
    (It doesn't fire in this particular run, which uses the default
    in-app-events-only APPSFLYER_ENABLED_REPORTS.) The assertion stays narrowed
    to `_warn_if_before_retention_floor`'s own message text ("Proceeding
    anyway", unique to it) so it remains scoped to the in-app-events boundary
    behavior it's actually pinning, regardless of which reports are enabled.
    """
    _set_env(monkeypatch)
    monkeypatch.setattr(pipeline, "_today", lambda: datetime.date(2026, 7, 9))
    # retention_floor = 2026-07-09 - 90d = 2026-04-10

    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.pipeline"), respx.mock:
        _mock_all_ok()
        run_daily(date=datetime.date(2026, 4, 10), dry_run=True)

    assert not any("Proceeding anyway" in record.message for record in caplog.records)


def test_run_daily_config_event_time_window_beats_lookback(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """Issue #50: APPSFLYER_EVENT_TIME_FROM/TO drive the flagless daily window
    directly (the API's from/to filter on event time server-side), winning
    over the lookback setting."""
    _set_env(
        monkeypatch,
        APPSFLYER_EVENT_TIME_FROM="2026-06-20",
        APPSFLYER_EVENT_TIME_TO="2026-06-25",
        APPSFLYER_DAILY_LOOKBACK_DAYS="3",
    )

    with respx.mock:
        _mock_all_ok()
        summary = run_daily(dry_run=True)

    assert summary.all_succeeded
    assert all(
        r.start_date == datetime.date(2026, 6, 20) and r.end_date == datetime.date(2026, 6, 25)
        for r in summary.results
    )


def test_run_daily_event_time_to_defaults_to_yesterday(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    _set_env(monkeypatch, APPSFLYER_EVENT_TIME_FROM="2026-07-05")
    monkeypatch.setattr(pipeline, "_today", lambda: datetime.date(2026, 7, 9))

    with respx.mock:
        _mock_all_ok()
        summary = run_daily(dry_run=True)

    assert all(
        r.start_date == datetime.date(2026, 7, 5) and r.end_date == datetime.date(2026, 7, 8)
        for r in summary.results
    )


def test_run_daily_explicit_date_beats_event_time_window(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    _set_env(monkeypatch, APPSFLYER_EVENT_TIME_FROM="2026-06-20")
    target = datetime.date(2026, 7, 1)

    with respx.mock:
        _mock_all_ok()
        summary = run_daily(date=target, dry_run=True)

    assert all(r.start_date == target and r.end_date == target for r in summary.results)


def test_run_daily_event_time_from_after_dynamic_end_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FROM beyond the dynamic yesterday-end is a runtime PipelineError — with
    TO unset it cannot be caught at config-load time."""
    _set_env(monkeypatch, APPSFLYER_EVENT_TIME_FROM="2026-07-15")
    monkeypatch.setattr(pipeline, "_today", lambda: datetime.date(2026, 7, 9))

    with pytest.raises(PipelineError, match="after the window end"):
        run_daily(dry_run=True)


def test_run_daily_old_event_time_from_warns(
    monkeypatch: pytest.MonkeyPatch,
    load_spy: list[dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_env(
        monkeypatch,
        APPSFLYER_EVENT_TIME_FROM="2026-02-01",
        APPSFLYER_EVENT_TIME_TO="2026-02-05",
    )
    monkeypatch.setattr(pipeline, "_today", lambda: datetime.date(2026, 7, 9))

    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.pipeline"), respx.mock:
        _mock_all_ok()
        run_daily(dry_run=True)

    assert any("retention floor" in record.message for record in caplog.records)


def test_iter_work_items_hard_clamps_installs_but_not_in_app_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Architecture decision 3: installs (hard_clamp_retention=True, 60 days)
    gets its start date clamped up to the retention floor; in-app-events
    (hard_clamp_retention=False, 90 days) keeps warn-and-proceed -- its
    chunks still start at the caller's requested start regardless.
    """
    _set_env(monkeypatch, APPSFLYER_ENABLED_REPORTS=ALL_REPORTS_ENABLED)
    settings = get_settings()
    fixed_today = datetime.date(2026, 8, 31)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    start = fixed_today - datetime.timedelta(days=100)  # 100 days back
    end = fixed_today - datetime.timedelta(days=1)

    items = list(_iter_work_items(settings, start, end))

    in_app_events_starts = {
        s for spec, a, s, e in items if a == "app1" and spec.name == "in_app_events"
    }
    installs_starts = {s for spec, a, s, e in items if a == "app1" and spec.name == "installs"}

    assert min(in_app_events_starts) == start  # unclamped -- 100 days back, unchanged
    assert min(installs_starts) == fixed_today - datetime.timedelta(days=60)  # hard-clamped


def test_iter_work_items_skips_installs_entirely_when_window_is_fully_before_the_floor(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _set_env(monkeypatch, APPSFLYER_ENABLED_REPORTS=ALL_REPORTS_ENABLED)
    settings = get_settings()
    fixed_today = datetime.date(2026, 8, 31)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    start = fixed_today - datetime.timedelta(days=200)
    end = fixed_today - datetime.timedelta(days=150)  # entirely before the 60-day floor

    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.pipeline"):
        items = list(_iter_work_items(settings, start, end))

    installs_items = [i for i in items if i[0].name == "installs"]
    in_app_events_items = [i for i in items if i[0].name == "in_app_events"]
    assert installs_items == []  # nothing to fetch -- entirely before the hard floor
    assert len(in_app_events_items) > 0  # in-app-events is unaffected (90-day retention)
    assert any("entirely before" in r.message for r in caplog.records)


def test_active_retention_days_narrows_to_installs_60_once_registered() -> None:
    """No code change needed for this -- Stage 3's min(...)-based
    _active_retention_days() already does the right thing once REPORTS grows
    to include a 60-day spec. This test just locks in that it actually does.
    """
    from appsflyer_pipeline.pipeline import _active_retention_days

    assert _active_retention_days() == 60


def test_run_backfill_default_window_uses_max_retention_days_not_the_cross_report_minimum(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """Pins run_backfill's no-args default_start computation directly, by
    capturing the start/end it actually passes to _iter_work_items --
    independent of WindowResult, which carries no per-report-family identity
    (see the note above). Proves Architecture decision 3's addendum: the
    default resolves off MAX_RETENTION_DAYS (90), not
    _active_retention_days()'s cross-REPORTS minimum, which drops to 60 the
    moment installs (retention_days=60) is registered alongside in-app-events.
    """
    _set_env(monkeypatch)
    fixed_today = datetime.date(2026, 7, 7)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    expected_end = fixed_today - datetime.timedelta(days=1)
    expected_start = expected_end - datetime.timedelta(days=MAX_RETENTION_DAYS - 1)

    captured: dict[str, datetime.date] = {}
    real_iter_work_items = pipeline._iter_work_items

    def _capturing_iter_work_items(
        settings: Settings, start: datetime.date, end: datetime.date
    ) -> Any:
        captured["start"] = start
        captured["end"] = end
        return real_iter_work_items(settings, start, end)

    monkeypatch.setattr(pipeline, "_iter_work_items", _capturing_iter_work_items)

    with respx.mock:
        _mock_all_ok()
        run_backfill(dry_run=True)

    assert captured["start"] == expected_start
    assert captured["end"] == expected_end


def test_iter_work_items_skips_installs_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """The opt-in gate: with no APPSFLYER_ENABLED_REPORTS set, only the two
    in-app-events specs are eligible. This is what keeps installs out of the
    live scheduled `daily` timer until the Этап 9 cutover decision -- the
    plan's Non-Goal ("this plan only makes installs *available* to run
    manually; nothing here changes what the deployed systemd timer pulls").
    The window is pinned recent so installs' own 60-day hard clamp cannot be
    what excludes it -- the gate has to be.
    """
    _set_env(monkeypatch)
    settings = get_settings()
    fixed_today = datetime.date(2026, 8, 31)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    start = fixed_today - datetime.timedelta(days=3)
    end = fixed_today - datetime.timedelta(days=1)

    items = list(_iter_work_items(settings, start, end))

    assert {spec.name for spec, _, _, _ in items} == {"in_app_events"}
    assert len(items) == len(APP_IDS) * len(DEFAULT_ENABLED_REPORTS)


def test_iter_work_items_runs_only_the_reports_named_in_enabled_reports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit opt-in narrows to exactly the named keys -- installs only,
    with in-app-events excluded, is the shape of a manual installs-only run.
    """
    _set_env(monkeypatch, APPSFLYER_ENABLED_REPORTS="installs_non_organic")
    settings = get_settings()
    fixed_today = datetime.date(2026, 8, 31)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    day = fixed_today - datetime.timedelta(days=1)

    items = list(_iter_work_items(settings, day, day))

    assert {spec.name for spec, _, _, _ in items} == {"installs"}
    assert {spec.attribution_type for spec, _, _, _ in items} == {"non_organic"}
    assert len(items) == len(APP_IDS)


def test_run_window_rejects_an_unknown_enabled_report_key(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """A typo must fail the run loudly, not silently no-op into fetching
    nothing -- the same fail-loud treatment the missing-table preflight gets.
    The check runs before the preflight and before any network call, so no
    respx mocks are needed here: reaching one would itself be the bug.
    """
    _set_env(monkeypatch, APPSFLYER_ENABLED_REPORTS="in_app_events_non_organic,instals_retargeting")

    with pytest.raises(PipelineError, match="instals_retargeting"):
        run_daily(date=datetime.date(2026, 5, 20), dry_run=True)

    assert load_spy == []


def test_run_window_preflights_only_the_enabled_reports_tables(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """A fresh deploy on the default (in-app-events-only) config must not abort
    the whole run because the installs table hasn't been provisioned yet --
    that would defeat the point of the gate. Only enabled specs' tables are
    checked.
    """
    _set_env(monkeypatch)
    checked: list[str] = []

    def _fake_check_connection(engine: object, table_name: str) -> ConnectionStatus:
        checked.append(table_name)
        return ConnectionStatus(
            server_version="test",
            table_exists=table_name != BASE_ENV["DB_TABLE_INSTALLS"],
            row_count=0,
        )

    monkeypatch.setattr(pipeline, "check_connection", _fake_check_connection)

    with respx.mock:
        _mock_all_ok()
        summary = run_daily(date=datetime.date(2026, 5, 20))

    assert checked == [BASE_ENV["DB_TABLE"]]  # installs' table never consulted
    assert summary.all_succeeded


def test_run_window_preflight_still_covers_an_enabled_installs_table(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """The narrowed preflight must not become a no-op for installs: once an
    operator opts installs in, its missing table has to abort the run exactly
    like in-app-events' does.
    """
    _set_env(monkeypatch, APPSFLYER_ENABLED_REPORTS=ALL_REPORTS_ENABLED)
    monkeypatch.setattr(
        pipeline,
        "check_connection",
        lambda engine, table_name: ConnectionStatus(
            server_version="test",
            table_exists=table_name != BASE_ENV["DB_TABLE_INSTALLS"],
            row_count=0,
        ),
    )

    with pytest.raises(PipelineError, match=BASE_ENV["DB_TABLE_INSTALLS"]):
        run_daily(date=datetime.date(2026, 5, 20))


def test_installs_transform_is_not_re_filtered_by_the_event_name_filter(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """`_process_window` must gate the client-side re-filters on the spec, the
    same way `appsflyer_client._fetch_csv` already gates the API-side params
    (`if spec.sends_event_name and event_names is not None`).

    installs has `sends_event_name=False`, so `event_name` is never sent to the
    API and the response legitimately carries Event Name values the filter
    doesn't name ("install"). Passing that filter into `transform_events`
    anyway re-filters those rows away client-side -- every installs row
    silently dropped, and the idempotent delete-then-insert then WIPES the
    window at exit 0. That is the exact issue-#10/#45 failure shape, reached
    through a filter that was never meant to apply to this report.
    """
    _set_env(
        monkeypatch,
        APPSFLYER_ENABLED_REPORTS="installs_non_organic",
        APPSFLYER_MEDIA_SOURCE="Facebook Ads",
        APPSFLYER_EVENT_NAMES="af_purchase,af_purchase_YC",
    )
    monkeypatch.setattr(pipeline, "_today", lambda: datetime.date(2026, 6, 1))
    filters: list[tuple[str | None, list[str] | None]] = []

    def _spying_transform_events(
        df: Any,
        *,
        spec: ReportSpec,
        app_id: str,
        media_source_filter: str | None,
        event_names_filter: list[str] | None,
    ) -> list[dict[str, Any]]:
        filters.append((media_source_filter, event_names_filter))
        return transform_events(
            df,
            spec=spec,
            app_id=app_id,
            media_source_filter=media_source_filter,
            event_names_filter=event_names_filter,
        )

    monkeypatch.setattr(pipeline, "transform_events", _spying_transform_events)

    with respx.mock:
        _mock_all_ok()
        summary = run_daily(date=datetime.date(2026, 5, 19))

    assert summary.all_succeeded
    # sends_event_name=False -> the filter is dropped; sends_media_source=True
    # -> it still applies (and the fixture row matches it).
    assert filters == [("Facebook Ads", None)] * len(APP_IDS)
    assert summary.total_loaded == len(APP_IDS)  # 1 installs row per app, NOT filtered away
    assert all(len(call["rows"]) == 1 for call in load_spy)


def test_installs_retention_floor_is_60_not_90(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch, APPSFLYER_ENABLED_REPORTS=ALL_REPORTS_ENABLED)
    settings = get_settings()
    fixed_today = datetime.date(2026, 8, 31)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    start = fixed_today - datetime.timedelta(days=80)  # between 60 and 90 days back
    end = fixed_today - datetime.timedelta(days=1)

    items = list(_iter_work_items(settings, start, end))

    installs_starts = {s for spec, a, s, e in items if a == "app1" and spec.name == "installs"}
    in_app_events_starts = {
        s for spec, a, s, e in items if a == "app1" and spec.name == "in_app_events"
    }
    assert min(installs_starts) == fixed_today - datetime.timedelta(days=60)
    assert min(in_app_events_starts) == start  # 80 days back, well within in-app-events' 90
