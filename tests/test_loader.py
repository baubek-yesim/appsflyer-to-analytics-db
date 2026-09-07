from __future__ import annotations

import datetime
import logging
import sqlite3
from typing import Any

import pytest
from sqlalchemy import create_engine as _sa_create_engine
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from appsflyer_pipeline import loader
from appsflyer_pipeline.config import Settings
from appsflyer_pipeline.loader import (
    _CREATE_TABLE_TEMPLATE_BY_REPORT_NAME,
    PipelineError,
    _validate_identifier,
    create_engine,
    create_table,
    load_events,
)
from appsflyer_pipeline.reports import REPORTS, ReportSpec


@pytest.mark.parametrize("name", ["appsflyer_events_fb", "Table1", "a_b_c123"])
def test_validate_identifier_accepts_safe_names(name: str) -> None:
    assert _validate_identifier(name) == name


@pytest.mark.parametrize("name", ["bad name", "table;drop table x", "table`x", "a-b", ""])
def test_validate_identifier_rejects_unsafe_names(name: str) -> None:
    with pytest.raises(PipelineError):
        _validate_identifier(name)


def _unreachable_engine() -> Engine:
    """A real Engine pointed at a host nothing listens on — connection refused
    is immediate (not a timeout), so calls through it fail fast and deterministically.
    """
    settings = Settings(
        db_host="127.0.0.1",
        db_port=59999,
        db_user="user",
        db_password="pw",
        db_name="db",
        db_table="some_table",
        db_table_installs="some_installs_table",
        appsflyer_api_token="token",
        appsflyer_app_ids=["id1"],
        _env_file=None,
    )  # type: ignore[call-arg]
    return create_engine(settings)


def test_create_table_wraps_sqlalchemy_error() -> None:
    engine = _unreachable_engine()
    with pytest.raises(PipelineError, match="Could not create table") as excinfo:
        create_table(engine, "some_table", "in_app_events")
    assert isinstance(excinfo.value.__cause__, SQLAlchemyError)


def test_load_events_wraps_sqlalchemy_error() -> None:
    engine = _unreachable_engine()
    with pytest.raises(PipelineError, match="Could not load events") as excinfo:
        load_events(
            engine,
            REPORTS["in_app_events_non_organic"],
            "some_table",
            [],
            app_id="app1",
            start_date=datetime.date(2020, 1, 1),
            end_date=datetime.date(2020, 1, 1),
        )
    assert isinstance(excinfo.value.__cause__, SQLAlchemyError)


def test_create_table_installs_ddl_covers_every_insert_column() -> None:
    """Regression test for the master spec's Этап 6 acceptance criterion:
    the installs DDL must cover exactly the 130-column set (128 mapped +
    attribution_type + app_id) transform_events() will actually produce.
    """
    spec = REPORTS["installs_non_organic"]
    ddl = _CREATE_TABLE_TEMPLATE_BY_REPORT_NAME["installs"].format(table="t")
    for column in spec.insert_columns:
        assert f"`{column}`" in ddl, f"DDL is missing column {column!r}"
    assert "PRIMARY KEY (`id`)" in ddl
    assert "idx_app_attr_install" in ddl
    assert "install_time" in ddl


def test_create_table_template_registry_covers_every_report_name() -> None:
    for spec in REPORTS.values():
        assert spec.name in _CREATE_TABLE_TEMPLATE_BY_REPORT_NAME


def test_load_events_deletes_on_install_time_for_installs_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression test for the master spec's Этап 6 test list: installs'
    DELETE predicate must key on install_time (spec.window_column), not
    event_time -- the two report families disagree on which timestamp bounds
    their window.
    """
    captured_sql: list[str] = []
    real_text = loader.text  # type: ignore[attr-defined]  # not explicitly re-exported by loader

    def _capturing_text(sql_string: str) -> object:
        captured_sql.append(sql_string)
        return real_text(sql_string)

    monkeypatch.setattr(loader, "text", _capturing_text)
    engine = _unreachable_engine()
    spec = REPORTS["installs_non_organic"]

    with pytest.raises(PipelineError):
        load_events(
            engine,
            spec,
            "installs_table",
            [],
            app_id="app1",
            start_date=datetime.date(2026, 1, 1),
            end_date=datetime.date(2026, 1, 1),
        )

    delete_sql = captured_sql[0]  # text() is called for DELETE before INSERT
    assert "`install_time`" in delete_sql
    assert "`event_time`" not in delete_sql


# --- BAF-11 stage 5: "an empty fetch never wipes a populated window" guard ---
#
# Exercised against an in-memory SQLite table shaped like the spec's insert
# columns: load_events' SQL is plain DELETE/INSERT/SELECT COUNT with backtick
# identifiers, which SQLite accepts verbatim, so the guard's real
# transaction-level behavior is testable here without a MySQL server.
# tests/test_loader_integration.py re-proves the same contract against the
# real MariaDB/MySQL in CI.


def _sqlite_engine(spec: ReportSpec) -> Engine:
    # Python 3.12 deprecates sqlite3's implicit datetime adapter; register the
    # documented replacement so the window-bound params bind without a warning.
    sqlite3.register_adapter(datetime.datetime, lambda d: d.isoformat(" "))
    engine = _sa_create_engine("sqlite://")
    columns_sql = ", ".join(f"`{c}` TEXT" for c in spec.insert_columns)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `t` ({columns_sql})"))
    return engine


def _in_app_events_row(appsflyer_id: str) -> dict[str, Any]:
    spec = REPORTS["in_app_events_non_organic"]
    row: dict[str, Any] = dict.fromkeys(spec.insert_columns)
    row.update(
        {
            "event_time": "2020-01-01 12:00:00",
            "event_name": "af_purchase",
            "appsflyer_id": appsflyer_id,
            "attribution_type": "non_organic",
            "app_id": "app1",
        }
    )
    return row


def _count(engine: Engine) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text("SELECT COUNT(*) FROM `t`")).scalar_one())


_WINDOW: dict[str, Any] = {
    "app_id": "app1",
    "start_date": datetime.date(2020, 1, 1),
    "end_date": datetime.date(2020, 1, 1),
}


def test_load_events_refuses_to_wipe_populated_window_by_default(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Issue #45: AppsFlyer answers a window past its availability floor (or
    an upstream hiccup) with a valid, header-only EMPTY report, and the
    idempotent delete-then-insert would then erase the only copy of that
    window -- ours. Refuse loudly instead of wiping: the caller sees a
    PipelineError (a FAILED window, exit 1), the rows stay put.
    """
    spec = REPORTS["in_app_events_non_organic"]
    engine = _sqlite_engine(spec)
    load_events(engine, spec, "t", [_in_app_events_row("af-1")], **_WINDOW)

    with (
        caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.loader"),
        pytest.raises(PipelineError, match="refusing to wipe"),
    ):
        load_events(engine, spec, "t", [], **_WINDOW)

    assert _count(engine) == 1  # nothing deleted
    assert any("existing=1" in r.message for r in caplog.records)


def test_load_events_allow_wipe_deletes_populated_window_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The deliberate escape hatch (operator cleanup, test teardown): with
    allow_wipe=True the pre-stage-5 behavior applies -- delete, insert
    nothing, and issue #10's WARNING so the wipe is loud in journalctl.
    """
    spec = REPORTS["in_app_events_non_organic"]
    engine = _sqlite_engine(spec)
    load_events(engine, spec, "t", [_in_app_events_row("af-1")], **_WINDOW)

    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.loader"):
        loaded = load_events(engine, spec, "t", [], allow_wipe=True, **_WINDOW)

    assert loaded == 0
    assert _count(engine) == 0
    assert any("wiped previously loaded window" in r.message for r in caplog.records)


def test_load_events_empty_rows_into_empty_window_is_a_quiet_noop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A genuinely quiet window (e.g. com.yesimmobile x retargeting, empty at
    the source for its whole history) must keep loading as a silent no-op:
    the guard only bites when there is something to lose.
    """
    spec = REPORTS["in_app_events_non_organic"]
    engine = _sqlite_engine(spec)

    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.loader"):
        loaded = load_events(engine, spec, "t", [], **_WINDOW)

    assert loaded == 0
    assert _count(engine) == 0
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)


def test_load_events_replaces_populated_window_with_new_rows() -> None:
    """The guard must not get in the way of the normal idempotent path: a
    non-empty fetch still owns the window outright (delete + insert)."""
    spec = REPORTS["in_app_events_non_organic"]
    engine = _sqlite_engine(spec)
    load_events(engine, spec, "t", [_in_app_events_row("af-1")], **_WINDOW)

    loaded = load_events(
        engine, spec, "t", [_in_app_events_row("af-2"), _in_app_events_row("af-3")], **_WINDOW
    )

    assert loaded == 2
    assert _count(engine) == 2
