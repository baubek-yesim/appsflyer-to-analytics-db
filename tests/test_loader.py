from __future__ import annotations

import datetime

import pytest
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
from appsflyer_pipeline.reports import REPORTS


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
