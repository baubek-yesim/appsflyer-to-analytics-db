from __future__ import annotations

import datetime

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from appsflyer_pipeline.config import Settings
from appsflyer_pipeline.loader import (
    PipelineError,
    _validate_identifier,
    create_engine,
    create_table,
    create_view,
    load_events,
)


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
        appsflyer_api_token="token",
        appsflyer_app_ids=["id1"],
        _env_file=None,
    )  # type: ignore[call-arg]
    return create_engine(settings)


def test_create_table_wraps_sqlalchemy_error() -> None:
    engine = _unreachable_engine()
    with pytest.raises(PipelineError, match="Could not create table") as excinfo:
        create_table(engine, "some_table")
    assert isinstance(excinfo.value.__cause__, SQLAlchemyError)


def test_create_view_wraps_sqlalchemy_error() -> None:
    engine = _unreachable_engine()
    with pytest.raises(PipelineError, match="Could not create view") as excinfo:
        create_view(engine, "some_table")
    assert isinstance(excinfo.value.__cause__, SQLAlchemyError)


def test_load_events_wraps_sqlalchemy_error() -> None:
    engine = _unreachable_engine()
    with pytest.raises(PipelineError, match="Could not load events") as excinfo:
        load_events(
            engine,
            "some_table",
            [],
            app_id="app1",
            attribution_type="non_organic",
            start_date=datetime.date(2020, 1, 1),
            end_date=datetime.date(2020, 1, 1),
        )
    assert isinstance(excinfo.value.__cause__, SQLAlchemyError)
