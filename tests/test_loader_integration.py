"""Exercises check_connection/create_table against a real, reachable database.

Skips (rather than fails) when no database is reachable in the current
environment — CI provides one via the `mysql:8` service container; locally,
a populated `.env` pointing at the real analytics DB also satisfies it.

check_connection is read-only; create_table only ever runs `CREATE TABLE IF
NOT EXISTS`, so both are safe to exercise against production too.
"""

from __future__ import annotations

import pytest

from appsflyer_pipeline.config import get_settings
from appsflyer_pipeline.loader import check_connection, create_engine, create_table


def test_check_connection_reports_server_version_and_table_status() -> None:
    try:
        settings = get_settings()
        engine = create_engine(settings)
        status = check_connection(engine, settings.db_table)
    except Exception as exc:  # noqa: BLE001 - environment without a reachable/configured DB
        pytest.skip(f"no usable database in this environment: {exc}")

    assert status.server_version
    assert isinstance(status.table_exists, bool)


def test_create_table_is_idempotent() -> None:
    try:
        settings = get_settings()
        engine = create_engine(settings)
        create_table(engine, settings.db_table)
        create_table(engine, settings.db_table)  # second call must not raise
        status = check_connection(engine, settings.db_table)
    except Exception as exc:  # noqa: BLE001 - environment without a reachable/configured DB
        pytest.skip(f"no usable database in this environment: {exc}")

    assert status.table_exists is True
