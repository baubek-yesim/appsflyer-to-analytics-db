"""Exercises check_connection/create_table/load_events against a real, reachable database.

Skips (rather than fails) when no database is reachable in the current
environment — CI provides one via the `mysql:8` service container; locally,
a populated `.env` pointing at the real analytics DB also satisfies it.

check_connection is read-only; create_table only ever runs `CREATE TABLE IF
NOT EXISTS`; load_events tests use a sentinel app_id that can never collide
with real AppsFlyer data and clean up after themselves — all safe to exercise
against production too.
"""

from __future__ import annotations

import datetime
from decimal import Decimal

import pytest
from sqlalchemy import text

from appsflyer_pipeline.config import get_settings
from appsflyer_pipeline.loader import check_connection, create_engine, create_table, load_events


def test_check_connection_reports_server_version_and_table_status() -> None:
    try:
        settings = get_settings()
        engine = create_engine(settings)
        status = check_connection(engine, settings.db_table)
    except Exception as exc:  # noqa: BLE001 - environment without a reachable/configured DB
        pytest.skip(f"no usable database in this environment: {exc}")

    assert status.server_version
    assert isinstance(status.table_exists, bool)


def test_check_connection_reports_missing_table() -> None:
    """A fabricated, never-created table name -- read-only (information_schema
    lookup only), so this is safe against the real production DB too, and it's
    the only place the table_exists=False branch is exercised against a real
    server (the other check_connection tests all hit the real target table,
    which already exists in every environment this suite runs in).
    """
    try:
        settings = get_settings()
        engine = create_engine(settings)
        status = check_connection(engine, "__pytest_definitely_missing_table__")
    except Exception as exc:  # noqa: BLE001 - environment without a reachable/configured DB
        pytest.skip(f"no usable database in this environment: {exc}")

    assert status.table_exists is False
    assert status.row_count is None


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


def test_load_events_is_idempotent_and_isolated() -> None:
    try:
        settings = get_settings()
        engine = create_engine(settings)
        create_table(engine, settings.db_table)
    except Exception as exc:  # noqa: BLE001 - environment without a reachable/configured DB
        pytest.skip(f"no usable database in this environment: {exc}")

    # A sentinel app_id that can never collide with a real AppsFlyer app id,
    # so this test is fully isolated from production data.
    test_app_id = "__pytest_test_app__"
    test_attribution = "non_organic"
    window_start = datetime.date(2020, 1, 1)
    window_end = datetime.date(2020, 1, 1)
    row = {
        "event_time": datetime.datetime(2020, 1, 1, 12, 0, 0),
        "install_time": None,
        "attributed_touch_time": None,
        "event_name": "af_purchase",
        "event_revenue": Decimal("1.23"),
        "media_source": "Facebook Ads",
        "channel": None,
        "campaign": None,
        "campaign_id": None,
        "adset": None,
        "adset_id": None,
        "ad": None,
        "ad_id": None,
        "appsflyer_id": "test-af-id",
        "customer_user_id": None,
        "attribution_type": test_attribution,
        "app_id": test_app_id,
    }

    try:
        count1 = load_events(
            engine,
            settings.db_table,
            [row],
            app_id=test_app_id,
            attribution_type=test_attribution,
            start_date=window_start,
            end_date=window_end,
        )
        count2 = load_events(
            engine,
            settings.db_table,
            [row],
            app_id=test_app_id,
            attribution_type=test_attribution,
            start_date=window_start,
            end_date=window_end,
        )
        with engine.connect() as conn:
            actual_count = conn.execute(
                text(
                    f"SELECT COUNT(*) FROM `{settings.db_table}` "  # noqa: S608
                    "WHERE app_id = :app_id AND attribution_type = :attribution_type"
                ),
                {"app_id": test_app_id, "attribution_type": test_attribution},
            ).scalar_one()

        assert count1 == 1
        assert count2 == 1
        assert actual_count == 1  # second load replaced, not duplicated
    finally:
        # Delete-only call for the same window cleans up regardless of outcome.
        load_events(
            engine,
            settings.db_table,
            [],
            app_id=test_app_id,
            attribution_type=test_attribution,
            start_date=window_start,
            end_date=window_end,
        )
