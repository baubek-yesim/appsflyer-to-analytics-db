"""Database engine factory, connectivity checks, DDL, and idempotent loading
for the analytics MariaDB.
"""

from __future__ import annotations

import datetime
import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote_plus

from sqlalchemy import create_engine as _create_engine
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from appsflyer_pipeline.config import Settings

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


class PipelineError(RuntimeError):
    """Raised for actionable, user-facing pipeline failures."""


def _validate_identifier(name: str) -> str:
    """Guard against building SQL with an unsafe table/column identifier.

    SQLAlchemy can't parameterize identifiers (only values), so any name that
    reaches raw SQL is checked against an allowlist pattern first.
    """
    if not _IDENTIFIER_RE.match(name):
        raise PipelineError(f"Unsafe or invalid SQL identifier: {name!r}")
    return name


def create_engine(settings: Settings) -> Engine:
    """Build a pooled, timeout-guarded engine — mirrors standard SQLAlchemy+PyMySQL practice."""
    url = (
        f"mysql+pymysql://{quote_plus(settings.db_user)}:{quote_plus(settings.db_password)}"
        f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
    )
    return _create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={
            "connect_timeout": 10,
            "read_timeout": 60,
            "write_timeout": 60,
        },
        future=True,
    )


@dataclass(frozen=True)
class ConnectionStatus:
    server_version: str
    table_exists: bool
    row_count: int | None


def check_connection(engine: Engine, table_name: str) -> ConnectionStatus:
    """Verify DB connectivity and report the target table's existence/row count."""
    table_name = _validate_identifier(table_name)
    try:
        with engine.connect() as conn:
            version = conn.execute(text("SELECT VERSION()")).scalar_one()
            table_exists = (
                conn.execute(
                    text(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = DATABASE() AND table_name = :table"
                    ),
                    {"table": table_name},
                ).scalar_one()
                > 0
            )
            row_count = None
            if table_exists:
                count_query = text(f"SELECT COUNT(*) FROM `{table_name}`")
                row_count = conn.execute(count_query).scalar_one()
            return ConnectionStatus(
                server_version=version, table_exists=table_exists, row_count=row_count
            )
    except SQLAlchemyError as exc:
        raise PipelineError(f"Could not connect to the database: {exc}") from exc


# Schema per Mark Malovichko's DDL (BAF-2 comment 62293); mirrored in sql/create_table.sql
# for reference/manual execution — keep the two in sync if the schema ever changes.
# Time columns are DATETIME, not TIMESTAMP (schema owner's decision, applied to production
# 2026-07-10): stores the literal wall-clock value with no session-timezone conversion.
# `id`/PRIMARY KEY/idx_app_attr_time added 2026-07-08 (issue #14) — see
# sql/migrations/2026-07-08-add-id-pk-and-index.sql for the one-time migration an
# already-provisioned table needs (this template only affects fresh CREATE TABLE calls).
# `is_primary_attribution` added 2026-07-10 (issue #55) — see
# sql/migrations/2026-07-10-add-is-primary-attribution.sql for the one-time migration an
# already-provisioned table needs.
_CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS `{table}` (
    `id`                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `event_time`            DATETIME       NOT NULL,
    `install_time`          DATETIME       NULL,
    `attributed_touch_time` DATETIME       NULL,
    `event_name`            VARCHAR(100)   NOT NULL,
    `event_revenue`         DECIMAL(18,4)  NULL,
    `media_source`          VARCHAR(100)   NULL,
    `channel`               VARCHAR(255)   NULL,
    `campaign`              VARCHAR(255)   NULL,
    `campaign_id`           VARCHAR(255)   NULL,
    `adset`                 VARCHAR(255)   NULL,
    `adset_id`              VARCHAR(255)   NULL,
    `ad`                    VARCHAR(255)   NULL,
    `ad_id`                 VARCHAR(255)   NULL,
    `appsflyer_id`          VARCHAR(100)   NOT NULL,
    `customer_user_id`      VARCHAR(255)   NULL,
    `attribution_type`      VARCHAR(50)    NOT NULL,
    `is_primary_attribution` TINYINT(1)    NOT NULL,
    `app_id`                VARCHAR(100)   NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_app_attr_time` (`app_id`, `attribution_type`, `event_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def create_table(engine: Engine, table_name: str) -> None:
    """Create the target table if it doesn't already exist (idempotent)."""
    table_name = _validate_identifier(table_name)
    ddl = _CREATE_TABLE_TEMPLATE.format(table=table_name)
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
    except SQLAlchemyError as exc:
        raise PipelineError(f"Could not create table `{table_name}`: {exc}") from exc


# Column order for INSERT — must match the keys transform.transform_events() produces.
_INSERT_COLUMNS = (
    "event_time",
    "install_time",
    "attributed_touch_time",
    "event_name",
    "event_revenue",
    "media_source",
    "channel",
    "campaign",
    "campaign_id",
    "adset",
    "adset_id",
    "ad",
    "ad_id",
    "appsflyer_id",
    "customer_user_id",
    "attribution_type",
    "is_primary_attribution",
    "app_id",
)


_VIEW_COLUMNS_SQL = ", ".join(f"`{c}`" for c in _INSERT_COLUMNS)

# Issue #55: a de-duplicated read over the base table. Collapses a dual-attributed
# purchase (same event_time/event_name/appsflyer_id in both the UA and retargeting
# reports) to its primary row, while passing single-attribution rows through
# untouched. Window functions require MariaDB >=10.2 / MySQL 8 (both satisfied).
_CREATE_VIEW_TEMPLATE = f"""
CREATE OR REPLACE VIEW `{{view}}` AS
SELECT {_VIEW_COLUMNS_SQL}
FROM (
    SELECT {_VIEW_COLUMNS_SQL},
           ROW_NUMBER() OVER (
               PARTITION BY `event_time`, `event_name`, `appsflyer_id`
               ORDER BY `is_primary_attribution` DESC, `attribution_type` ASC
           ) AS _dedup_rn
    FROM `{{table}}`
) ranked
WHERE _dedup_rn = 1
"""


def create_view(engine: Engine, table_name: str) -> None:
    """Create/replace the `<table_name>_deduped` view (idempotent)."""
    table_name = _validate_identifier(table_name)
    view_name = _validate_identifier(f"{table_name}_deduped")
    ddl = _CREATE_VIEW_TEMPLATE.format(view=view_name, table=table_name)
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
    except SQLAlchemyError as exc:
        raise PipelineError(f"Could not create view `{view_name}`: {exc}") from exc


def load_events(
    engine: Engine,
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    app_id: str,
    attribution_type: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> int:
    """Idempotently load one (app_id, attribution_type, date-range) partition.

    Deletes any existing rows in the exact window this call owns, then bulk-
    inserts `rows`, all inside one transaction — safe to re-run for the same
    window (backfill chunk retries, daily re-runs) without duplicating data.
    """
    table_name = _validate_identifier(table_name)
    window_start = datetime.datetime.combine(start_date, datetime.time.min)
    window_end = datetime.datetime.combine(end_date + datetime.timedelta(days=1), datetime.time.min)

    delete_stmt = text(
        f"DELETE FROM `{table_name}` "
        "WHERE app_id = :app_id AND attribution_type = :attribution_type "
        "AND event_time >= :window_start AND event_time < :window_end"
    )
    columns_sql = ", ".join(f"`{c}`" for c in _INSERT_COLUMNS)
    placeholders_sql = ", ".join(f":{c}" for c in _INSERT_COLUMNS)
    insert_stmt = text(f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({placeholders_sql})")

    try:
        with engine.begin() as conn:
            deleted = conn.execute(
                delete_stmt,
                {
                    "app_id": app_id,
                    "attribution_type": attribution_type,
                    "window_start": window_start,
                    "window_end": window_end,
                },
            ).rowcount
            if rows:
                conn.execute(insert_stmt, rows)
    except SQLAlchemyError as exc:
        raise PipelineError(
            f"Could not load events into `{table_name}` for app_id={app_id!r} "
            f"attribution_type={attribution_type!r} window=[{start_date}, {end_date}]: {exc}"
        ) from exc

    logger.info(
        "loaded app_id=%s attribution_type=%s window=[%s, %s]: deleted=%d inserted=%d",
        app_id,
        attribution_type,
        start_date,
        end_date,
        deleted,
        len(rows),
    )
    if deleted > 0 and not rows:
        # Issue #10: delete-then-insert makes a successful-but-empty fetch erase an
        # already-loaded window with zero trace. Legitimate only if AppsFlyer really
        # revised the window to zero events — so it must be loud in journalctl.
        logger.warning(
            "wiped previously loaded window: app_id=%s attribution_type=%s "
            "window=[%s, %s] deleted=%d inserted=0",
            app_id,
            attribution_type,
            start_date,
            end_date,
            deleted,
        )
    return len(rows)
