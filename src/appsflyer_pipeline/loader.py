"""Database engine factory, connectivity checks, and DDL for the analytics MariaDB.

Idempotent load logic (delete-by-window-then-insert) lands in Stage 4.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import quote_plus

from sqlalchemy import create_engine as _create_engine
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from appsflyer_pipeline.config import Settings

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
                count_query = text(f"SELECT COUNT(*) FROM `{table_name}`")  # noqa: S608
                row_count = conn.execute(count_query).scalar_one()
            return ConnectionStatus(
                server_version=version, table_exists=table_exists, row_count=row_count
            )
    except SQLAlchemyError as exc:
        raise PipelineError(f"Could not connect to the database: {exc}") from exc


# Schema per Mark Malovichko's DDL (BAF-2 comment 62293); mirrored in sql/create_table.sql
# for reference/manual execution — keep the two in sync if the schema ever changes.
_CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS `{table}` (
    `event_time`            TIMESTAMP      NOT NULL,
    `install_time`          TIMESTAMP      NULL,
    `attributed_touch_time` TIMESTAMP      NULL,
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
    `app_id`                VARCHAR(100)   NOT NULL
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
