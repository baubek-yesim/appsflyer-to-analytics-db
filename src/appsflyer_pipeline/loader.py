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
from appsflyer_pipeline.reports import ReportSpec

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
_IN_APP_EVENTS_CREATE_TABLE_TEMPLATE = """
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
    `app_id`                VARCHAR(100)   NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_app_attr_time` (`app_id`, `attribution_type`, `event_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

# installs/installs_retarget DDL (BAF-11 stage 5/6) -- 128 columns per the
# live column-sizing measurement (docs/superpowers/specs/2026-08-13-baf-11-column-sizing.md),
# mirrored in sql/create_table_installs.sql. Column order matches
# reports.INSTALLS_RAW_COLUMNS exactly (normalized). Empty-in-sample columns
# are typed by semantic category, not guessed per-column -- see this plan's
# Architecture decision 6 for the categorization rule and the byte-budget
# arithmetic (~28.4 KB of the 65,535-byte row limit).
# `install_time`/`event_time`/`appsflyer_id` are NOT NULL: guaranteed
# non-blank by ReportSpec.required_not_null before a row ever reaches here
# (transform.transform_events skips-and-counts a row missing any of them).
_INSTALLS_CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS `{table}` (
    `id`                              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `attributed_touch_type`           VARCHAR(32)    NULL,
    `attributed_touch_time`           DATETIME       NULL,
    `install_time`                    DATETIME       NOT NULL,
    `event_time`                      DATETIME       NOT NULL,
    `event_name`                      VARCHAR(32)    NULL,
    `event_value`                     TEXT           NULL,
    `event_revenue`                   DECIMAL(18,4)  NULL,
    `event_revenue_currency`          VARCHAR(16)    NULL,
    `event_revenue_usd`               DECIMAL(18,4)  NULL,
    `event_source`                    VARCHAR(16)    NULL,
    `is_receipt_validated`            VARCHAR(16)    NULL,
    `partner`                         VARCHAR(64)    NULL,
    `media_source`                    VARCHAR(64)    NULL,
    `channel`                         VARCHAR(32)    NULL,
    `keywords`                        VARCHAR(128)   NULL,
    `campaign`                        VARCHAR(128)   NULL,
    `campaign_id`                     VARCHAR(64)    NULL,
    `adset`                           VARCHAR(128)   NULL,
    `adset_id`                        VARCHAR(64)    NULL,
    `ad`                              VARCHAR(128)   NULL,
    `ad_id`                           VARCHAR(64)    NULL,
    `ad_type`                         VARCHAR(64)    NULL,
    `site_id`                         VARCHAR(128)   NULL,
    `sub_site_id`                     VARCHAR(64)    NULL,
    `sub_param_1`                     VARCHAR(16)    NULL,
    `sub_param_2`                     VARCHAR(64)    NULL,
    `sub_param_3`                     VARCHAR(64)    NULL,
    `sub_param_4`                     VARCHAR(255)   NULL,
    `sub_param_5`                     VARCHAR(128)   NULL,
    `cost_model`                      VARCHAR(32)    NULL,
    `cost_value`                      DECIMAL(18,4)  NULL,
    `cost_currency`                   VARCHAR(16)    NULL,
    `contributor_1_partner`           VARCHAR(64)    NULL,
    `contributor_1_media_source`      VARCHAR(64)    NULL,
    `contributor_1_campaign`          VARCHAR(128)   NULL,
    `contributor_1_touch_type`        VARCHAR(32)    NULL,
    `contributor_1_touch_time`        VARCHAR(64)    NULL,
    `contributor_2_partner`           VARCHAR(64)    NULL,
    `contributor_2_media_source`      VARCHAR(64)    NULL,
    `contributor_2_campaign`          VARCHAR(128)   NULL,
    `contributor_2_touch_type`        VARCHAR(32)    NULL,
    `contributor_2_touch_time`        VARCHAR(64)    NULL,
    `contributor_3_partner`           VARCHAR(64)    NULL,
    `contributor_3_media_source`      VARCHAR(64)    NULL,
    `contributor_3_campaign`          VARCHAR(128)   NULL,
    `contributor_3_touch_type`        VARCHAR(32)    NULL,
    `contributor_3_touch_time`        VARCHAR(64)    NULL,
    `region`                          VARCHAR(16)    NULL,
    `country_code`                    VARCHAR(16)    NULL,
    `state`                           VARCHAR(32)    NULL,
    `city`                            VARCHAR(64)    NULL,
    `postal_code`                     VARCHAR(32)    NULL,
    `dma`                             VARCHAR(16)    NULL,
    `ip`                              VARCHAR(32)    NULL,
    `wifi`                            VARCHAR(16)    NULL,
    `operator`                        VARCHAR(128)   NULL,
    `carrier`                         VARCHAR(128)   NULL,
    `language`                        VARCHAR(32)    NULL,
    `appsflyer_id`                    VARCHAR(128)   NOT NULL,
    `advertising_id`                  VARCHAR(128)   NULL,
    `idfa`                            VARCHAR(128)   NULL,
    `android_id`                      VARCHAR(128)   NULL,
    `customer_user_id`                VARCHAR(255)   NULL,
    `imei`                            VARCHAR(64)    NULL,
    `idfv`                            VARCHAR(128)   NULL,
    `platform`                        VARCHAR(16)    NULL,
    `device_type`                     VARCHAR(32)    NULL,
    `os_version`                      VARCHAR(16)    NULL,
    `app_version`                     VARCHAR(32)    NULL,
    `sdk_version`                     VARCHAR(16)    NULL,
    `appsflyer_app_id`                VARCHAR(32)    NULL,
    `app_name`                        VARCHAR(128)   NULL,
    `bundle_id`                       VARCHAR(64)    NULL,
    `is_retargeting`                  VARCHAR(16)    NULL,
    `retargeting_conversion_type`     VARCHAR(32)    NULL,
    `attribution_lookback`            VARCHAR(16)    NULL,
    `reengagement_window`             VARCHAR(16)    NULL,
    `is_primary_attribution`          VARCHAR(16)    NULL,
    `user_agent`                      TEXT           NULL,
    `http_referrer`                   TEXT           NULL,
    `original_url`                    TEXT           NULL,
    `store_reinstall`                 VARCHAR(16)    NULL,
    `impressions`                     VARCHAR(32)    NULL,
    `contributor_3_match_type`        VARCHAR(32)    NULL,
    `custom_dimension`                VARCHAR(64)    NULL,
    `conversion_type`                 VARCHAR(32)    NULL,
    `google_play_click_time`          VARCHAR(64)    NULL,
    `match_type`                      VARCHAR(32)    NULL,
    `mediation_network`               VARCHAR(64)    NULL,
    `oaid`                            VARCHAR(64)    NULL,
    `deeplink_url`                    TEXT           NULL,
    `blocked_reason`                  VARCHAR(64)    NULL,
    `blocked_sub_reason`              VARCHAR(64)    NULL,
    `google_play_broadcast_referrer`  TEXT           NULL,
    `google_play_install_begin_time`  VARCHAR(64)    NULL,
    `campaign_type`                   VARCHAR(32)    NULL,
    `custom_data`                     TEXT           NULL,
    `rejected_reason`                 VARCHAR(64)    NULL,
    `device_download_time`            VARCHAR(64)    NULL,
    `keyword_match_type`              VARCHAR(16)    NULL,
    `contributor_1_match_type`        VARCHAR(32)    NULL,
    `contributor_2_match_type`        VARCHAR(32)    NULL,
    `device_model`                    VARCHAR(128)   NULL,
    `monetization_network`            VARCHAR(64)    NULL,
    `segment`                         VARCHAR(64)    NULL,
    `is_lat`                          VARCHAR(16)    NULL,
    `google_play_referrer`            TEXT           NULL,
    `blocked_reason_value`            VARCHAR(32)    NULL,
    `store_product_page`              VARCHAR(32)    NULL,
    `device_category`                 VARCHAR(64)    NULL,
    `app_type`                        VARCHAR(32)    NULL,
    `rejected_reason_value`           VARCHAR(32)    NULL,
    `ad_unit`                         VARCHAR(64)    NULL,
    `keyword_id`                      VARCHAR(64)    NULL,
    `placement`                       VARCHAR(64)    NULL,
    `network_account_id`              VARCHAR(64)    NULL,
    `install_app_store`               VARCHAR(32)    NULL,
    `amazon_fire_id`                  VARCHAR(128)   NULL,
    `att`                             VARCHAR(16)    NULL,
    `engagement_type`                 VARCHAR(64)    NULL,
    `contributor_1_engagement_type`   VARCHAR(64)    NULL,
    `contributor_2_engagement_type`   VARCHAR(64)    NULL,
    `contributor_3_engagement_type`   VARCHAR(64)    NULL,
    `gdpr_applies`                    VARCHAR(16)    NULL,
    `ad_user_data_enabled`            VARCHAR(16)    NULL,
    `ad_personalization_enabled`      VARCHAR(16)    NULL,
    `total_candidates`                VARCHAR(16)    NULL,
    `engagement_destination`          VARCHAR(32)    NULL,
    `attribution_type`                VARCHAR(50)    NOT NULL,
    `app_id`                          VARCHAR(100)   NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_app_attr_install` (`app_id`, `attribution_type`, `install_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

# BAF-11 stage 4: selects the DDL template by ReportSpec.name (not by table
# name -- an operator-configured string tells us nothing about which columns
# a table should have). Every REPORTS entry's `.name` must be a key here;
# pinned by test_loader.py's test_create_table_template_registry_covers_every_report_name.
_CREATE_TABLE_TEMPLATE_BY_REPORT_NAME: dict[str, str] = {
    "in_app_events": _IN_APP_EVENTS_CREATE_TABLE_TEMPLATE,
    "installs": _INSTALLS_CREATE_TABLE_TEMPLATE,
}


def create_table(engine: Engine, table_name: str, report_name: str) -> None:
    """Create the target table if it doesn't already exist (idempotent).

    `report_name` selects the DDL shape (ReportSpec.name -- "in_app_events" or
    "installs" today) -- the two report families have different schemas.
    """
    table_name = _validate_identifier(table_name)
    try:
        ddl_template = _CREATE_TABLE_TEMPLATE_BY_REPORT_NAME[report_name]
    except KeyError as exc:
        raise PipelineError(f"No DDL template registered for report {report_name!r}") from exc
    ddl = ddl_template.format(table=table_name)
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
    except SQLAlchemyError as exc:
        raise PipelineError(f"Could not create table `{table_name}`: {exc}") from exc


def load_events(
    engine: Engine,
    spec: ReportSpec,
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    app_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
    allow_wipe: bool = False,
) -> int:
    """Idempotently load one (app_id, attribution_type, date-range) partition.

    Deletes any existing rows in the exact window this call owns, then bulk-
    inserts `rows`, all inside one transaction — safe to re-run for the same
    window (backfill chunk retries, daily re-runs) without duplicating data.

    BAF-11 stage 5 (issue #45): an EMPTY `rows` against a window that already
    holds data is refused with `PipelineError` unless `allow_wipe=True`.
    AppsFlyer answers a window past its availability floor -- and some
    upstream hiccups -- with a valid, header-only empty report, which
    delete-then-insert would otherwise turn into the silent erasure of the
    only copy of that window (our table) at exit 0. A genuinely quiet window
    (nothing loaded before, nothing fetched now) is still a no-op; the guard
    only bites when there is something to lose. `allow_wipe=True` is the
    deliberate escape hatch for operator cleanup and test teardown, and keeps
    issue #10's "wiped" WARNING.
    """
    table_name = _validate_identifier(table_name)
    window_column = _validate_identifier(spec.window_column)
    attribution_type = spec.attribution_type
    window_start = datetime.datetime.combine(start_date, datetime.time.min)
    window_end = datetime.datetime.combine(end_date + datetime.timedelta(days=1), datetime.time.min)

    delete_stmt = text(
        f"DELETE FROM `{table_name}` "
        "WHERE app_id = :app_id AND attribution_type = :attribution_type "
        f"AND `{window_column}` >= :window_start AND `{window_column}` < :window_end"
    )
    columns_sql = ", ".join(f"`{c}`" for c in spec.insert_columns)
    placeholders_sql = ", ".join(f":{c}" for c in spec.insert_columns)
    insert_stmt = text(f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({placeholders_sql})")

    count_stmt = text(
        f"SELECT COUNT(*) FROM `{table_name}` "
        "WHERE app_id = :app_id AND attribution_type = :attribution_type "
        f"AND `{window_column}` >= :window_start AND `{window_column}` < :window_end"
    )
    window_params = {
        "app_id": app_id,
        "attribution_type": attribution_type,
        "window_start": window_start,
        "window_end": window_end,
    }

    try:
        with engine.begin() as conn:
            if not rows and not allow_wipe:
                existing = int(conn.execute(count_stmt, window_params).scalar_one())
                if existing > 0:
                    logger.warning(
                        "refusing to wipe populated window: app_id=%s attribution_type=%s "
                        "window=[%s, %s] existing=%d fetched=0 -- an empty report for a window "
                        "that already holds rows is issue #45's shape (past AppsFlyer's "
                        "availability floor, or an upstream anomaly); rows left untouched",
                        app_id,
                        attribution_type,
                        start_date,
                        end_date,
                        existing,
                    )
                    raise PipelineError(
                        f"refusing to wipe populated window `{table_name}` for app_id={app_id!r} "
                        f"attribution_type={attribution_type!r} window=[{start_date}, {end_date}]: "
                        f"fetched 0 rows but {existing} already loaded (issue #45); re-run once "
                        "the source returns data, or pass allow_wipe=True to erase deliberately"
                    )
            deleted = conn.execute(delete_stmt, window_params).rowcount
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
