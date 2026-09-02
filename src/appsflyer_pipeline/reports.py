"""ReportSpec: one AppsFlyer report, as data (BAF-11 stage 3).

Pure refactor -- REPORTS holds only the two report definitions that already
exist (in-app-events non_organic + retargeting); nothing about what any
existing command requests, transforms, or writes changes. Structurally
separates "which report" (endpoint, request params, column mapping, target
table) from the rest of the pipeline so a second report (installs, BAF-11
stage 5/6) can be added by registering a new ReportSpec instead of threading
a new branch through appsflyer_client.py/transform.py/loader.py/pipeline.py.

See docs/superpowers/plans/2026-08-31-baf-11-stage-3-report-spec.md's
"Architecture decisions" section for which fields of the master spec's
proposed ReportSpec shape this stage deliberately does NOT wire up yet
(max_chunk_days, decimal_columns, dedupe_key, partition_columns) and why.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from appsflyer_pipeline.appsflyer_client import MAX_RETENTION_DAYS, AttributionType
from appsflyer_pipeline.config import Settings


@dataclass(frozen=True)
class ReportSpec:
    """One AppsFlyer report as data: request shape, column mapping, target table."""

    name: str
    endpoint: str
    attribution_type: AttributionType
    sends_event_name: bool
    sends_media_source: bool
    additional_fields: tuple[str, ...]
    retention_days: int
    column_map: Mapping[str, str]
    timestamp_columns: tuple[str, ...]
    required_not_null: tuple[str, ...]
    # A callable, not a settings attribute name string: getattr(settings, name)
    # would type-check as Any under mypy strict and silently swallow a typo.
    table: Callable[[Settings], str]
    insert_columns: tuple[str, ...]
    window_column: str


def _in_app_events_table(settings: Settings) -> str:
    return settings.db_table


# Raw AppsFlyer column -> target table column. Moved here from transform.py
# (BAF-11 stage 3): ReportSpec, not the transform module, now owns "which
# columns this report maps" so a future all-fields report (installs,
# column_map treated as "map everything") can differ per spec instead of per
# module constant. Values confirmed byte-identical to the pre-refactor
# transform._COLUMN_MAP by this task's Step 3.
_IN_APP_EVENTS_COLUMN_MAP: dict[str, str] = {
    "Event Time": "event_time",
    "Install Time": "install_time",
    "Attributed Touch Time": "attributed_touch_time",
    "Event Name": "event_name",
    "Event Revenue": "event_revenue",
    "Media Source": "media_source",
    "Channel": "channel",
    "Campaign": "campaign",
    "Campaign ID": "campaign_id",
    "Adset": "adset",
    "Adset ID": "adset_id",
    "Ad": "ad",
    "Ad ID": "ad_id",
    "AppsFlyer ID": "appsflyer_id",
    "Customer User ID": "customer_user_id",
}
_IN_APP_EVENTS_TIMESTAMP_COLUMNS = ("event_time", "install_time", "attributed_touch_time")
_IN_APP_EVENTS_REQUIRED_NOT_NULL = ("event_time", "event_name", "appsflyer_id")

# Column order for INSERT -- moved here from loader._INSERT_COLUMNS (BAF-11
# stage 3). Must match the keys transform.transform_events() produces for
# this spec; pinned by test_reports.py's
# test_insert_columns_match_transform_events_output_keys_for_every_report.
_IN_APP_EVENTS_INSERT_COLUMNS = (
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
    "app_id",
)

REPORTS: dict[str, ReportSpec] = {
    "in_app_events_non_organic": ReportSpec(
        name="in_app_events",
        endpoint="in_app_events_report",
        attribution_type="non_organic",
        sends_event_name=True,
        sends_media_source=True,
        additional_fields=(),
        retention_days=MAX_RETENTION_DAYS,
        column_map=_IN_APP_EVENTS_COLUMN_MAP,
        timestamp_columns=_IN_APP_EVENTS_TIMESTAMP_COLUMNS,
        required_not_null=_IN_APP_EVENTS_REQUIRED_NOT_NULL,
        table=_in_app_events_table,
        insert_columns=_IN_APP_EVENTS_INSERT_COLUMNS,
        window_column="event_time",
    ),
    "in_app_events_retargeting": ReportSpec(
        name="in_app_events",
        endpoint="in-app-events-retarget",
        attribution_type="retargeting",
        sends_event_name=True,
        sends_media_source=True,
        additional_fields=(),
        retention_days=MAX_RETENTION_DAYS,
        column_map=_IN_APP_EVENTS_COLUMN_MAP,
        timestamp_columns=_IN_APP_EVENTS_TIMESTAMP_COLUMNS,
        required_not_null=_IN_APP_EVENTS_REQUIRED_NOT_NULL,
        table=_in_app_events_table,
        insert_columns=_IN_APP_EVENTS_INSERT_COLUMNS,
        window_column="event_time",
    ),
}
