"""ReportSpec: one AppsFlyer report, as data (BAF-11 stages 3-4).

Structurally separates "which report" (endpoint, request params, column
mapping, dedupe key, retention policy, target table) from the rest of the
pipeline, so adding a report means registering a ReportSpec rather than
threading a new branch through
appsflyer_client.py/transform.py/loader.py/pipeline.py.

REPORTS holds FOUR entries since stage 4: in-app-events non_organic +
retargeting (BAF-2's original two, unchanged byte for byte) and installs
non_organic + retargeting. The two families differ in nearly every axis the
dataclass exposes -- installs sends no `event_name`, requests 47
`additional_fields`, maps its columns by normalization rather than a
hand-written dict (`column_map=None`), keys its dedup on
`(appsflyer_id, event_time)`, windows on `install_time`, and hard-clamps its
own start date to a 60-day availability floor. Since stage 5 in-app-events
hard-clamps too, to its own 31-day floor -- see IN_APP_EVENTS_AVAILABILITY_DAYS.

Registered is not the same as enabled: `Settings.appsflyer_enabled_reports`
(stage 4) decides which of these entries a backfill/daily run may actually
fetch, and defaults to the two in-app-events keys only -- installs is
available to run manually but stays out of the deployed scheduled timer until
the cutover decision. `cli.py`'s create-table/check-connection deliberately
ignore that gate; see the note at the top of that module.

See docs/superpowers/plans/2026-08-31-baf-11-stage-3-report-spec.md's
"Architecture decisions" section for which fields of the master spec's
proposed ReportSpec shape are still deliberately not wired up
(max_chunk_days, decimal_columns, partition_columns) and why -- `dedupe_key`
was on that list until stage 4 added it.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from appsflyer_pipeline.appsflyer_client import AttributionType
from appsflyer_pipeline.config import Settings
from appsflyer_pipeline.transform import DEDUPE_DISCRIMINATOR_ROW_KEY, normalize_column_name


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
    column_map: Mapping[str, str] | None
    timestamp_columns: tuple[str, ...]
    required_not_null: tuple[str, ...]
    # A callable, not a settings attribute name string: getattr(settings, name)
    # would type-check as Any under mypy strict and silently swallow a typo.
    table: Callable[[Settings], str]
    insert_columns: tuple[str, ...]
    window_column: str
    # BAF-11 stage 4 (installs): a callable, not a column-name tuple, for the
    # same reason `table` is a callable (Stage 3 decision) -- a typo in a key
    # column name is a mypy/runtime error at the definition site, not a
    # silently-wrong tuple discovered downstream.
    dedupe_key: Callable[[dict[str, Any]], tuple[Any, ...]]
    # True for every registered spec since BAF-11 stage 5 (installs since
    # stage 4; in-app-events kept warn-and-proceed until the full raw export
    # made our table the only copy of anything older than its 31-day window).
    # Kept as a field rather than removed so a future spec can opt out
    # deliberately, and so _iter_work_items' clamp stays data-driven.
    hard_clamp_retention: bool


# AppsFlyer's documented raw-data *availability* windows (support.appsflyer.com,
# "Data availability windows", read 2026-09-07) -- distinct from the 90-day
# HTTP 400 boundary appsflyer_client.MAX_RETENTION_DAYS describes:
#   in-app events:  "31 out of the last 90 days"
#   attributions (installs, retargeting conversions): "60 out of the last 90 days"
# A request for dates between the availability window and the 90-day boundary
# comes back HTTP 200 with a valid header and zero rows -- indistinguishable
# from a genuinely quiet window (issue #45, live-observed as a ~35-day floor
# on 2026-07-09). Every spec hard-clamps its start date to its own window
# (BAF-11 stage 5): past it, our table is the only copy of the data, and the
# idempotent delete-then-insert would replace it with that valid empty.
IN_APP_EVENTS_AVAILABILITY_DAYS = 31
INSTALLS_AVAILABILITY_DAYS = 60


def _in_app_events_table(settings: Settings) -> str:
    return settings.db_table


def _in_app_events_dedupe_key(row: dict[str, Any]) -> tuple[Any, ...]:
    """Reproduces _dedupe_rows' pre-Stage-4 hardcoded key exactly -- BAF-11
    stage 2's (event_time, event_name, appsflyer_id, Event Value) key, byte
    for byte. See this plan's Architecture decision 7 for why installs gets
    its own key instead of reusing this one.
    """
    return (
        row["event_time"],
        row["event_name"],
        row["appsflyer_id"],
        row[DEDUPE_DISCRIMINATOR_ROW_KEY],
    )


def _installs_table(settings: Settings) -> str:
    return settings.db_table_installs


def _installs_dedupe_key(row: dict[str, Any]) -> tuple[Any, ...]:
    """installs' own key (BAF-11 stage 4) -- (appsflyer_id, event_time), NOT
    in-app-events' key. See this plan's Architecture decision 7 for the full
    rationale (Event Value is always empty for installs; Event Name barely
    varies; Install Time is constant per device across installs-retarget's
    re-engagement rows, so it can't discriminate the way it does as
    in-app-events' tiebreak; Event Time is the field that actually varies per
    distinct re-engagement touch). Flagged there as an open question for
    Mark/data-analytics sign-off, not a settled policy.
    """
    return (row["appsflyer_id"], row["event_time"])


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

# The 81 fields AppsFlyer always returns for installs_report/installs-retarget
# regardless of additional_fields -- confirmed identical, same order, to
# in_app_events_report's own 81 default columns (column-sizing spec, both
# sections). Kept as a separate tuple (not reused from in-app-events) because
# the two report families' column sets are conceptually independent even
# though today's measurement happens to make them equal.
_INSTALLS_DEFAULT_FIELDS: tuple[str, ...] = (
    "Attributed Touch Type",
    "Attributed Touch Time",
    "Install Time",
    "Event Time",
    "Event Name",
    "Event Value",
    "Event Revenue",
    "Event Revenue Currency",
    "Event Revenue USD",
    "Event Source",
    "Is Receipt Validated",
    "Partner",
    "Media Source",
    "Channel",
    "Keywords",
    "Campaign",
    "Campaign ID",
    "Adset",
    "Adset ID",
    "Ad",
    "Ad ID",
    "Ad Type",
    "Site ID",
    "Sub Site ID",
    "Sub Param 1",
    "Sub Param 2",
    "Sub Param 3",
    "Sub Param 4",
    "Sub Param 5",
    "Cost Model",
    "Cost Value",
    "Cost Currency",
    "Contributor 1 Partner",
    "Contributor 1 Media Source",
    "Contributor 1 Campaign",
    "Contributor 1 Touch Type",
    "Contributor 1 Touch Time",
    "Contributor 2 Partner",
    "Contributor 2 Media Source",
    "Contributor 2 Campaign",
    "Contributor 2 Touch Type",
    "Contributor 2 Touch Time",
    "Contributor 3 Partner",
    "Contributor 3 Media Source",
    "Contributor 3 Campaign",
    "Contributor 3 Touch Type",
    "Contributor 3 Touch Time",
    "Region",
    "Country Code",
    "State",
    "City",
    "Postal Code",
    "DMA",
    "IP",
    "WIFI",
    "Operator",
    "Carrier",
    "Language",
    "AppsFlyer ID",
    "Advertising ID",
    "IDFA",
    "Android ID",
    "Customer User ID",
    "IMEI",
    "IDFV",
    "Platform",
    "Device Type",
    "OS Version",
    "App Version",
    "SDK Version",
    "App ID",
    "App Name",
    "Bundle ID",
    "Is Retargeting",
    "Retargeting Conversion Type",
    "Attribution Lookback",
    "Reengagement Window",
    "Is Primary Attribution",
    "User Agent",
    "HTTP Referrer",
    "Original URL",
)

# The 47 fields requested via the API's additional_fields param (confirmed
# live 2026-08-13 -- no 400, all 47 land). Public: reused as-is for
# ReportSpec.additional_fields below, and importable by tests that need to
# build a realistic 128-column fixture CSV without a second hand-typed list.
INSTALLS_ADDITIONAL_FIELDS: tuple[str, ...] = (
    "Store Reinstall",
    "Impressions",
    "Contributor 3 Match Type",
    "Custom Dimension",
    "Conversion Type",
    "Google Play Click Time",
    "Match Type",
    "Mediation Network",
    "OAID",
    "Deeplink URL",
    "Blocked Reason",
    "Blocked Sub Reason",
    "Google Play Broadcast Referrer",
    "Google Play Install Begin Time",
    "Campaign Type",
    "Custom Data",
    "Rejected Reason",
    "Device Download Time",
    "Keyword Match Type",
    "Contributor 1 Match Type",
    "Contributor 2 Match Type",
    "Device Model",
    "Monetization Network",
    "Segment",
    "Is LAT",
    "Google Play Referrer",
    "Blocked Reason Value",
    "Store Product Page",
    "Device Category",
    "App Type",
    "Rejected Reason Value",
    "Ad Unit",
    "Keyword ID",
    "Placement",
    "Network Account ID",
    "Install App Store",
    "Amazon Fire ID",
    "ATT",
    "Engagement Type",
    "Contributor 1 Engagement Type",
    "Contributor 2 Engagement Type",
    "Contributor 3 Engagement Type",
    "GDPR Applies",
    "Ad User Data Enabled",
    "Ad Personalization Enabled",
    "Total Candidates",
    "Engagement Destination",
)

# Public: default + additional, in AppsFlyer's own column order -- the single
# source of truth for "every raw column installs' full pass-through mode
# expects." Reused to derive insert_columns below and by
# sql/create_table_installs.sql / loader.py's DDL template (Task 3), which
# MUST list its columns in this exact order.
INSTALLS_RAW_COLUMNS: tuple[str, ...] = (*_INSTALLS_DEFAULT_FIELDS, *INSTALLS_ADDITIONAL_FIELDS)

_INSTALLS_INSERT_COLUMNS: tuple[str, ...] = tuple(
    normalize_column_name(raw) for raw in INSTALLS_RAW_COLUMNS
) + ("attribution_type", "app_id")

REPORTS: dict[str, ReportSpec] = {
    "in_app_events_non_organic": ReportSpec(
        name="in_app_events",
        endpoint="in_app_events_report",
        attribution_type="non_organic",
        sends_event_name=True,
        sends_media_source=True,
        additional_fields=(),
        retention_days=IN_APP_EVENTS_AVAILABILITY_DAYS,
        column_map=_IN_APP_EVENTS_COLUMN_MAP,
        timestamp_columns=_IN_APP_EVENTS_TIMESTAMP_COLUMNS,
        required_not_null=_IN_APP_EVENTS_REQUIRED_NOT_NULL,
        table=_in_app_events_table,
        insert_columns=_IN_APP_EVENTS_INSERT_COLUMNS,
        window_column="event_time",
        dedupe_key=_in_app_events_dedupe_key,
        hard_clamp_retention=True,
    ),
    "in_app_events_retargeting": ReportSpec(
        name="in_app_events",
        endpoint="in-app-events-retarget",
        attribution_type="retargeting",
        sends_event_name=True,
        sends_media_source=True,
        additional_fields=(),
        retention_days=IN_APP_EVENTS_AVAILABILITY_DAYS,
        column_map=_IN_APP_EVENTS_COLUMN_MAP,
        timestamp_columns=_IN_APP_EVENTS_TIMESTAMP_COLUMNS,
        required_not_null=_IN_APP_EVENTS_REQUIRED_NOT_NULL,
        table=_in_app_events_table,
        insert_columns=_IN_APP_EVENTS_INSERT_COLUMNS,
        window_column="event_time",
        dedupe_key=_in_app_events_dedupe_key,
        hard_clamp_retention=True,
    ),
    "installs_non_organic": ReportSpec(
        name="installs",
        endpoint="installs_report",
        attribution_type="non_organic",
        sends_event_name=False,
        sends_media_source=True,
        additional_fields=INSTALLS_ADDITIONAL_FIELDS,
        retention_days=INSTALLS_AVAILABILITY_DAYS,
        column_map=None,
        timestamp_columns=("event_time", "install_time", "attributed_touch_time"),
        required_not_null=("appsflyer_id", "install_time", "event_time"),
        table=_installs_table,
        insert_columns=_INSTALLS_INSERT_COLUMNS,
        window_column="install_time",
        dedupe_key=_installs_dedupe_key,
        hard_clamp_retention=True,
    ),
    "installs_retargeting": ReportSpec(
        name="installs",
        endpoint="installs-retarget",
        attribution_type="retargeting",
        sends_event_name=False,
        sends_media_source=True,
        additional_fields=INSTALLS_ADDITIONAL_FIELDS,
        retention_days=INSTALLS_AVAILABILITY_DAYS,
        column_map=None,
        timestamp_columns=("event_time", "install_time", "attributed_touch_time"),
        required_not_null=("appsflyer_id", "install_time", "event_time"),
        table=_installs_table,
        insert_columns=_INSTALLS_INSERT_COLUMNS,
        window_column="install_time",
        dedupe_key=_installs_dedupe_key,
        hard_clamp_retention=True,
    ),
}
