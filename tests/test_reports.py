"""Tests for the ReportSpec registry (BAF-11 stage 3).

Stage 3 is a pure refactor -- REPORTS holds only the two report definitions
that already exist. These tests pin the registry's shape and the one new
invariant the refactor introduces: insert_columns must always equal exactly
the keys transform_events() actually produces for that spec.
"""

from __future__ import annotations

import polars as pl

from appsflyer_pipeline.reports import INSTALLS_ADDITIONAL_FIELDS, INSTALLS_RAW_COLUMNS, REPORTS
from appsflyer_pipeline.transform import transform_events


def test_registry_covers_in_app_events_non_organic_and_retargeting() -> None:
    # BAF-11 stage 4: narrowed from `set(REPORTS) == {...}` to a subset check --
    # REPORTS now also holds the two installs specs (see
    # test_registry_covers_installs_alongside_in_app_events for the full-set
    # assertion). Only the iteration/count assumption changes here; every
    # per-spec expected value below is untouched.
    assert {"in_app_events_non_organic", "in_app_events_retargeting"} <= set(REPORTS)


def test_non_organic_spec_matches_the_existing_endpoint_and_attribution_type() -> None:
    spec = REPORTS["in_app_events_non_organic"]
    assert spec.name == "in_app_events"
    assert spec.endpoint == "in_app_events_report"
    assert spec.attribution_type == "non_organic"


def test_retargeting_spec_matches_the_existing_endpoint_and_attribution_type() -> None:
    spec = REPORTS["in_app_events_retargeting"]
    assert spec.name == "in_app_events"
    assert spec.endpoint == "in-app-events-retarget"
    assert spec.attribution_type == "retargeting"


def test_both_specs_send_event_name_and_media_source_and_no_additional_fields() -> None:
    # BAF-11 stage 4: narrowed from `REPORTS.values()` to the two in-app-events
    # keys -- installs' specs legitimately have sends_event_name=False and 47
    # additional_fields (see test_both_installs_specs_never_send_event_name_...
    # below), so iterating the whole registry no longer matches what this test
    # means. Expected values for in-app-events are unchanged.
    for key in ("in_app_events_non_organic", "in_app_events_retargeting"):
        spec = REPORTS[key]
        assert spec.sends_event_name is True
        assert spec.sends_media_source is True
        assert spec.additional_fields == ()


def test_both_specs_share_the_in_app_events_table_and_retention() -> None:
    # BAF-11 stage 5: 31, not 90 -- AppsFlyer's documented raw-data availability
    # window for in-app events is "31 out of the last 90 days"
    # (support.appsflyer.com "Data availability windows"); 90 is only the HTTP
    # 400 boundary. installs has its own 60-day window
    # (test_both_installs_specs_have_a_hard_clamped_60_day_retention).
    for key in ("in_app_events_non_organic", "in_app_events_retargeting"):
        spec = REPORTS[key]
        assert spec.retention_days == 31
        assert spec.window_column == "event_time"


def test_table_callable_reads_settings_db_table() -> None:
    # BAF-11 stage 4: narrowed from `REPORTS.values()` -- installs' table
    # callable reads settings.db_table_installs, not settings.db_table (see
    # test_installs_table_callable_reads_settings_db_table_installs below).
    class _FakeSettings:
        db_table = "appsflyer_events_fb"

    for key in ("in_app_events_non_organic", "in_app_events_retargeting"):
        assert REPORTS[key].table(_FakeSettings()) == "appsflyer_events_fb"  # type: ignore[arg-type]


def _raw_row_for(column_map: dict[str, str]) -> dict[str, str]:
    """One raw AppsFlyer row with every column `column_map` expects, plus the
    dedupe discriminator -- values are placeholders except the fields
    transform_events requires non-empty or parses as a timestamp/decimal,
    which need a real, well-formed value.
    """
    row: dict[str, str] = {raw: f"value-{target}" for raw, target in column_map.items()}
    row["Event Time"] = "2026-05-20 10:05:00"
    row["Install Time"] = "2026-05-19 09:30:00"
    row["Attributed Touch Time"] = "2026-05-19 09:00:00"
    row["Event Name"] = "af_purchase"
    row["Event Revenue"] = "9.99"
    row["AppsFlyer ID"] = "af-id-1"
    row["Event Value"] = "af-value-1"
    return row


def _installs_raw_row() -> dict[str, str]:
    row: dict[str, str] = dict.fromkeys(INSTALLS_RAW_COLUMNS, "")
    row.update(
        {
            "AppsFlyer ID": "af-id-1",
            "Install Time": "2026-05-19 09:30:00",
            "Event Time": "2026-05-20 10:05:00",
            "Attributed Touch Time": "2026-05-19 09:00:00",
        }
    )
    return row


def test_insert_columns_match_transform_events_output_keys_for_every_report() -> None:
    for spec in REPORTS.values():
        raw_row = (
            _installs_raw_row() if spec.column_map is None else _raw_row_for(dict(spec.column_map))
        )
        df = pl.DataFrame([raw_row], schema=dict.fromkeys(raw_row, pl.Utf8))

        rows = transform_events(
            df,
            spec=spec,
            app_id="id1458505230",
            media_source_filter=None,
            event_names_filter=None,
        )

        assert len(rows) == 1
        assert set(rows[0]) == set(spec.insert_columns)


def test_registry_covers_installs_alongside_in_app_events() -> None:
    assert set(REPORTS) == {
        "in_app_events_non_organic",
        "in_app_events_retargeting",
        "installs_non_organic",
        "installs_retargeting",
    }


def test_installs_non_organic_spec_matches_the_confirmed_live_endpoint() -> None:
    spec = REPORTS["installs_non_organic"]
    assert spec.name == "installs"
    assert spec.endpoint == "installs_report"
    assert spec.attribution_type == "non_organic"


def test_installs_retargeting_spec_matches_the_confirmed_live_endpoint() -> None:
    spec = REPORTS["installs_retargeting"]
    assert spec.name == "installs"
    assert spec.endpoint == "installs-retarget"
    assert spec.attribution_type == "retargeting"


def test_both_installs_specs_never_send_event_name_but_send_47_additional_fields() -> None:
    for key in ("installs_non_organic", "installs_retargeting"):
        spec = REPORTS[key]
        assert spec.sends_event_name is False
        assert len(spec.additional_fields) == 47
        assert spec.additional_fields == INSTALLS_ADDITIONAL_FIELDS


def test_both_installs_specs_have_a_hard_clamped_60_day_retention() -> None:
    for key in ("installs_non_organic", "installs_retargeting"):
        spec = REPORTS[key]
        assert spec.retention_days == 60
        assert spec.hard_clamp_retention is True
        assert spec.window_column == "install_time"


def test_in_app_events_specs_hard_clamp_to_the_31_day_availability_window() -> None:
    """BAF-11 stage 5: a request for in-app-events dates older than the 31-day
    availability window comes back as a valid, header-only EMPTY report
    (issue #45), and delete-then-insert would then wipe the only copy of that
    window -- our own table. Warn-and-proceed (stage 4's choice) is no longer
    acceptable once the full raw export goes live.
    """
    for key in ("in_app_events_non_organic", "in_app_events_retargeting"):
        assert REPORTS[key].hard_clamp_retention is True


def test_both_installs_specs_have_column_map_none() -> None:
    for key in ("installs_non_organic", "installs_retargeting"):
        assert REPORTS[key].column_map is None


def test_installs_table_callable_reads_settings_db_table_installs() -> None:
    class _FakeSettings:
        db_table_installs = "appsflyer_installs_fb"

    for key in ("installs_non_organic", "installs_retargeting"):
        assert REPORTS[key].table(_FakeSettings()) == "appsflyer_installs_fb"  # type: ignore[arg-type]


def test_installs_raw_columns_has_128_entries_matching_the_column_sizing_measurement() -> None:
    """Pinned against docs/superpowers/specs/2026-08-13-baf-11-column-sizing.md's
    128-column installs_report/installs-retarget measurement (2026-08-13,
    com.yesimmobile, 2026-08-11: 1,872 and 918 rows).
    """
    assert len(INSTALLS_RAW_COLUMNS) == 128
    assert len(set(INSTALLS_RAW_COLUMNS)) == 128  # no duplicate raw names
    assert "Event Value" in INSTALLS_RAW_COLUMNS
    assert "App ID" in INSTALLS_RAW_COLUMNS


def test_installs_insert_columns_has_no_naming_collisions() -> None:
    spec = REPORTS["installs_non_organic"]
    assert len(spec.insert_columns) == len(set(spec.insert_columns)) == 130  # 128 + 2 injected
    assert "app_id" in spec.insert_columns  # the pipeline's OWN injected column
    assert "appsflyer_app_id" in spec.insert_columns  # AppsFlyer's raw "App ID", renamed
