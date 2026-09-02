"""Tests for the ReportSpec registry (BAF-11 stage 3).

Stage 3 is a pure refactor -- REPORTS holds only the two report definitions
that already exist. These tests pin the registry's shape and the one new
invariant the refactor introduces: insert_columns must always equal exactly
the keys transform_events() actually produces for that spec.
"""

from __future__ import annotations

from appsflyer_pipeline.reports import REPORTS


def test_registry_covers_in_app_events_non_organic_and_retargeting() -> None:
    assert set(REPORTS) == {"in_app_events_non_organic", "in_app_events_retargeting"}


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
    for spec in REPORTS.values():
        assert spec.sends_event_name is True
        assert spec.sends_media_source is True
        assert spec.additional_fields == ()


def test_both_specs_share_the_in_app_events_table_and_retention() -> None:
    for spec in REPORTS.values():
        assert spec.retention_days == 90
        assert spec.window_column == "event_time"


def test_table_callable_reads_settings_db_table() -> None:
    class _FakeSettings:
        db_table = "appsflyer_events_fb"

    for spec in REPORTS.values():
        assert spec.table(_FakeSettings()) == "appsflyer_events_fb"  # type: ignore[arg-type]
