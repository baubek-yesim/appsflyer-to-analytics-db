"""Tests for the ReportSpec registry (BAF-11 stage 3).

Stage 3 is a pure refactor -- REPORTS holds only the two report definitions
that already exist. These tests pin the registry's shape and the one new
invariant the refactor introduces: insert_columns must always equal exactly
the keys transform_events() actually produces for that spec.
"""

from __future__ import annotations

import polars as pl

from appsflyer_pipeline.reports import REPORTS
from appsflyer_pipeline.transform import transform_events


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


def test_insert_columns_match_transform_events_output_keys_for_every_report() -> None:
    for spec in REPORTS.values():
        raw_row = _raw_row_for(dict(spec.column_map))
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
