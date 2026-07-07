from __future__ import annotations

import datetime
from decimal import Decimal

import polars as pl
import pytest

from appsflyer_pipeline.transform import TransformError, transform_events

RAW_COLUMNS = [
    "Attributed Touch Type",
    "Attributed Touch Time",
    "Install Time",
    "Event Time",
    "Event Name",
    "Event Revenue",
    "Media Source",
    "Channel",
    "Campaign",
    "Campaign ID",
    "Adset",
    "Adset ID",
    "Ad",
    "Ad ID",
    "AppsFlyer ID",
    "Customer User ID",
    "Region",  # an extra raw column we don't care about, matching the real ~81-column response
]


def _raw_row(**overrides: str | None) -> dict[str, str | None]:
    row: dict[str, str | None] = {
        "Attributed Touch Type": "click",
        "Attributed Touch Time": "2026-05-19 09:00:00",
        "Install Time": "2026-05-19 09:30:00",
        "Event Time": "2026-05-20 10:05:00",
        "Event Name": "af_purchase",
        "Event Revenue": "9.99",
        "Media Source": "Facebook Ads",
        "Channel": "Social",
        "Campaign": "Summer Sale",
        "Campaign ID": "cmp-1",
        "Adset": "Adset A",
        "Adset ID": "adset-1",
        "Ad": "Ad A",
        "Ad ID": "ad-1",
        "AppsFlyer ID": "af-id-1",
        "Customer User ID": "user-1",
        "Region": "EU",
    }
    row.update(overrides)
    return row


def _df(rows: list[dict[str, str | None]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=dict.fromkeys(RAW_COLUMNS, pl.Utf8))


def test_transform_maps_columns_and_adds_attribution_app_id() -> None:
    df = _df([_raw_row()])
    rows = transform_events(
        df,
        attribution_type="non_organic",
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase", "af_purchase_YC"],
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["event_time"] == datetime.datetime(2026, 5, 20, 10, 5, 0)
    assert row["install_time"] == datetime.datetime(2026, 5, 19, 9, 30, 0)
    assert row["attributed_touch_time"] == datetime.datetime(2026, 5, 19, 9, 0, 0)
    assert row["event_name"] == "af_purchase"
    assert row["event_revenue"] == Decimal("9.99")
    assert row["media_source"] == "Facebook Ads"
    assert row["campaign_id"] == "cmp-1"
    assert row["appsflyer_id"] == "af-id-1"
    assert row["customer_user_id"] == "user-1"
    assert row["attribution_type"] == "non_organic"
    assert row["app_id"] == "id1458505230"
    assert "Region" not in row


def test_transform_filters_out_non_matching_media_source() -> None:
    df = _df([_raw_row(**{"Media Source": "Google Ads"})])
    rows = transform_events(
        df,
        attribution_type="non_organic",
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase", "af_purchase_YC"],
    )
    assert rows == []


def test_transform_filters_out_non_matching_event_name() -> None:
    df = _df([_raw_row(**{"Event Name": "af_login"})])
    rows = transform_events(
        df,
        attribution_type="non_organic",
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase", "af_purchase_YC"],
    )
    assert rows == []


def test_transform_handles_blank_optional_fields_as_none() -> None:
    df = _df(
        [
            _raw_row(
                **{
                    "Install Time": None,
                    "Attributed Touch Time": "",
                    "Customer User ID": None,
                }
            )
        ]
    )
    rows = transform_events(
        df,
        attribution_type="retargeting",
        app_id="com.yesimmobile",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase", "af_purchase_YC"],
    )
    assert rows[0]["install_time"] is None
    assert rows[0]["attributed_touch_time"] is None
    assert rows[0]["customer_user_id"] is None


def test_transform_raises_on_missing_required_raw_column() -> None:
    df = pl.DataFrame([{"Event Time": "2026-05-20 10:05:00"}], schema={"Event Time": pl.Utf8})
    with pytest.raises(TransformError, match="missing expected column"):
        transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase"],
        )


def test_transform_raises_on_blank_required_field() -> None:
    df = _df([_raw_row(**{"AppsFlyer ID": ""})])
    with pytest.raises(TransformError, match="appsflyer_id"):
        transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase"],
        )


def test_transform_raises_on_unparseable_revenue() -> None:
    df = _df([_raw_row(**{"Event Revenue": "not-a-number"})])
    with pytest.raises(TransformError, match="event_revenue"):
        transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase"],
        )


def test_transform_empty_dataframe_returns_empty_list() -> None:
    df = _df([])
    rows = transform_events(
        df,
        attribution_type="non_organic",
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase"],
    )
    assert rows == []
