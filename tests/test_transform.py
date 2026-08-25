from __future__ import annotations

import datetime
import logging
from decimal import Decimal
from io import BytesIO

import polars as pl
import pytest

from appsflyer_pipeline.appsflyer_client import AttributionType
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
    "Is Primary Attribution",
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
        "Is Primary Attribution": "true",
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


def test_transform_preserves_appsflyer_times_verbatim() -> None:
    """Issue #51: time values must never be shifted, only validated.

    Times flow raw CSV string -> naive datetime -> literal DATETIME write, so
    round-tripping each parsed value through AppsFlyer's own format must
    reproduce the raw string byte-for-byte, and the values must stay
    timezone-naive — a tz-aware datetime is the only way a shift could creep
    into the PyMySQL serialization. Live-verified against production
    2026-07-10: every raw Event Time string in a full window matched the
    stored value exactly.
    """
    raw_times = {
        "Attributed Touch Time": "2026-07-09 21:41:54",
        "Install Time": "2026-07-09 00:56:09",
        "Event Time": "2026-07-09 23:14:40",
    }
    edge_times = {
        "Attributed Touch Time": "2026-07-08 23:59:59",
        "Install Time": "2026-07-09 00:00:00",
        "Event Time": "2026-07-09 00:00:00",
    }
    df = _df([_raw_row(**raw_times), _raw_row(**edge_times)])
    rows = transform_events(
        df,
        attribution_type="non_organic",
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase", "af_purchase_YC"],
    )
    assert len(rows) == 2
    for expected, row in zip((raw_times, edge_times), rows, strict=True):
        for raw_col, target_col in (
            ("Attributed Touch Time", "attributed_touch_time"),
            ("Install Time", "install_time"),
            ("Event Time", "event_time"),
        ):
            value = row[target_col]
            assert isinstance(value, datetime.datetime)
            assert value.tzinfo is None
            assert value.strftime("%Y-%m-%d %H:%M:%S") == expected[raw_col]


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


def test_transform_keeps_every_media_source_when_filter_unset() -> None:
    """BAF-11 stage 1: with no media-source filter the client-side re-filter has
    to disappear entirely, not degrade into a comparison. Organic rows carry an
    EMPTY Media Source, so a predicate built from None would drop exactly the
    rows the full raw export exists to capture.
    """
    df = _df(
        [
            _raw_row(**{"Media Source": "Facebook Ads", "AppsFlyer ID": "af-id-1"}),
            _raw_row(**{"Media Source": "Google Ads", "AppsFlyer ID": "af-id-2"}),
            _raw_row(**{"Media Source": "", "AppsFlyer ID": "af-id-3"}),
        ]
    )
    rows = transform_events(
        df,
        attribution_type="non_organic",
        app_id="id1458505230",
        media_source_filter=None,
        event_names_filter=["af_purchase", "af_purchase_YC"],
    )
    assert [row["media_source"] for row in rows] == ["Facebook Ads", "Google Ads", ""]


def test_transform_keeps_every_event_name_when_filter_unset() -> None:
    df = _df(
        [
            _raw_row(**{"Event Name": "af_purchase", "AppsFlyer ID": "af-id-1"}),
            _raw_row(**{"Event Name": "af_login", "AppsFlyer ID": "af-id-2"}),
            _raw_row(**{"Event Name": "install", "AppsFlyer ID": "af-id-3"}),
        ]
    )
    rows = transform_events(
        df,
        attribution_type="non_organic",
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=None,
    )
    assert [row["event_name"] for row in rows] == ["af_purchase", "af_login", "install"]


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
                    "Event Revenue": "",
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
    assert rows[0]["event_revenue"] is None


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


def test_transform_raises_on_unparseable_timestamp() -> None:
    df = _df([_raw_row(**{"Event Time": "not-a-timestamp"})])
    with pytest.raises(TransformError, match="timestamp"):
        transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase"],
        )


def test_transform_keeps_non_primary_rows() -> None:
    """Issue #47 (data-analytics decision on #46, reversing #7's filter): rows
    with `Is Primary Attribution = false` load like any other. attribution_type
    is part of Mark's dedup key, so a dual-attributed purchase legitimately
    appears once per report — the flag is not consulted at all.
    """
    df = _df(
        [
            _raw_row(**{"AppsFlyer ID": "af-primary", "Is Primary Attribution": "true"}),
            _raw_row(**{"AppsFlyer ID": "af-secondary", "Is Primary Attribution": "false"}),
        ]
    )
    rows = transform_events(
        df,
        attribution_type="non_organic",
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase", "af_purchase_YC"],
    )
    assert [r["appsflyer_id"] for r in rows] == ["af-primary", "af-secondary"]


@pytest.mark.parametrize("attribution_type", ["non_organic", "retargeting"])
def test_transform_never_requires_the_flag_column(attribution_type: AttributionType) -> None:
    """Issue #47: `Is Primary Attribution` is no longer required for either
    report — a response without it transforms fine (it was only ever read by
    the removed filter, never loaded into the table).
    """
    df = _df([_raw_row()]).drop("Is Primary Attribution")
    rows = transform_events(
        df,
        attribution_type=attribution_type,
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase"],
    )
    assert len(rows) == 1


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


# A headers-only export body, as the real API returns for a genuinely quiet
# window (live-verified 2026-07-09: HTTP 200, UTF-8 BOM, all 81 columns,
# zero data rows). Reconstructed here with every required raw column plus a
# sample of the real export's extra columns -- the full 81-column line adds
# no test signal and re-capturing it costs an API-quota report download.
HEADERS_ONLY_EXPORT = (
    b"\xef\xbb\xbf"
    + (
        ",".join(
            [
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
                "Campaign",
                "Campaign ID",
                "Adset",
                "Adset ID",
                "Ad",
                "Ad ID",
                "AppsFlyer ID",
                "Customer User ID",
                "Is Primary Attribution",
                "Region",
                "Cost Currency",
            ]
        ).encode()
    )
    + b"\n"
)


@pytest.mark.parametrize("attribution_type", ["non_organic", "retargeting"])
def test_transform_headers_only_response_returns_empty(
    attribution_type: AttributionType,
) -> None:
    """Issue #26: the one legitimate empty shape -- a headers-only CSV whose
    columns include everything we require -- transforms to [] (so the loader's
    delete+insert-0, with #10's wipe WARNING, still applies). Parsed through
    pl.read_csv exactly like production to also pin polars' BOM handling.
    """
    df = pl.read_csv(BytesIO(HEADERS_ONLY_EXPORT), infer_schema_length=0)
    assert df.columns[0] == "Attributed Touch Type"  # BOM stripped, not '﻿Attributed...'
    rows = transform_events(
        df,
        attribution_type=attribution_type,
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase"],
    )
    assert rows == []


@pytest.mark.parametrize(
    "df",
    [
        pytest.param(
            pl.read_csv(
                BytesIO(b"Subscription package limitation. Contact your CSM"),
                infer_schema_length=0,
            ),
            id="one-line-error-text",  # parses to a (0, 1) frame
        ),
        pytest.param(
            pl.read_csv(
                BytesIO(HEADERS_ONLY_EXPORT.replace(b"Event Time", b"Event Time Renamed")),
                infer_schema_length=0,
            ),
            id="renamed-required-column",
        ),
        pytest.param(pl.DataFrame(), id="zero-column-empty-frame"),
    ],
)
def test_transform_raises_on_zero_row_frame_with_missing_columns(
    df: pl.DataFrame,
) -> None:
    """Issue #26: a 0-row frame whose headers do NOT include the required
    columns is an anomaly (error-text body or schema drift), not a quiet
    window -- before this fix it bypassed the schema guard via the is_empty
    early-return and wiped the window downstream at exit 0.
    """
    with pytest.raises(TransformError, match="missing expected column"):
        transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase"],
        )


def test_transform_collapses_exact_duplicate_rows(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """AppsFlyer returning the identical row twice within one report response
    (not issue #7's cross-report case) is collapsed to one row, with a WARNING
    for visibility — same principle as issue #10's wipe-visibility logging.
    """
    df = _df([_raw_row(), _raw_row()])
    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.transform"):
        rows = transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase", "af_purchase_YC"],
        )
    assert len(rows) == 1
    assert any(
        "collapsed 1 exact-duplicate" in r.message and "id1458505230" in r.message
        for r in caplog.records
    )


def test_transform_keeps_only_the_latest_install_time_on_conflict(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Same key (event_time, event_name, appsflyer_id) but different
    event_revenue: only the row with the latest install_time survives
    (data-analytics decision 2026-08-14), and the WARNING reports the revenue
    that went with the discarded row.

    Ordering is by install_time, not by position — here the LATER install_time
    arrives first in the report, so the surviving row is the first one.
    """
    df = _df(
        [
            _raw_row(**{"Event Revenue": "3", "Install Time": "2026-05-19 09:30:00"}),
            _raw_row(**{"Event Revenue": "4", "Install Time": "2026-05-18 08:00:00"}),
        ]
    )
    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.transform"):
        rows = transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase", "af_purchase_YC"],
        )

    assert [row["event_revenue"] for row in rows] == [Decimal("3")]
    messages = " ".join(r.message for r in caplog.records)
    assert "dropped 1 conflicting" in messages and "id1458505230" in messages
    assert "discarded event_revenue: 4" in messages


def test_transform_later_install_time_wins_from_either_report_position(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The mirror image of the test above: the later install_time arrives
    second and still wins, so the outcome doesn't depend on report order.
    """
    df = _df(
        [
            _raw_row(**{"Event Revenue": "3", "Install Time": "2026-05-18 08:00:00"}),
            _raw_row(**{"Event Revenue": "4", "Install Time": "2026-05-19 09:30:00"}),
        ]
    )
    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.transform"):
        rows = transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase", "af_purchase_YC"],
        )

    assert [row["event_revenue"] for row in rows] == [Decimal("4")]
    assert "discarded event_revenue: 3" in " ".join(r.message for r in caplog.records)


def test_transform_null_install_time_loses_to_a_real_one() -> None:
    """A NULL install_time ranks below any real timestamp, mirroring MariaDB's
    `ORDER BY install_time DESC` (NULLs last) in the SQL rewrite.
    """
    df = _df(
        [
            _raw_row(**{"Event Revenue": "3", "Install Time": ""}),
            _raw_row(**{"Event Revenue": "4", "Install Time": "2026-05-18 08:00:00"}),
        ]
    )
    rows = transform_events(
        df,
        attribution_type="non_organic",
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase", "af_purchase_YC"],
    )

    assert [row["event_revenue"] for row in rows] == [Decimal("4")]


def test_transform_warns_when_install_time_cannot_break_the_tie(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The case measured in production (3 vs 4 EUR at 2026-07-19 04:26:43): two
    distinct purchases by one user in the same second share an appsflyer_id and
    therefore an install_time. Nothing in the data can choose between them, so
    the last row in report order wins (matching the SQL rewrite's `id DESC`) and
    the tie is called out separately — this is a real purchase being discarded.
    """
    df = _df(
        [
            _raw_row(**{"Event Revenue": "3"}),
            _raw_row(**{"Event Revenue": "4"}),
        ]
    )
    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.transform"):
        rows = transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase", "af_purchase_YC"],
        )

    assert [row["event_revenue"] for row in rows] == [Decimal("4")]
    messages = " ".join(r.message for r in caplog.records)
    assert "1 of those conflict(s) had identical install_time" in messages
    assert "picked by report order, not by data" in messages


def test_transform_counts_every_conflict_but_names_only_the_first(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Two conflicting pairs are both resolved and both counted; the WARNING
    names the first key so a large window doesn't produce an unreadable log
    line, and the discarded revenue is summed across both.
    """
    df = _df(
        [
            _raw_row(**{"Event Revenue": "3"}),
            _raw_row(**{"Event Revenue": "4"}),
            _raw_row(**{"AppsFlyer ID": "other-id", "Event Revenue": "5"}),
            _raw_row(**{"AppsFlyer ID": "other-id", "Event Revenue": "6"}),
        ]
    )
    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.transform"):
        rows = transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase", "af_purchase_YC"],
        )

    assert len(rows) == 2
    messages = " ".join(r.message for r in caplog.records)
    assert "dropped 2 conflicting" in messages
    assert "discarded event_revenue: 8" in messages  # 3 + 5, the two losers
    assert "af-id-1" in messages and "other-id" not in messages


def test_transform_reports_conflicts_and_exact_duplicates_separately(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A window carrying both kinds of duplicate keeps the two counts apart:
    the exact pair collapses to one row, the conflicting pair resolves to one.
    """
    df = _df(
        [
            _raw_row(),
            _raw_row(),
            _raw_row(**{"AppsFlyer ID": "other-id", "Event Revenue": "3"}),
            _raw_row(**{"AppsFlyer ID": "other-id", "Event Revenue": "4"}),
        ]
    )
    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.transform"):
        rows = transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase", "af_purchase_YC"],
        )

    assert len(rows) == 2
    messages = " ".join(r.message for r in caplog.records)
    assert "collapsed 1 exact-duplicate" in messages
    assert "dropped 1 conflicting" in messages


def test_transform_conflict_with_null_revenue_does_not_break_the_sum(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """`event_revenue` is nullable, so the discarded-revenue tally has to treat
    a NULL loser as zero rather than raising on Decimal + None.
    """
    df = _df(
        [
            _raw_row(**{"Event Revenue": "", "Install Time": "2026-05-18 08:00:00"}),
            _raw_row(**{"Event Revenue": "4", "Install Time": "2026-05-19 09:30:00"}),
        ]
    )
    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.transform"):
        rows = transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase", "af_purchase_YC"],
        )

    assert [row["event_revenue"] for row in rows] == [Decimal("4")]
    assert "discarded event_revenue: 0" in " ".join(r.message for r in caplog.records)
