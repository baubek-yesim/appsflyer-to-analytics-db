from __future__ import annotations

import datetime

import httpx
import polars as pl
import pytest
import respx
from tenacity import wait_none

from appsflyer_pipeline import appsflyer_client
from appsflyer_pipeline.appsflyer_client import (
    AppsFlyerAPIError,
    AttributionType,
    _fetch_csv,
    _is_retryable,
    chunk_date_range,
    fetch_events,
)

SAMPLE_CSV = (
    "Attributed Touch Time,Install Time,Event Time,Event Name,Event Revenue,"
    "Media Source,Campaign,AppsFlyer ID,Customer User ID\n"
    "2026-05-20 10:00:00,2026-05-19 09:00:00,2026-05-20 10:05:00,af_purchase,9.99,"
    "Facebook Ads,Summer Sale,af-id-1,user-1\n"
)


def _url(app_id: str, attribution_type: str) -> str:
    endpoint = (
        "in_app_events_report" if attribution_type == "non_organic" else "in-app-events-retarget"
    )
    return f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/{endpoint}/v5"


@respx.mock
def test_fetch_events_parses_csv() -> None:
    route = respx.get(_url("id123", "non_organic")).mock(
        return_value=httpx.Response(200, text=SAMPLE_CSV)
    )
    with httpx.Client() as client:
        df = fetch_events(
            client,
            app_id="id123",
            attribution_type="non_organic",
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source="Facebook Ads",
            event_names=["af_purchase", "af_purchase_YC"],
        )
    assert route.called
    assert df.shape[0] == 1
    assert "Event Name" in df.columns
    assert df["Event Name"][0] == "af_purchase"


@respx.mock
def test_fetch_events_follows_redirect_to_rawdata_domain() -> None:
    """AppsFlyer 302-redirects hq1.appsflyer.com to a signed rawdata.appsflyer.com
    URL to deliver the actual export; httpx does not follow redirects by default
    (unlike requests, which Mark's original scripts relied on), so this must be
    handled explicitly — confirmed against the real API during Stage 3.
    """
    redirect_url = "https://rawdata.appsflyer.com/export/token/abc123"
    respx.get(_url("id123", "non_organic")).mock(
        return_value=httpx.Response(302, headers={"location": redirect_url})
    )
    respx.get(redirect_url).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))

    with httpx.Client() as client:
        df = fetch_events(
            client,
            app_id="id123",
            attribution_type="non_organic",
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source="Facebook Ads",
            event_names=["af_purchase"],
        )
    assert df.shape[0] == 1


@respx.mock
def test_fetch_events_sends_expected_params_and_headers() -> None:
    route = respx.get(_url("id123", "retargeting")).mock(
        return_value=httpx.Response(200, text=SAMPLE_CSV)
    )
    with httpx.Client() as client:
        fetch_events(
            client,
            app_id="id123",
            attribution_type="retargeting",
            from_date=datetime.date(2026, 5, 1),
            to_date=datetime.date(2026, 5, 20),
            api_token="secret-token",
            media_source="Facebook Ads",
            event_names=["af_purchase", "af_purchase_YC"],
        )
    request = route.calls.last.request
    assert request.url.params["from"] == "2026-05-01"
    assert request.url.params["to"] == "2026-05-20"
    assert request.url.params["event_name"] == "af_purchase,af_purchase_YC"
    assert request.url.params["media_source"] == "Facebook Ads"
    assert request.headers["Authorization"] == "Bearer secret-token"


@respx.mock
def test_fetch_events_never_sends_additional_fields() -> None:
    """Dual attribution (issue #7): the `Is Primary Attribution` column that
    transform.py filters on is a STANDARD v5 export column. Requesting it via
    `additional_fields=is_primary_attribution` gets HTTP 400 "Unknown
    additional field" from the real API (verified live, 2026-07-07) — this
    pins the request shape so that regression can't quietly come back.
    """
    ua_route = respx.get(_url("id123", "non_organic")).mock(
        return_value=httpx.Response(200, text=SAMPLE_CSV)
    )
    rt_route = respx.get(_url("id123", "retargeting")).mock(
        return_value=httpx.Response(200, text=SAMPLE_CSV)
    )
    attribution_types: tuple[AttributionType, ...] = ("non_organic", "retargeting")
    with httpx.Client() as client:
        for attribution_type in attribution_types:
            fetch_events(
                client,
                app_id="id123",
                attribution_type=attribution_type,
                from_date=datetime.date(2026, 5, 20),
                to_date=datetime.date(2026, 5, 20),
                api_token="token",
                media_source="Facebook Ads",
                event_names=["af_purchase"],
            )
    for route in (ua_route, rt_route):
        assert "additional_fields" not in route.calls.last.request.url.params


@respx.mock
def test_fetch_events_empty_body_returns_empty_dataframe() -> None:
    respx.get(_url("id123", "non_organic")).mock(return_value=httpx.Response(200, text=""))
    with httpx.Client() as client:
        df = fetch_events(
            client,
            app_id="id123",
            attribution_type="non_organic",
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source="Facebook Ads",
            event_names=["af_purchase"],
        )
    assert df.is_empty()


@respx.mock
def test_fetch_events_raises_on_client_error_without_retry() -> None:
    route = respx.get(_url("id123", "non_organic")).mock(
        return_value=httpx.Response(401, text="unauthorized")
    )
    with httpx.Client() as client, pytest.raises(AppsFlyerAPIError, match="401"):
        fetch_events(
            client,
            app_id="id123",
            attribution_type="non_organic",
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source="Facebook Ads",
            event_names=["af_purchase"],
        )
    assert route.call_count == 1


@respx.mock
def test_fetch_csv_retries_on_5xx_then_succeeds() -> None:
    route = respx.get(_url("id123", "non_organic")).mock(
        side_effect=[
            httpx.Response(500, text="server error"),
            httpx.Response(500, text="server error"),
            httpx.Response(200, text=SAMPLE_CSV),
        ]
    )
    fast_fetch = _fetch_csv.retry_with(wait=wait_none())  # type: ignore[attr-defined]
    with httpx.Client() as client:
        content = fast_fetch(
            client,
            app_id="id123",
            attribution_type="non_organic",
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source="Facebook Ads",
            event_names=["af_purchase"],
        )
    assert route.call_count == 3
    assert b"af_purchase" in content


@respx.mock
def test_fetch_events_wraps_transport_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A network-level failure (not an HTTP error response) is retried (it's a
    TransportError, retryable per _is_retryable) and, on exhaustion, wrapped
    into AppsFlyerAPIError. wait_none() keeps this fast — without it, 4 real
    exponential-jitter sleeps add up to ~15s before the reraise.
    """
    monkeypatch.setattr(
        appsflyer_client,
        "_fetch_csv",
        appsflyer_client._fetch_csv.retry_with(wait=wait_none()),  # type: ignore[attr-defined]
    )
    route = respx.get(_url("id123", "non_organic")).mock(side_effect=httpx.ConnectError("boom"))

    with httpx.Client() as client, pytest.raises(AppsFlyerAPIError, match="Network failure"):
        fetch_events(
            client,
            app_id="id123",
            attribution_type="non_organic",
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source="Facebook Ads",
            event_names=["af_purchase"],
        )
    assert route.call_count == 5  # stop_after_attempt(5), all exhausted


def _status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.com")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (httpx.ConnectError("boom"), True),
        (_status_error(500), True),
        (_status_error(429), True),
        (_status_error(401), False),
        (ValueError("not an httpx error"), False),
    ],
)
def test_is_retryable_matrix(exc: BaseException, expected: bool) -> None:
    assert _is_retryable(exc) is expected


def test_chunk_date_range_splits_into_max_31_day_windows() -> None:
    chunks = chunk_date_range(datetime.date(2026, 1, 1), datetime.date(2026, 3, 31))
    assert all((end - start).days < 31 for start, end in chunks)
    assert chunks[0][0] == datetime.date(2026, 1, 1)
    assert chunks[-1][1] == datetime.date(2026, 3, 31)
    for (_, prev_end), (next_start, _) in zip(chunks, chunks[1:], strict=False):
        assert next_start == prev_end + datetime.timedelta(days=1)


def test_chunk_date_range_single_day() -> None:
    chunks = chunk_date_range(datetime.date(2026, 5, 20), datetime.date(2026, 5, 20))
    assert chunks == [(datetime.date(2026, 5, 20), datetime.date(2026, 5, 20))]


def test_chunk_date_range_rejects_inverted_range() -> None:
    with pytest.raises(ValueError, match="after"):
        chunk_date_range(datetime.date(2026, 5, 20), datetime.date(2026, 5, 1))


@respx.mock
def test_fetch_events_raises_on_malformed_csv() -> None:
    """Issue #11: a ragged row (more fields than the header declares) makes
    polars raise ComputeError with infer_schema_length=0 -- isolate it as
    AppsFlyerAPIError so _process_window treats it like any other per-window
    upstream-data failure, instead of killing the whole run.
    """
    respx.get(_url("id123", "non_organic")).mock(
        return_value=httpx.Response(200, text="a,b,c\n1,2,3,4,5\n")
    )
    with httpx.Client() as client, pytest.raises(AppsFlyerAPIError, match="unparseable CSV"):
        fetch_events(
            client,
            app_id="id123",
            attribution_type="non_organic",
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source="Facebook Ads",
            event_names=["af_purchase"],
        )


class _StubDataFrame:
    """Minimal stand-in for a polars DataFrame exposing only what fetch_events
    reads (.height) -- avoids generating an actual 1M-row CSV in a unit test.
    """

    height = 1_000_000


@respx.mock
def test_fetch_events_raises_on_1m_row_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Issue #15: AppsFlyer's Pull API silently truncates raw-data exports beyond
    1,000,000 rows with no error. fetch_events must fail loudly instead of
    returning truncated data as if it were a complete, successful fetch.
    """
    monkeypatch.setattr(pl, "read_csv", lambda *args, **kwargs: _StubDataFrame())
    respx.get(_url("id123", "non_organic")).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))
    with httpx.Client() as client, pytest.raises(AppsFlyerAPIError, match="1M-row cap"):
        fetch_events(
            client,
            app_id="id123",
            attribution_type="non_organic",
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source="Facebook Ads",
            event_names=["af_purchase"],
        )
