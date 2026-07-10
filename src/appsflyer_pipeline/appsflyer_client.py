"""AppsFlyer Pull API client — Non-Organic + Retargeting v5 endpoints.

Hybrid-adapted from Mark Malovichko's reference scripts (BAF-2 comment 62293):
same endpoints, request params, and 90-day/31-day chunk math, rebuilt on
httpx + tenacity for retry/backoff and returning typed polars DataFrames.

Column names are left exactly as AppsFlyer returns them (e.g. "Attributed
Touch Time") — normalizing to the target schema is transform.py's job (Stage 4).
"""

from __future__ import annotations

import datetime
from io import BytesIO
from typing import Literal

import httpx
import polars as pl
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

AttributionType = Literal["non_organic", "retargeting"]

_ENDPOINT_BY_ATTRIBUTION: dict[AttributionType, str] = {
    "non_organic": "in_app_events_report",
    "retargeting": "in-app-events-retarget",
}

_BASE_URL = "https://hq1.appsflyer.com/api/raw-data/export/app"
_REQUEST_TIMEOUT = 120.0

# AppsFlyer Pull API limits (per Mark's comment on BAF-2): data retained 90 days,
# and at most 31 days of data can be requested per call.
MAX_RETENTION_DAYS = 90
MAX_CHUNK_DAYS = 31


class AppsFlyerAPIError(RuntimeError):
    """Raised for actionable AppsFlyer Pull API failures."""


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return False


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=30),
    reraise=True,
)
def _fetch_csv(
    client: httpx.Client,
    *,
    app_id: str,
    attribution_type: AttributionType,
    from_date: datetime.date,
    to_date: datetime.date,
    api_token: str,
    media_source: str,
    event_names: list[str],
    timezone: str | None = None,
) -> bytes:
    endpoint = _ENDPOINT_BY_ATTRIBUTION[attribution_type]
    url = f"{_BASE_URL}/{app_id}/{endpoint}/v5"
    params = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "event_name": ",".join(event_names),
        "media_source": media_source,
    }
    if timezone is not None:
        # Issue #53: without this param AppsFlyer reports in UTC; with it, event
        # times and the from/to day boundaries follow the app's configured zone.
        params["timezone"] = timezone
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "text/csv",
    }
    # AppsFlyer redirects (302) from hq1.appsflyer.com to a signed rawdata.appsflyer.com
    # URL to deliver the actual export; httpx (unlike requests) does not follow
    # redirects by default, so this must be explicit per-request.
    response = client.get(
        url, params=params, headers=headers, timeout=_REQUEST_TIMEOUT, follow_redirects=True
    )
    response.raise_for_status()
    return response.content


def fetch_events(
    client: httpx.Client,
    *,
    app_id: str,
    attribution_type: AttributionType,
    from_date: datetime.date,
    to_date: datetime.date,
    api_token: str,
    media_source: str,
    event_names: list[str],
    timezone: str | None = None,
) -> pl.DataFrame:
    """Fetch one app/attribution-type/date-range chunk as a raw DataFrame.

    Returns an empty DataFrame when AppsFlyer has no matching events for the
    window — delivered as a headers-only CSV, a legitimately common case. A
    truly EMPTY response body is an upstream anomaly and raises
    AppsFlyerAPIError instead (issue #26).

    `timezone` (issue #53) selects the timezone AppsFlyer expresses the report
    in — both the event-time values and the from/to day boundaries. None (the
    default) means UTC.
    """
    try:
        content = _fetch_csv(
            client,
            app_id=app_id,
            attribution_type=attribution_type,
            from_date=from_date,
            to_date=to_date,
            api_token=api_token,
            media_source=media_source,
            event_names=event_names,
            timezone=timezone,
        )
    except httpx.HTTPStatusError as exc:
        raise AppsFlyerAPIError(
            f"AppsFlyer API [{attribution_type}] for {app_id} ({from_date} to {to_date}) "
            f"failed: HTTP {exc.response.status_code}: {exc.response.text[:200]}"
        ) from exc
    except httpx.TransportError as exc:
        raise AppsFlyerAPIError(
            f"Network failure calling AppsFlyer API [{attribution_type}] for {app_id}: {exc}"
        ) from exc

    if not content.strip():
        # Issue #26: a legitimate empty report always includes CSV headers
        # (live-verified 2026-07-09: a quiet window returns a headers-only,
        # 81-column CSV). A truly empty body is an upstream anomaly -- raising
        # fails only this window and preserves its previously loaded rows,
        # instead of flowing into load_events' delete-then-insert-nothing.
        raise AppsFlyerAPIError(
            f"AppsFlyer returned an empty response body [{attribution_type}] for {app_id} "
            f"({from_date} to {to_date}) — a legitimate empty report always includes CSV headers"
        )
    # infer_schema_length=0 forces every column to Utf8: chunks are read
    # independently and later pl.concat'ed, so dtype inference (e.g. an
    # all-null column guessed as Int64 in one chunk, Utf8 in another) must
    # not be allowed to diverge between them. transform.py applies real types.
    try:
        df = pl.read_csv(BytesIO(content), infer_schema_length=0)
    except (pl.exceptions.ComputeError, pl.exceptions.NoDataError) as exc:
        raise AppsFlyerAPIError(
            f"AppsFlyer returned an unparseable CSV [{attribution_type}] for {app_id} "
            f"({from_date} to {to_date}): {exc}"
        ) from exc
    if df.height >= 1_000_000:
        raise AppsFlyerAPIError(
            f"Report for {app_id} [{attribution_type}] {from_date}..{to_date} hit the "
            f"Pull API 1M-row cap — data is likely truncated; split the window into smaller chunks."
        )
    return df


def chunk_date_range(
    start: datetime.date, end: datetime.date, max_days: int = MAX_CHUNK_DAYS
) -> list[tuple[datetime.date, datetime.date]]:
    """Split [start, end] (inclusive) into consecutive windows of at most `max_days` days."""
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    chunks: list[tuple[datetime.date, datetime.date]] = []
    current_start = start
    while current_start <= end:
        current_end = min(current_start + datetime.timedelta(days=max_days - 1), end)
        chunks.append((current_start, current_end))
        current_start = current_end + datetime.timedelta(days=1)
    return chunks
