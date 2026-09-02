"""AppsFlyer Pull API client — Non-Organic + Retargeting v5 endpoints.

Hybrid-adapted from Mark Malovichko's reference scripts (BAF-2 comment 62293):
same endpoints, request params, and 90-day/31-day chunk math, rebuilt on
httpx + tenacity for retry/backoff and returning typed polars DataFrames.

Column names are left exactly as AppsFlyer returns them (e.g. "Attributed
Touch Time") — normalizing to the target schema is transform.py's job (Stage 4).
"""

from __future__ import annotations

import datetime
import logging
from io import BytesIO
from typing import TYPE_CHECKING, Literal

import httpx
import polars as pl
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

AttributionType = Literal["non_organic", "retargeting"]

if TYPE_CHECKING:
    from appsflyer_pipeline.reports import ReportSpec

_BASE_URL = "https://hq1.appsflyer.com/api/raw-data/export/app"
_REQUEST_TIMEOUT = 120.0

# AppsFlyer Pull API limits (per Mark's comment on BAF-2): data retained 90 days,
# and at most 31 days of data can be requested per call.
MAX_RETENTION_DAYS = 90
MAX_CHUNK_DAYS = 31

# BAF-11 stage 2: AppsFlyer's Pull API defaults to truncating a report at
# maximum_rows=200,000 with no error -- a normal 200 response, just short.
# A 31-day chunk on the unfiltered stream measures ~239k rows/day-equivalent
# (probed 2026-08-13), so it can silently exceed that default. This raises the
# ceiling to the client's own 1M-row hard cap (see fetch_events below).
DEFAULT_MAXIMUM_ROWS = 1_000_000

logger = logging.getLogger(__name__)


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
    spec: ReportSpec,
    from_date: datetime.date,
    to_date: datetime.date,
    api_token: str,
    media_source: str | None,
    event_names: list[str] | None,
    timezone: str | None = None,
    maximum_rows: int = DEFAULT_MAXIMUM_ROWS,
) -> bytes:
    url = f"{_BASE_URL}/{app_id}/{spec.endpoint}/v5"
    params: dict[str, str | int] = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "maximum_rows": maximum_rows,
    }
    # BAF-11 stage 1: None means "no server-side filter", which has to be an
    # ABSENT param. Sending `media_source=` (empty) instead would be a filter
    # matching nothing, and AppsFlyer answers that with a valid header-only
    # report — indistinguishable downstream from a genuinely quiet window, so
    # the idempotent delete-then-insert would wipe it (issue #45's shape).
    if spec.sends_event_name and event_names is not None:
        params["event_name"] = ",".join(event_names)
    if spec.sends_media_source and media_source is not None:
        params["media_source"] = media_source
    if timezone is not None:
        # Issue #53: without this param AppsFlyer reports in UTC; with it, event
        # times and the from/to day boundaries follow the app's configured zone.
        params["timezone"] = timezone
    # BAF-11 stage 3: both registered specs have additional_fields=(), so this
    # is dead for now -- installs (stage 5/6) is the first spec to set it.
    if spec.additional_fields:
        params["additional_fields"] = ",".join(spec.additional_fields)
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


def _fetch_and_parse(
    client: httpx.Client,
    *,
    app_id: str,
    spec: ReportSpec,
    from_date: datetime.date,
    to_date: datetime.date,
    api_token: str,
    media_source: str | None,
    event_names: list[str] | None,
    timezone: str | None,
    maximum_rows: int,
) -> pl.DataFrame:
    """One HTTP call for exactly [from_date, to_date] -- no splitting, no
    row-cap decision. `fetch_events` is the public entry point; this is its
    single-window building block, factored out so `fetch_events` can call it
    twice on a truncated response without duplicating the error handling.
    """
    try:
        content = _fetch_csv(
            client,
            app_id=app_id,
            spec=spec,
            from_date=from_date,
            to_date=to_date,
            api_token=api_token,
            media_source=media_source,
            event_names=event_names,
            timezone=timezone,
            maximum_rows=maximum_rows,
        )
    except httpx.HTTPStatusError as exc:
        raise AppsFlyerAPIError(
            f"AppsFlyer API [{spec.attribution_type}] for {app_id} ({from_date} to {to_date}) "
            f"failed: HTTP {exc.response.status_code}: {exc.response.text[:200]}"
        ) from exc
    except httpx.TransportError as exc:
        raise AppsFlyerAPIError(
            f"Network failure calling AppsFlyer API [{spec.attribution_type}] for {app_id}: {exc}"
        ) from exc

    if not content.strip():
        # Issue #26: a legitimate empty report always includes CSV headers
        # (live-verified 2026-07-09: a quiet window returns a headers-only,
        # 81-column CSV). A truly empty body is an upstream anomaly -- raising
        # fails only this window and preserves its previously loaded rows,
        # instead of flowing into load_events' delete-then-insert-nothing.
        raise AppsFlyerAPIError(
            f"AppsFlyer returned an empty response body [{spec.attribution_type}] for {app_id} "
            f"({from_date} to {to_date}) — a legitimate empty report always includes CSV headers"
        )
    # infer_schema_length=0 forces every column to Utf8: chunks are read
    # independently and later pl.concat'ed, so dtype inference (e.g. an
    # all-null column guessed as Int64 in one chunk, Utf8 in another) must
    # not be allowed to diverge between them. transform.py applies real types.
    try:
        return pl.read_csv(BytesIO(content), infer_schema_length=0)
    except (pl.exceptions.ComputeError, pl.exceptions.NoDataError) as exc:
        raise AppsFlyerAPIError(
            f"AppsFlyer returned an unparseable CSV [{spec.attribution_type}] for {app_id} "
            f"({from_date} to {to_date}): {exc}"
        ) from exc


def fetch_events(
    client: httpx.Client,
    *,
    app_id: str,
    spec: ReportSpec,
    from_date: datetime.date,
    to_date: datetime.date,
    api_token: str,
    media_source: str | None,
    event_names: list[str] | None,
    timezone: str | None = None,
    maximum_rows: int = DEFAULT_MAXIMUM_ROWS,
) -> pl.DataFrame:
    """Fetch one app/attribution-type/date-range chunk as a raw DataFrame.

    `media_source`/`event_names` are optional filters (BAF-11 stage 1): None
    omits the param entirely, so AppsFlyer returns every media source / every
    event name for the window.

    Returns an empty DataFrame when AppsFlyer has no matching events for the
    window — delivered as a headers-only CSV, a legitimately common case. A
    truly EMPTY response body is an upstream anomaly and raises
    AppsFlyerAPIError instead (issue #26).

    `timezone` (issue #53) selects the timezone AppsFlyer expresses the report
    in — both the event-time values and the from/to day boundaries. None (the
    default) means UTC.

    `maximum_rows` (BAF-11 stage 2) is sent on every request to raise
    AppsFlyer's own silent-truncation default (200,000) — see the module
    docstring on DEFAULT_MAXIMUM_ROWS. If a response comes back with exactly
    `maximum_rows` rows, that is itself evidence of truncation (a complete
    report would have returned fewer), so the window is split in half and each
    half is fetched (and, if still truncated, split again) instead of loading
    a silently incomplete window. A single day that still hits the cap cannot
    be split further and raises AppsFlyerAPIError.
    """
    df = _fetch_and_parse(
        client,
        app_id=app_id,
        spec=spec,
        from_date=from_date,
        to_date=to_date,
        api_token=api_token,
        media_source=media_source,
        event_names=event_names,
        timezone=timezone,
        maximum_rows=maximum_rows,
    )
    if df.height < maximum_rows:
        return df

    if from_date == to_date:
        raise AppsFlyerAPIError(
            f"Report for {app_id} [{spec.attribution_type}] {from_date}..{to_date} hit the "
            f"{maximum_rows}-row cap on a single day — data is likely truncated and the "
            f"window cannot be split any further."
        )

    mid = from_date + (to_date - from_date) // 2
    logger.warning(
        "AppsFlyer response for %s [%s] %s..%s hit the %d-row cap -- splitting into "
        "%s..%s and %s..%s (extra report-download quota spent)",
        app_id,
        spec.attribution_type,
        from_date,
        to_date,
        maximum_rows,
        from_date,
        mid,
        mid + datetime.timedelta(days=1),
        to_date,
    )
    first_half = fetch_events(
        client,
        app_id=app_id,
        spec=spec,
        from_date=from_date,
        to_date=mid,
        api_token=api_token,
        media_source=media_source,
        event_names=event_names,
        timezone=timezone,
        maximum_rows=maximum_rows,
    )
    second_half = fetch_events(
        client,
        app_id=app_id,
        spec=spec,
        from_date=mid + datetime.timedelta(days=1),
        to_date=to_date,
        api_token=api_token,
        media_source=media_source,
        event_names=event_names,
        timezone=timezone,
        maximum_rows=maximum_rows,
    )
    try:
        return pl.concat([first_half, second_half])
    except pl.exceptions.PolarsError as exc:
        raise AppsFlyerAPIError(
            f"Could not combine split halves [{spec.attribution_type}] for {app_id} "
            f"({from_date} to {to_date}): {exc}"
        ) from exc


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
