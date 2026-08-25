# BAF-11 Stage 2: Quota-Aware Chunking + Full-Array Dedupe/NULL Hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the AppsFlyer chunk size configurable and quota-safe (Этап 2 of the BAF-11 spec), and fix two data-loss bugs in `transform.py` that block ever turning on the unfiltered full-array export (Этап 8 of the same spec): a real `Event Value` conflict silently drops distinct rows, and one row with a missing required field currently costs the entire 31-day window.

**Architecture:** No new modules. Three independent, additive changes to the existing BAF-2 pipeline: (1) `Settings.appsflyer_chunk_days` threaded into `pipeline._iter_work_items`, replacing the hardcoded 31-day chunk; (2) `appsflyer_client.fetch_events` gains an explicit `maximum_rows` request param and a recursive halve-and-retry when a response returns exactly that many rows (AppsFlyer truncates silently otherwise); (3) `transform._dedupe_rows`'s key gains a fourth, never-persisted discriminator (`Event Value`), and a row missing a required field is skipped-and-counted instead of raising and losing the whole window.

**Tech Stack:** Python 3.12, pydantic-settings, httpx, polars, pytest (TDD, `pytest-cov` branch coverage).

**Spec:** `docs/superpowers/plans/2026-08-13-baf-11-full-raw-export.md` (Этап 2 "Управляемый чанк и бюджет квоты", section "Дедупликация ломается на полном массиве...", and Q6 under "Вопросы Mark Malovichko") plus `docs/design-spec.md` (risk table, dedup key history). This plan implements the parts of that spec that don't require a schema change or a production cutover — see "Out of scope" below.

## Global Constraints

- Python 3.12, `uv`-managed (`uv sync`, `uv run ...`) — never bare `python`/`pip`.
- TDD: every step below writes the failing test before the implementation that makes it pass.
- Gates after every task: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (strict — `files=["src","tests"]`), `uv run pytest`. Run `uv run pre-commit run --all-files` before the final PR.
- Commit after each step that says "Commit" — small, reviewable commits, not one commit per task.
- Work happens on the already-created branch `baf-11-stage-2-quota-budget` (based on the merged BAF-11 stage-1 filters + the BAF-2 dedup-key fix). Do not touch `main`.
- **Ticket requirement #2 is binding:** `in-app-events` stays in the *same* table with the *same* 17-column schema (`loader._INSERT_COLUMNS`). The new dedupe discriminator (`Event Value`) must never be persisted or appear in `loader.load_events`'s input rows.
- Do not touch `loader.py`'s delete-then-insert transaction logic, `cli.py`, or `scripts/load_csv.py` — out of scope for this plan.
- Every new/changed log message that previously fed a test assertion must keep the exact substring the existing test matches on (see per-task notes) — this plan is additive, not a rewrite of working behavior.

## Out of scope (left for later BAF-11 stages, tracked in the spec doc)

- `ReportSpec` refactor (Этап 3), the `installs` report and its new table (Этапы 5-6), the `appsflyer_events_fb` PK/index migration (Этап 7), and the production cutover (Этап 9) are **not** part of this plan. This plan only makes the code safe to eventually run unfiltered — it does not flip the production filters off. `appsflyer_media_source`/`appsflyer_event_names` stay configured on the server exactly as they are today.

---

### Task 1: Configurable API chunk size (`APPSFLYER_CHUNK_DAYS`)

**Files:**
- Modify: `src/appsflyer_pipeline/config.py`
- Modify: `src/appsflyer_pipeline/pipeline.py:93-104` (`_iter_work_items`)
- Test: `tests/test_config.py`
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `appsflyer_client.MAX_CHUNK_DAYS` (existing constant, `= 31`), `appsflyer_client.chunk_date_range(start, end, max_days=...)` (existing function, unchanged signature).
- Produces: `Settings.appsflyer_chunk_days: int` (new field, default `31`, bounds `[1, 31]`) — later tasks/stages read this the same way `appsflyer_daily_lookback_days` is read today.

- [ ] **Step 1: Write the failing config tests**

Add to `tests/test_config.py`, right after `test_daily_lookback_out_of_bounds_rejected` (after line 177):

```python
def test_chunk_days_defaults_to_31(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _settings(monkeypatch).appsflyer_chunk_days == 31


def test_chunk_days_accepts_valid_value(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(monkeypatch, APPSFLYER_CHUNK_DAYS="10")
    assert settings.appsflyer_chunk_days == 10


@pytest.mark.parametrize("raw", ["0", "-3", "32", "not-a-number"])
def test_chunk_days_out_of_bounds_rejected(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    with pytest.raises(ValidationError):
        _settings(monkeypatch, APPSFLYER_CHUNK_DAYS=raw)
```

Also add `"APPSFLYER_CHUNK_DAYS"` to the `_OPTIONAL_ENV_KEYS` tuple at the top of `tests/test_config.py` (line 25-32), alongside `"APPSFLYER_DAILY_LOOKBACK_DAYS"` — same rationale in the existing comment: an ambient value from the developer's shell must not silently change what a test asserts.

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `uv run pytest tests/test_config.py -k chunk_days -v`
Expected: FAIL — `AttributeError` / `pydantic.errors` about an unknown/missing `appsflyer_chunk_days` field (the field doesn't exist yet).

- [ ] **Step 3: Add the field to `Settings`**

In `src/appsflyer_pipeline/config.py`, add this field right after `appsflyer_daily_lookback_days` (after line 94):

```python
    # Quota-aware chunk size (BAF-11 stage 2): the Pull API bills by call, not by
    # rows returned, so a wide chunk is strictly cheaper on quota than many
    # narrow ones (measured 2026-08-13: a 31-day chunk costs the same single
    # download as a 1-day one). The hard ceiling is AppsFlyer's own per-call
    # limit (appsflyer_client.MAX_CHUNK_DAYS); this only lets an operator go
    # narrower, e.g. to shrink one retry's blast radius.
    appsflyer_chunk_days: Annotated[int, Field(ge=1, le=31)] = 31
```

- [ ] **Step 4: Run the config tests to verify they pass**

Run: `uv run pytest tests/test_config.py -v`
Expected: PASS — all tests, including the three new ones and every pre-existing one.

- [ ] **Step 5: Commit**

```bash
git add src/appsflyer_pipeline/config.py tests/test_config.py
git commit -m "BAF-11 stage 2: add configurable APPSFLYER_CHUNK_DAYS"
```

- [ ] **Step 6: Write the failing pipeline test**

Add `"APPSFLYER_CHUNK_DAYS"` to `_OPTIONAL_ENV_KEYS` in `tests/test_pipeline.py` (line 44-51), same rationale as Step 1.

Add to `tests/test_pipeline.py`, right after `test_iter_work_items_yields_expected_matrix` (after line 147):

```python
def test_iter_work_items_respects_configured_chunk_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, APPSFLYER_CHUNK_DAYS="10")
    settings = get_settings()
    start = datetime.date(2026, 1, 1)
    end = datetime.date(2026, 1, 31)  # 31 days -> 4 chunks of <=10 days each

    items = list(_iter_work_items(settings, start, end))

    one_series = [(s, e) for a, t, s, e in items if a == "app1" and t == "non_organic"]
    assert all((e - s).days < 10 for s, e in one_series)
    assert len(one_series) == 4
```

- [ ] **Step 7: Run it to verify it fails**

Run: `uv run pytest tests/test_pipeline.py -k configured_chunk_days -v`
Expected: FAIL — chunks are still 31 days wide (the assertion `(e - s).days < 10` fails on the first, 31-day-wide chunk).

- [ ] **Step 8: Wire the setting through `_iter_work_items`**

In `src/appsflyer_pipeline/pipeline.py`, change line 103 from:

```python
            for chunk_start, chunk_end in chunk_date_range(start, end):
```

to:

```python
            for chunk_start, chunk_end in chunk_date_range(
                start, end, max_days=settings.appsflyer_chunk_days
            ):
```

- [ ] **Step 9: Run the pipeline tests to verify they pass**

Run: `uv run pytest tests/test_pipeline.py -v`
Expected: PASS — the new test, and `test_iter_work_items_yields_expected_matrix` (which never sets `APPSFLYER_CHUNK_DAYS`, so it keeps exercising the default-31 path).

- [ ] **Step 10: Commit**

```bash
git add src/appsflyer_pipeline/pipeline.py tests/test_pipeline.py
git commit -m "BAF-11 stage 2: thread APPSFLYER_CHUNK_DAYS into chunk_date_range"
```

---

### Task 2: Explicit `maximum_rows` + split-and-retry on truncation

**Files:**
- Modify: `src/appsflyer_pipeline/appsflyer_client.py:41-175` (`_fetch_csv`, `fetch_events`)
- Test: `tests/test_appsflyer_client.py`

**Interfaces:**
- Consumes: nothing new from other tasks.
- Produces: `appsflyer_client.DEFAULT_MAXIMUM_ROWS: int = 1_000_000` (new module constant); `fetch_events(..., maximum_rows: int = DEFAULT_MAXIMUM_ROWS)` (new keyword-only parameter, default preserves today's behavior for every existing caller in `pipeline.py`, which does not pass it).

**Context:** AppsFlyer's Pull API defaults to truncating a report at `maximum_rows=200,000` rows with **no error** — the response is a normal 200 with a full CSV, just short. A 31-day chunk on the unfiltered stream measures ~239k rows/day-equivalent (2026-08-13 probe), so it can exceed that default silently. Sending an explicit `maximum_rows=1000000` raises the ceiling; if a response still comes back at exactly that many rows, that is itself the truncation signal (AppsFlyer would have returned fewer if there were fewer), and the fix is to split the window in half and re-fetch each half — cheap because it only spends extra API calls on windows that actually hit the ceiling.

- [ ] **Step 1: Write the failing "sends maximum_rows" test**

Add to `tests/test_appsflyer_client.py`, right after `test_fetch_events_sends_expected_params_and_headers` (after line 107):

```python
@respx.mock
def test_fetch_events_sends_maximum_rows_by_default() -> None:
    """BAF-11 stage 2: AppsFlyer's own default truncates a report at 200,000
    rows with no error -- a normal 200 response, just short. An explicit
    maximum_rows raises that ceiling; fetch_events must always send it.
    """
    route = respx.get(_url("id123", "non_organic")).mock(
        return_value=httpx.Response(200, text=SAMPLE_CSV)
    )
    with httpx.Client() as client:
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
    assert route.calls.last.request.url.params["maximum_rows"] == "1000000"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_appsflyer_client.py -k sends_maximum_rows -v`
Expected: FAIL — `KeyError: 'maximum_rows'` (the param doesn't exist in the request yet).

- [ ] **Step 3: Write the failing split-and-retry test**

Add to `tests/test_appsflyer_client.py`, right after `test_fetch_events_raises_on_1m_row_cap`'s test function (after line ~400, the end of that test):

```python
TWO_ROW_CSV = (
    "Attributed Touch Time,Install Time,Event Time,Event Name,Event Revenue,"
    "Media Source,Campaign,AppsFlyer ID,Customer User ID\n"
    "2026-05-20 10:00:00,2026-05-19 09:00:00,2026-05-20 10:05:00,af_purchase,9.99,"
    "Facebook Ads,Summer Sale,af-id-1,user-1\n"
    "2026-05-20 10:00:00,2026-05-19 09:00:00,2026-05-20 10:05:00,af_purchase,9.99,"
    "Facebook Ads,Summer Sale,af-id-2,user-2\n"
)


@respx.mock
def test_fetch_events_splits_window_when_response_hits_maximum_rows() -> None:
    """BAF-11 stage 2: a response carrying exactly `maximum_rows` rows is
    treated as truncated, not complete. The 20-day window is split into two
    10-day halves; each half comes back under the cap and both are combined.
    """
    call_log: list[tuple[str, str]] = []

    def _responder(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        call_log.append((params["from"], params["to"]))
        if (params["from"], params["to"]) == ("2026-05-01", "2026-05-20"):
            return httpx.Response(200, text=TWO_ROW_CSV)  # == maximum_rows -> looks truncated
        return httpx.Response(200, text=SAMPLE_CSV)  # each half: 1 row, under the cap

    respx.get(_url("id123", "non_organic")).mock(side_effect=_responder)
    with httpx.Client() as client:
        df = fetch_events(
            client,
            app_id="id123",
            attribution_type="non_organic",
            from_date=datetime.date(2026, 5, 1),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source="Facebook Ads",
            event_names=["af_purchase"],
            maximum_rows=2,
        )

    assert df.shape[0] == 2
    assert call_log == [
        ("2026-05-01", "2026-05-20"),
        ("2026-05-01", "2026-05-10"),
        ("2026-05-11", "2026-05-20"),
    ]
```

- [ ] **Step 4: Run it to verify it fails**

Run: `uv run pytest tests/test_appsflyer_client.py -k splits_window -v`
Expected: FAIL — `TypeError: fetch_events() got an unexpected keyword argument 'maximum_rows'` (the parameter doesn't exist on `fetch_events` yet).

- [ ] **Step 5: Add the `maximum_rows` param to `_fetch_csv`**

In `src/appsflyer_pipeline/appsflyer_client.py`, add the module constant right after `MAX_CHUNK_DAYS = 31` (line 34):

```python
# BAF-11 stage 2: AppsFlyer's Pull API defaults to truncating a report at
# maximum_rows=200,000 with no error -- a normal 200 response, just short.
# A 31-day chunk on the unfiltered stream measures ~239k rows/day-equivalent
# (probed 2026-08-13), so it can silently exceed that default. This raises the
# ceiling to the client's own 1M-row hard cap (see fetch_events below).
DEFAULT_MAXIMUM_ROWS = 1_000_000
```

Change `_fetch_csv`'s signature (line 55-66) to add the parameter:

```python
def _fetch_csv(
    client: httpx.Client,
    *,
    app_id: str,
    attribution_type: AttributionType,
    from_date: datetime.date,
    to_date: datetime.date,
    api_token: str,
    media_source: str | None,
    event_names: list[str] | None,
    timezone: str | None = None,
    maximum_rows: int = DEFAULT_MAXIMUM_ROWS,
) -> bytes:
```

Change the `params` dict construction (line 69-72) to:

```python
    params: dict[str, str | int] = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "maximum_rows": maximum_rows,
    }
```

- [ ] **Step 6: Extract the single-call fetch-and-parse helper, wire in `maximum_rows`, and add the split-retry logic**

`fetch_events` (line 100-175) currently does one HTTP call, error handling, the empty-body check, the CSV parse, and the 1M-row-cap check, all in one function. Split it: everything up to and including the CSV parse becomes a private helper; the row-cap decision (raise vs. split-and-retry) moves to the public function so it can recurse.

Replace the entire body of `appsflyer_client.py` from `def fetch_events(` (line 100) through the end of that function (line 175, the `return df` line) with:

```python
def _fetch_and_parse(
    client: httpx.Client,
    *,
    app_id: str,
    attribution_type: AttributionType,
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
            attribution_type=attribution_type,
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
        return pl.read_csv(BytesIO(content), infer_schema_length=0)
    except (pl.exceptions.ComputeError, pl.exceptions.NoDataError) as exc:
        raise AppsFlyerAPIError(
            f"AppsFlyer returned an unparseable CSV [{attribution_type}] for {app_id} "
            f"({from_date} to {to_date}): {exc}"
        ) from exc


def fetch_events(
    client: httpx.Client,
    *,
    app_id: str,
    attribution_type: AttributionType,
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
        attribution_type=attribution_type,
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
            f"Report for {app_id} [{attribution_type}] {from_date}..{to_date} hit the "
            f"{maximum_rows}-row cap on a single day — data is likely truncated and the "
            f"window cannot be split any further."
        )

    mid = from_date + (to_date - from_date) // 2
    first_half = fetch_events(
        client,
        app_id=app_id,
        attribution_type=attribution_type,
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
        attribution_type=attribution_type,
        from_date=mid + datetime.timedelta(days=1),
        to_date=to_date,
        api_token=api_token,
        media_source=media_source,
        event_names=event_names,
        timezone=timezone,
        maximum_rows=maximum_rows,
    )
    return pl.concat([first_half, second_half])
```

- [ ] **Step 7: Fix the pre-existing 1M-cap test's message assertion**

`test_fetch_events_raises_on_1m_row_cap` (existing, around line 385) calls `fetch_events` with `from_date == to_date` and no explicit `maximum_rows`, so it takes the new "cannot be split any further" branch — still raises `AppsFlyerAPIError`, but the message changed from the old hardcoded `"...1M-row cap..."` to the new general-purpose `f"...hit the {maximum_rows}-row cap..."`, which for the default `maximum_rows=1_000_000` renders as `"hit the 1000000-row cap"`. Update the test's `pytest.raises` match:

```python
    with httpx.Client() as client, pytest.raises(AppsFlyerAPIError, match="1000000-row cap"):
```

- [ ] **Step 8: Run the full client test file**

Run: `uv run pytest tests/test_appsflyer_client.py -v`
Expected: PASS — every test in the file, including both new tests from Steps 1 and 3, and the updated 1M-cap message test from Step 7.

- [ ] **Step 9: Commit**

```bash
git add src/appsflyer_pipeline/appsflyer_client.py tests/test_appsflyer_client.py
git commit -m "BAF-11 stage 2: send explicit maximum_rows, split-and-retry on truncation"
```

---

### Task 3: Full-array dedupe discriminator + skip-not-raise on missing required fields

**Files:**
- Modify: `src/appsflyer_pipeline/transform.py`
- Modify: `docs/design-spec.md` (one risk-table row)
- Test: `tests/test_transform.py`
- Test: `tests/test_pipeline.py` (fixture only, no new test)
- Test: `tests/test_cli.py` (fixture only, no new test)

**Interfaces:**
- Consumes: nothing new from Tasks 1-2.
- Produces: no change to `transform_events`'s public signature or return shape (still `list[dict[str, Any]]` with exactly the `loader._INSERT_COLUMNS` keys) — both changes are internal to how rows are deduped and filtered before that return.

**Context — why both fixes belong in one task:** they touch the exact same per-row loop in `transform_events`, and both were identified from the same production measurement (2026-08-13 probe of the unfiltered stream): a real conflicting-row bug (`_dedupe_rows` silently drops a legitimately distinct row) and a fragility bug (one bad row raises and loses 31 days of good ones). Both must land before the full-array export can safely go live (BAF-11 spec, Q6 / Этап 8).

**Part A — three test fixtures need a new raw column.** `transform_events` will start requiring the raw `"Event Value"` column (present in every real AppsFlyer response, just currently unmapped). Three test fixtures build fake CSVs/DataFrames by hand and must add it *before* touching `transform.py`, or every test that exercises the real pipeline (not just `test_transform.py`) breaks the moment the new required-column check lands.

- [ ] **Step 1: Add `Event Value` to `tests/test_transform.py`'s raw-row fixture**

In `tests/test_transform.py`, add `"Event Value"` to the `RAW_COLUMNS` list (line 14-33) and to `_raw_row`'s default dict (line 36-58):

```python
RAW_COLUMNS = [
    "Attributed Touch Type",
    "Attributed Touch Time",
    "Install Time",
    "Event Time",
    "Event Name",
    "Event Value",
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
        "Event Value": "af-value-1",
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
```

- [ ] **Step 2: Add `Event Value` to `SAMPLE_CSV` in `tests/test_pipeline.py`**

In `tests/test_pipeline.py`, replace the `SAMPLE_CSV` constant (line 18-24) with:

```python
SAMPLE_CSV = (
    "Attributed Touch Time,Install Time,Event Time,Event Name,Event Value,Event Revenue,"
    "Media Source,Channel,Campaign,Campaign ID,Adset,Adset ID,Ad,Ad ID,"
    "AppsFlyer ID,Customer User ID,Is Primary Attribution\n"
    "2026-05-20 10:00:00,2026-05-19 09:00:00,2026-05-20 10:05:00,af_purchase,af-value-1,9.99,"
    "Facebook Ads,Social,Summer Sale,cmp-1,Adset A,adset-1,Ad A,ad-1,af-id-1,user-1,true\n"
)
```

- [ ] **Step 3: Add `Event Value` to `SAMPLE_CSV` in `tests/test_cli.py`**

In `tests/test_cli.py`, replace the `SAMPLE_CSV` constant (line 35-41) with the identical replacement from Step 2 (same column added in the same position).

- [ ] **Step 4: Run the full suite to confirm these are pure fixture changes so far**

Run: `uv run pytest -q`
Expected: PASS — no behavior changed yet, only fixtures grew an unused extra column (polars/transform.py ignore extra raw columns that aren't in `_COLUMN_MAP`).

- [ ] **Step 5: Commit**

```bash
git add tests/test_transform.py tests/test_pipeline.py tests/test_cli.py
git commit -m "BAF-11 stage 2: add Event Value to test fixtures ahead of the dedupe fix"
```

**Part B — the dedupe-key fix.**

- [ ] **Step 6: Write the failing test for the false-conflict bug**

Add to `tests/test_transform.py`, right after `test_transform_keeps_only_the_latest_install_time_on_conflict` (after line 475):

```python
def test_transform_keeps_both_rows_when_event_value_differs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """BAF-11 stage 2: on the full unfiltered stream, two rows can share
    (event_time, event_name, appsflyer_id) and still be genuinely distinct
    events -- e.g. two screen_plans_cInternet impressions from one device in
    the same second with a different Event Value (measured live 2026-08-13: 5
    such rows in a single one-day, one-app conflict group). These must NOT
    collapse into one survivor the way a real conflict does.
    """
    df = _df(
        [
            _raw_row(**{"Event Value": "plan_1gb"}),
            _raw_row(**{"Event Value": "plan_5gb"}),
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
    assert not any(
        "dropped" in r.message or "collapsed" in r.message for r in caplog.records
    )
    assert "Event Value" not in rows[0]
    assert "event_value" not in rows[0]
```

- [ ] **Step 7: Run it to verify it fails**

Run: `uv run pytest tests/test_transform.py -k event_value_differs -v`
Expected: FAIL — `len(rows) == 1` today (the two rows share the old 3-column key and one is dropped as a conflict).

- [ ] **Step 8: Add the discriminator constants**

In `src/appsflyer_pipeline/transform.py`, add right after `_REQUIRED_NOT_NULL = (...)` (line 45):

```python
# Raw-only column read for dedup, never persisted (BAF-11 stage 2, Q6): on the
# unfiltered full-array stream, two real events can share
# (event_time, event_name, appsflyer_id) to the second and still be genuinely
# different rows -- e.g. two screen_plans_cInternet events from one device in
# one second with a different Event Value (measured live 2026-08-13: 5 such
# rows in one conflict group). Folding this into the dedupe key stops those
# from resolving to a single survivor. It is deliberately NOT added to
# _COLUMN_MAP: ticket requirement #2 keeps in-app-events on the same 17-column
# table, so this value is stripped back off every row before transform_events
# returns (see the `del` in transform_events below).
_DEDUPE_DISCRIMINATOR_RAW_COLUMN = "Event Value"
_DEDUPE_DISCRIMINATOR_ROW_KEY = "__dedupe_event_value"
```

- [ ] **Step 9: Update `_dedupe_rows`'s key and docstring**

In `src/appsflyer_pipeline/transform.py`, change the docstring's first line (line 86-87) from:

```python
    """Keep exactly ONE row per (event_time, event_name, appsflyer_id) key: the
    one with the latest `install_time`.
```

to:

```python
    """Keep exactly ONE row per (event_time, event_name, appsflyer_id,
    Event Value) key: the one with the latest `install_time`.
```

Add this paragraph right after the existing "Conflict handling changed on 2026-08-14..." paragraph (after line 105, before "The known cost, logged loudly:"):

```python

    Event Value joined the key on BAF-11 stage 2 (2026-08-25): on the
    unfiltered full-array stream, rows sharing the original 3-column key can
    be genuinely distinct events rather than conflicting duplicates -- see
    test_transform_keeps_both_rows_when_event_value_differs. The known
    production conflict this function was built for (two purchases of
    different amounts in the same second, distinguished by `Event Revenue`,
    NOT `Event Value`) is unaffected by this change and still resolves via
    the install_time tiebreak described below.
```

Change the key construction (line 117, 126) from:

```python
    slot_of_key: dict[tuple[Any, Any, Any], int] = {}
```

to:

```python
    slot_of_key: dict[tuple[Any, Any, Any, Any], int] = {}
```

and from:

```python
        key = (row["event_time"], row["event_name"], row["appsflyer_id"])
```

to:

```python
        key = (
            row["event_time"],
            row["event_name"],
            row["appsflyer_id"],
            row[_DEDUPE_DISCRIMINATOR_ROW_KEY],
        )
```

- [ ] **Step 10: Read the discriminator in `transform_events` and strip it before returning**

In `src/appsflyer_pipeline/transform.py`, change the missing-columns check (line 211) from:

```python
    missing = [raw for raw in _COLUMN_MAP if raw not in df.columns]
```

to:

```python
    missing = [
        raw
        for raw in (*_COLUMN_MAP, _DEDUPE_DISCRIMINATOR_RAW_COLUMN)
        if raw not in df.columns
    ]
```

Change the row-building loop's `select(...)` call and the line that constructs `row` (line 241-242) from:

```python
    for raw_row in filtered.select(list(_COLUMN_MAP)).iter_rows(named=True):
        row: dict[str, Any] = {target: raw_row[raw] for raw, target in _COLUMN_MAP.items()}
```

to:

```python
    select_columns = [*_COLUMN_MAP, _DEDUPE_DISCRIMINATOR_RAW_COLUMN]
    for raw_row in filtered.select(select_columns).iter_rows(named=True):
        row: dict[str, Any] = {target: raw_row[raw] for raw, target in _COLUMN_MAP.items()}
        row[_DEDUPE_DISCRIMINATOR_ROW_KEY] = raw_row[_DEDUPE_DISCRIMINATOR_RAW_COLUMN]
```

(Part C, next, edits the required-field check that sits between this and the `rows.append(row)` line — do not run the tests yet, Step 12 covers both parts together.)

Change the final return (line 255) from:

```python
    return _dedupe_rows(rows, attribution_type=attribution_type, app_id=app_id)
```

to:

```python
    deduped = _dedupe_rows(rows, attribution_type=attribution_type, app_id=app_id)
    for row in deduped:
        del row[_DEDUPE_DISCRIMINATOR_ROW_KEY]
    return deduped
```

**Part C — the skip-not-raise fix.**

- [ ] **Step 11: Write the failing test for the skip-not-raise behavior, and replace the old raise-based test**

In `tests/test_transform.py`, replace `test_transform_raises_on_blank_required_field` (line 235-244) entirely with:

```python
def test_transform_skips_rows_missing_a_required_field(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """BAF-11 stage 2: a row missing a required field is skipped and counted
    instead of failing the entire window. The previous raise-and-lose-the-
    window behavior only ever suited the narrow, pre-filtered FB-purchase
    stream; on the full unfiltered array a privacy-restricted or malformed row
    is more likely, and one bad row must not cost 31 days of good ones.
    """
    df = _df([_raw_row(**{"AppsFlyer ID": ""}), _raw_row()])
    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.transform"):
        rows = transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase", "af_purchase_YC"],
        )
    assert len(rows) == 1
    assert rows[0]["appsflyer_id"] == "af-id-1"
    messages = " ".join(r.message for r in caplog.records)
    assert "skipped 1 row" in messages
    assert "appsflyer_id" in messages
```

- [ ] **Step 12: Run the new test to verify it fails**

Run: `uv run pytest tests/test_transform.py -k skips_rows_missing -v`
Expected: FAIL — `TransformError` is raised for the blank `AppsFlyer ID` row instead of it being skipped. (`event_value_differs`, from Part B, should already PASS at this point — Steps 6-10 finished that fix before this step.)

- [ ] **Step 13: Replace the raise with skip-and-count**

In `src/appsflyer_pipeline/transform.py`, change the required-field check and the lines around it (originally line 249-253, now shifted a few lines down by Step 10's edits — find the block that reads `for required in _REQUIRED_NOT_NULL:`) from:

```python
        for required in _REQUIRED_NOT_NULL:
            if not row[required]:
                raise TransformError(f"Row has NULL/blank required field {required!r}: {row}")

        rows.append(row)
```

to:

```python
        if any(not row[required] for required in _REQUIRED_NOT_NULL):
            skipped_missing_required += 1
            continue

        rows.append(row)
```

Add the counter initialization right before the `for raw_row in filtered.select(...)` loop (the line added in Step 10):

```python
    skipped_missing_required = 0
    select_columns = [*_COLUMN_MAP, _DEDUPE_DISCRIMINATOR_RAW_COLUMN]
```

Add the summary warning right after the loop ends, before the `deduped = _dedupe_rows(...)` line:

```python
    if skipped_missing_required:
        logger.warning(
            "skipped %d row(s) missing a required field (%s): attribution_type=%s app_id=%s",
            skipped_missing_required,
            ", ".join(_REQUIRED_NOT_NULL),
            attribution_type,
            app_id,
        )

```

- [ ] **Step 14: Run the full transform test file**

Run: `uv run pytest tests/test_transform.py -v`
Expected: PASS — every test, including both new ones. In particular, re-check `test_transform_raises_on_unparseable_revenue` and `test_transform_raises_on_unparseable_timestamp` still PASS unmodified (those raise from `_parse_revenue`/`_parse_timestamp`, called before the required-field check — untouched by this change).

- [ ] **Step 15: Run the full test suite**

Run: `uv run pytest -q`
Expected: PASS — 98%+ branch coverage, no regressions in `test_pipeline.py` or `test_cli.py` (both only needed the Part A fixture change).

- [ ] **Step 16: Update the stale risk-table row in `docs/design-spec.md`**

Find the risk-table row starting `| **Two distinct purchases inside one second by one user**` (around line 142). Append this sentence to the end of that cell's text, right after `...cleaned by \`sql/migrations/2026-08-14-dedupe-keep-latest-install-time.sql\`.`:

```
 **BAF-11 stage 2 (2026-08-25):** the dedupe key also includes `Event Value`, which resolves a related but distinct false-conflict — two genuinely different events (e.g. two `screen_plans_cInternet` impressions in the same second) that used to collapse into one row on the unfiltered stream. This specific purchase-revenue conflict is unaffected, since `Event Value` differs from `Event Revenue` and does not distinguish these two rows; it still resolves via the install_time tiebreak above.
```

- [ ] **Step 17: Run mypy and ruff**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy`
Expected: all clean.

- [ ] **Step 18: Commit**

```bash
git add src/appsflyer_pipeline/transform.py tests/test_transform.py docs/design-spec.md
git commit -m "BAF-11 stage 2: dedupe key discriminator + skip-not-raise on missing required fields"
```

---

## Final check before opening a PR

- [ ] Run `uv run pre-commit run --all-files` — must be clean (this is what CI gates on).
- [ ] Run `uv run pytest --cov-fail-under=98` — must pass (CI's real threshold; local `pytest` alone doesn't enforce it).
- [ ] Confirm `git log --oneline baf-11-stage-1-optional-filters..HEAD` shows only the commits from this plan's tasks (no stray changes).
- [ ] Push the branch and open a PR titled `BAF-11 stage 2: quota-aware chunking + full-array dedupe/NULL hardening`, base `main`, following the existing `baf-11-stage-1-optional-filters` PR (#57) as a style reference.
