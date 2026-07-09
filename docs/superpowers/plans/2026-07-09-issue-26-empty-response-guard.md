# Issue #26 Empty-Response Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An anomalous-but-HTTP-200 AppsFlyer response (truly empty body, error text, header drift with zero rows) fails its window loudly instead of being classified as "no events" and wiping the window's previously loaded rows at exit 0.

**Architecture:** Two surgical changes where the knowledge already lives (spec: `docs/superpowers/specs/2026-07-09-issue-26-empty-response-guard-design.md`, approach B): `fetch_events` raises on a truly-empty body and additionally catches `NoDataError` from the CSV parse; `transform_events` runs its required-columns check *before* the `is_empty()` early-return so only a schema-valid empty yields `[]`. Failure routing reuses `_process_window`'s existing per-window isolation — no loader or pipeline source changes.

**Tech Stack:** Python 3.12, polars (CSV parse, `infer_schema_length=0`), httpx + respx (tests), pytest.

## Global Constraints

- Branch: `issue-26-empty-response-guard` (worktree created at execution time via superpowers:using-git-worktrees).
- No new dependencies; no changes to `src/appsflyer_pipeline/loader.py` or `src/appsflyer_pipeline/pipeline.py` source (tests may change).
- Gates that must stay green after every task: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (strict), `uv run pytest`. Final task additionally runs `uv run pre-commit run --all-files` and `uv run pytest --cov-fail-under=98` — these are the agreed merge gates (no remote-CI wait; CI runs post-hoc on push to main).
- Ruff line length 100.
- Task commits reference the issue as `(#26)` (bare reference — the closing keyword `Fixes #26` is reserved for the final merge commit).
- Every commit message ends with: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- Live-verified facts the code comments cite (do not re-probe; costs API quota): a genuinely quiet window returns HTTP 200 with a headers-only, 81-column, UTF-8-BOM-prefixed CSV (2026-07-09, `scratch/probe_issue26.py`); `pl.read_csv` raises `NoDataError` (not `ComputeError`) on empty-ish input incl. BOM-only bytes (polars 1.42.1).

---

### Task 1: `fetch_events` fails loud on an empty response body

**Files:**
- Modify: `src/appsflyer_pipeline/appsflyer_client.py:130-142`
- Test: `tests/test_appsflyer_client.py` (replaces `test_fetch_events_empty_body_returns_empty_dataframe`, lines 140-154)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `fetch_events(...)` raises `AppsFlyerAPIError` whose message contains `"empty response body"` for empty/whitespace-only bodies, and `"unparseable CSV"` for `NoDataError`-shaped bodies (BOM-only). Task 3's pipeline test relies on empty-body fetches never returning a DataFrame.

- [ ] **Step 1: Replace the empty-body test with the two failing tests**

Delete `test_fetch_events_empty_body_returns_empty_dataframe` (tests/test_appsflyer_client.py:140-154) and add in its place:

```python
@respx.mock
@pytest.mark.parametrize("body", ["", "   \n  "])
def test_fetch_events_raises_on_empty_body(body: str) -> None:
    """Issue #26: a legitimate empty report always includes CSV headers
    (live-verified 2026-07-09: a quiet window returns a headers-only,
    81-column CSV). A truly empty/whitespace body must fail the window --
    preserving its previously loaded rows -- instead of reading as
    'no events' and wiping them via delete-then-insert-nothing.
    """
    respx.get(_url("id123", "non_organic")).mock(return_value=httpx.Response(200, text=body))
    with httpx.Client() as client, pytest.raises(AppsFlyerAPIError, match="empty response body"):
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


@respx.mock
def test_fetch_events_raises_on_bom_only_body() -> None:
    """A BOM-only body slips past bytes.strip() (the BOM is not ASCII
    whitespace) and makes polars raise NoDataError -- which is NOT a
    ComputeError subclass, so before issue #26 it escaped the parse guard
    and crashed the whole run instead of failing one window.
    """
    respx.get(_url("id123", "non_organic")).mock(
        return_value=httpx.Response(200, content=b"\xef\xbb\xbf")
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
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `uv run pytest tests/test_appsflyer_client.py -k "empty_body or bom_only" -v`
Expected: 3 FAILs — the two `empty_body` params fail with `Failed: DID NOT RAISE` (current code returns an empty DataFrame); `bom_only` fails with `polars.exceptions.NoDataError: empty CSV` (escapes the current `except pl.exceptions.ComputeError`).

- [ ] **Step 3: Implement the client change**

In `src/appsflyer_pipeline/appsflyer_client.py`, replace lines 130-131:

```python
    if not content.strip():
        return pl.DataFrame()
```

with:

```python
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
```

and widen the parse guard (currently `except pl.exceptions.ComputeError as exc:`) to:

```python
    except (pl.exceptions.ComputeError, pl.exceptions.NoDataError) as exc:
```

(`NoDataError` is what polars raises for empty-ish input such as a BOM-only body — it is not a `ComputeError` subclass, and uncaught it would crash the whole run rather than fail one window.)

- [ ] **Step 4: Run the client test file to verify everything passes**

Run: `uv run pytest tests/test_appsflyer_client.py -v`
Expected: all tests PASS (the flipped tests plus every pre-existing client test — redirects, retries, 1M-cap, malformed CSV — unchanged).

- [ ] **Step 5: Run lint/type gates and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy`
Expected: all clean.

```bash
git add src/appsflyer_pipeline/appsflyer_client.py tests/test_appsflyer_client.py
git commit -m "Fail loud on an empty AppsFlyer response body (#26)

A truly empty 200 body now raises AppsFlyerAPIError instead of returning an
empty DataFrame that flows into a window wipe; the CSV parse guard also
catches polars NoDataError (BOM-only bodies), which is not a ComputeError
and previously crashed the whole run.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: `transform_events` validates headers before the empty early-return

**Files:**
- Modify: `src/appsflyer_pipeline/transform.py:134-145`
- Test: `tests/test_transform.py`

**Interfaces:**
- Consumes: nothing from Task 1 (independent change; both route through `_process_window`'s existing except clause).
- Produces: `transform_events(df, ...)` returns `[]` only for 0-row frames containing every required raw column; raises `TransformError` matching `"missing expected column"` for any 0-row frame with missing/unknown headers (including a 0-column `pl.DataFrame()`). Task 3's pipeline test relies on the error-text-200 shape raising this way.

- [ ] **Step 1: Write the failing tests**

First add two imports to the top of `tests/test_transform.py`: `from io import BytesIO` (stdlib group) and `from appsflyer_pipeline.appsflyer_client import AttributionType` (imported from its home module, NOT through `transform` — reaching through another module's un-exported import trips `mypy --strict` implicit-reexport, the exact lesson from the #15/#17 batch). Ruff's isort hook will verify placement.

Then add after `test_transform_empty_dataframe_returns_empty_list` (which stays — it covers a 0-row frame *with* the expected columns via `_df([])`):

```python
# A headers-only export body, as the real API returns for a genuinely quiet
# window (live-verified 2026-07-09: HTTP 200, UTF-8 BOM, all 81 columns,
# zero data rows). Reconstructed here with every required raw column plus a
# sample of the real export's extra columns -- the full 81-column line adds
# no test signal and re-capturing it costs an API-quota report download.
HEADERS_ONLY_EXPORT = b"\xef\xbb\xbf" + (
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
) + b"\n"


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
def test_transform_raises_on_zero_row_frame_with_missing_columns(df: pl.DataFrame) -> None:
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
```

Note: `HEADERS_ONLY_EXPORT.replace(b"Event Time", b"Event Time Renamed")` also renames the
`Attributed Touch Time` substring? No — `Attributed Touch Time` does not contain `Event Time`;
the only match is the exact `Event Time` column (byte-substring `Event Time` also appears in no
other header in the list). The renamed frame is therefore missing exactly `Event Time`.

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `uv run pytest tests/test_transform.py -k "headers_only or zero_row" -v`
Expected: the two `headers_only` params PASS already (0-row-with-valid-headers yields `[]` via the current early-return — they pin the invariant); all three `zero_row` params FAIL with `Failed: DID NOT RAISE` (current code returns `[]` for every empty frame).

- [ ] **Step 3: Implement the reorder**

In `src/appsflyer_pipeline/transform.py`, replace lines 134-145:

```python
    if df.is_empty():
        return []

    required_raw = list(_COLUMN_MAP)
    if attribution_type == "non_organic":
        required_raw.append(_PRIMARY_ATTRIBUTION_COLUMN)
    missing = [raw for raw in required_raw if raw not in df.columns]
    if missing:
        raise TransformError(
            f"AppsFlyer response is missing expected column(s): {missing} "
            f"(attribution_type={attribution_type}, app_id={app_id})"
        )
```

with:

```python
    required_raw = list(_COLUMN_MAP)
    if attribution_type == "non_organic":
        required_raw.append(_PRIMARY_ATTRIBUTION_COLUMN)
    missing = [raw for raw in required_raw if raw not in df.columns]
    if missing:
        raise TransformError(
            f"AppsFlyer response is missing expected column(s): {missing} "
            f"(attribution_type={attribution_type}, app_id={app_id})"
        )

    # Issue #26: this early-return must stay BELOW the column check. Only a
    # schema-valid empty (expected headers, zero rows -- the shape a genuinely
    # quiet window returns, live-verified 2026-07-09) may yield []; an
    # error-text body or a drifted header set parses to a 0-row frame too,
    # and returning [] for those would wipe the window downstream at exit 0.
    if df.is_empty():
        return []
```

- [ ] **Step 4: Run the transform test file to verify everything passes**

Run: `uv run pytest tests/test_transform.py -v`
Expected: all tests PASS — including the pre-existing `test_transform_empty_dataframe_returns_empty_list` (its `_df([])` frame carries all `RAW_COLUMNS`, so it now exercises the schema-valid-empty path) and `test_transform_raises_on_missing_required_raw_column` (unchanged behavior, now triggered one block earlier).

- [ ] **Step 5: Run lint/type gates and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy`
Expected: all clean.

```bash
git add src/appsflyer_pipeline/transform.py tests/test_transform.py
git commit -m "Validate expected columns before the empty early-return (#26)

A 0-row frame now yields [] only when the required raw columns are present
(the shape a genuinely quiet window returns). Error-text 200 bodies and
drifted header sets raise TransformError and fail their window instead of
wiping it at exit 0.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Pipeline-level routing test, RUNBOOK row, final gates

**Files:**
- Test: `tests/test_pipeline.py`
- Modify: `docs/RUNBOOK.md` (§11 troubleshooting table, after the quota row at line ~294)

**Interfaces:**
- Consumes: Task 1's `AppsFlyerAPIError` on empty bodies; Task 2's `TransformError` matching `"missing expected column"` for error-text 200 bodies.
- Produces: nothing further — this task proves the routing end-to-end and documents the operator side.

- [ ] **Step 1: Add the pipeline isolation test**

Add to `tests/test_pipeline.py` (after `test_run_daily_isolates_transform_error`, same pattern):

```python
@respx.mock
def test_run_daily_isolates_error_text_200_body(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """Issue #26 end-to-end: an HTTP 200 whose body is a one-line error string
    (parses to a 0-row, 1-column frame) fails exactly its own window via
    TransformError -- it must never reach load_events, where rows=[] would
    delete the window's existing data and report success.
    """
    _set_env(monkeypatch)
    respx.get(_url("app1", "non_organic")).mock(
        return_value=httpx.Response(200, text="Subscription package limitation. Contact your CSM")
    )
    respx.get(_url("app1", "retargeting")).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))
    respx.get(_url("app2", "non_organic")).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))
    respx.get(_url("app2", "retargeting")).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))

    summary = run_daily(date=datetime.date(2026, 5, 20))

    assert not summary.all_succeeded
    assert len(summary.failed) == 1
    failed = summary.failed[0]
    assert failed.app_id == "app1"
    assert failed.attribution_type == "non_organic"
    assert failed.error is not None
    assert "TransformError" in failed.error
    assert "missing expected column" in failed.error
    assert len(summary.succeeded) == 3
    assert len(load_spy) == 3  # the failed unit never reaches load_events
```

- [ ] **Step 2: Run it — expected to PASS immediately**

Run: `uv run pytest tests/test_pipeline.py::test_run_daily_isolates_error_text_200_body -v`
Expected: PASS. This is deliberate — Tasks 1-2 changed no routing, so this test documents that `_process_window`'s existing isolation handles the new failure shape with zero pipeline changes. If it FAILS, stop: Task 2's reorder is wrong, do not patch the pipeline.

- [ ] **Step 3: Add the RUNBOOK troubleshooting row**

In `docs/RUNBOOK.md` §11, insert a new table row directly after the quota-exhaustion row (the row beginning `| AppsFlyerAPIError: HTTP 400 "...maximum number of in-app event reports...`):

```markdown
| `AppsFlyerAPIError: ... empty response body` or `TransformError: ... missing expected column(s)` on a window that used to load fine | AppsFlyer sent an anomalous 200 (truly empty or error-text body), or the export's header set drifted — a legitimate empty report always carries the full CSV header row (issue #26, live-verified 2026-07-09) | Nothing was deleted — the window's previously loaded rows are intact. Re-run just that window with `--dry-run` to inspect; if AppsFlyer renamed columns, update `transform._COLUMN_MAP`; otherwise re-run the window once the upstream anomaly clears. |
```

- [ ] **Step 4: Run the full merge gates**

Run: `uv run pre-commit run --all-files`
Expected: all hooks pass (ruff, ruff-format, mypy).

Run: `uv run pytest --cov-fail-under=98`
Expected: all tests pass; branch coverage ≥ 98% (the new branches are all exercised).

- [ ] **Step 5: Commit**

```bash
git add tests/test_pipeline.py docs/RUNBOOK.md
git commit -m "Pin per-window isolation of error-text 200 bodies; document the new failure rows (#26)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Deviations from spec (deliberate, minor)

- The legit-empty test fixture reconstructs the headers-only body (BOM + all required columns + a
  sample of real extra columns) rather than embedding the verbatim 81-column line: the probe
  captured only the first 200 bytes, and re-capturing the full line would cost an API-quota report
  download for zero additional test signal (the guard checks `required ⊆ columns`, not the exact set).

## After the plan

- Merge: only on the user's explicit authorization; merge commit message carries `Fixes #26`.
- Server deploy (RUNBOOK §13) and the next-scheduled-fire observation are post-merge steps gated
  on their own explicit user go-ahead — not part of this plan's tasks.
- Board: move issue #26 to "In Progress" when the branch starts (pending `project` token scope).
