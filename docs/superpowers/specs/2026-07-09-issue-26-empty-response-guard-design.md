# Design: fail loud on empty/anomalous AppsFlyer responses instead of wiping the window

**Date:** 2026-07-09
**Issue:** [#26](https://github.com/baubek-yesim/appsflyer-to-analytics-db/issues/26) — the P1 from
the 2026-07-09 full-repo review.
**Status:** approved in brainstorming, ready for implementation planning

## Goals

1. Make it impossible for an anomalous-but-HTTP-200 AppsFlyer response — a truly empty body, a
   one-line error text, or header drift with zero rows — to be classified as "legitimately no
   events". Today that classification flows into `load_events`' delete-then-insert-nothing and
   erases the window's previously loaded rows at **exit 0** (no failed window, no OnFailure alert;
   only #10's WARNING).
2. Keep the one legitimate empty shape working untouched: a headers-only CSV carrying the expected
   columns. Live-probed 2026-07-09 (`scratch/probe_issue26.py`, gitignored): a genuinely quiet
   window returns exactly this — HTTP 200, `text/csv`, all 81 columns, UTF-8 BOM prefix, ~1.1 KB,
   0 data rows. `com.yesimmobile`/`retargeting` is quiet **every day** in production, so any
   design that false-fails on legit empties would break the scheduled run nightly.

## Non-goals

- Issues #28 (retention-floor warning anchor) and #29 (empty scalar config values) — tracked
  separately. #29's empty-`APPSFLYER_MEDIA_SOURCE` wipe shares the *outcome* but not the
  mechanism (its rows are dropped by the media-source filter, with valid headers present).
- A loader-level wipe guard (brainstorming approach C) — rejected: it would block the one
  legitimate wipe #10 deliberately preserved (AppsFlyer genuinely revising a window to zero
  events), and the residual case it protects against — a well-formed, expected-header, 0-row CSV
  that is nevertheless *wrong* — is indistinguishable from a legit empty response even in
  principle.
- Client-side header validation via an injected required-columns parameter (approach A) —
  rejected: it duplicates column knowledge that `transform.py` already owns and couples the client
  to the transform layer for the same end result.
- Any change to non-empty response handling — the existing schema guard already raises there.
- Honoring/probing AppsFlyer `maximum_rows`, retention-floor semantics for straddling windows,
  etc. — out of scope.

## Context

The wipe path, as shipped today:

- `appsflyer_client.fetch_events` (`appsflyer_client.py:130-137`): an empty body short-circuits to
  `return pl.DataFrame()`; any one-line body parses (with `infer_schema_length=0`) to a
  `(0, N)` frame whose "columns" are the error text.
- `transform.transform_events` (`transform.py:134-135`): `if df.is_empty(): return []` runs
  **before** the required-columns guard at `transform.py:137-145`, so 0-row frames of any shape
  bypass schema validation entirely.
- `loader.load_events` (`loader.py:188-199`): `rows=[]` still executes the window DELETE —
  correct for a *validated* empty window (that is the #10-sanctioned wipe), destructive for an
  anomalous one.

Empirically verified shapes (2026-07-09):

| Input | `pl.read_csv(..., infer_schema_length=0)` result | transform today |
|---|---|---|
| `b""` (empty body) | short-circuited to `pl.DataFrame()` before parse | `[]` → wipe |
| Real quiet window (200) | `(0, 81)` frame, expected headers, BOM stripped by polars | `[]` → *correct* |
| One-line error text (200) | `(0, 1)` frame, error string as column name | `[]` → wipe |
| Headers-only with renamed column | `(0, N)` frame, wrong headers | `[]` → wipe |
| Two-line error text | `(1, N)` frame | already caught (missing columns) |
| `b""` fed to `read_csv` directly | raises `polars.exceptions.NoDataError` — **not** `ComputeError` | would escape #11's except and crash the whole run |

Also probed: a fully-beyond-retention request returns **HTTP 400** with an explicit
"availability window is limited to 90 days" message (recorded on #28 and relevant to the BAF-2
retention question) — so known API error modes are non-200; this design defends against the
*unknown* 200-shaped ones and header drift.

Amplification: once #8's `APPSFLYER_DAILY_LOOKBACK_DAYS=3` is enabled, every scheduled run
rewrites two previously-loaded days per combo — one anomalous response would wipe them at exit 0.
This fix is a prerequisite for that enablement.

## Design

### Part 1 — `fetch_events` raises on an empty body (`src/appsflyer_pipeline/appsflyer_client.py`)

- Replace the `if not content.strip(): return pl.DataFrame()` short-circuit with
  `raise AppsFlyerAPIError(...)` naming app_id, attribution_type, and the window, and stating the
  invariant: *a legitimate empty report always includes CSV headers (live-verified 2026-07-09,
  issue #26)*. Message follows the existing `f"AppsFlyer ... [{attribution_type}] for {app_id}
  ({from_date} to {to_date})"` style.
- Widen the CSV-parse guard from `except pl.exceptions.ComputeError` to
  `except (pl.exceptions.ComputeError, pl.exceptions.NoDataError)`: polars raises `NoDataError`
  on empty-ish input (verified, polars 1.42.1). Today that exception isn't in
  `_process_window`'s catch list, so an empty-ish body slipping past `bytes.strip()` (e.g. a
  BOM-only body — the BOM is not ASCII whitespace) would crash the **entire run** rather than
  failing one window.

### Part 2 — `transform_events` validates headers before the empty early-return (`src/appsflyer_pipeline/transform.py`)

- Move the `missing = [raw for raw in required_raw if raw not in df.columns]` check (and the
  `required_raw` construction) **above** `if df.is_empty(): return []`.
- Resulting semantics:
  - 0-row frame **with** all required columns → `[]` (the one legitimate empty; the downstream
    delete+insert-0 and #10's wipe WARNING are unchanged).
  - 0-row frame **missing** any required column — schema drift, or a one-line error text parsed
    as `(0, 1)` — → `TransformError` with the existing "missing expected column(s)" message.
  - Retargeting still does not require `Is Primary Attribution` (existing semantics preserved).
- A 0-column `pl.DataFrame()` can no longer arrive via the real client path (Part 1 raises
  first), but the reorder makes transform self-sufficient for direct callers/tests too: it raises
  on every required column being missing.

### Failure routing (unchanged machinery, new inputs)

Both new failures surface as per-window errors through `_process_window`'s existing isolation:
the window fails, **no DELETE runs**, the CLI prints a `FAIL` line, the run exits 1, systemd marks
the unit failed, and the `OnFailure=` alert fires. Other windows proceed.

### Ops/docs

- `docs/RUNBOOK.md` §11: one new troubleshooting row — `AppsFlyerAPIError: ... empty response
  body` / `TransformError: ... missing expected column(s)` on a 0-row response → AppsFlyer-side
  anomaly or export schema drift; the window's previously loaded data is **preserved**;
  investigate (compare headers against `transform._COLUMN_MAP`), then re-run just that window.
- No systemd unit changes. Server pickup is a standard §13 redeploy after merge.

## Tests

- `tests/test_appsflyer_client.py`:
  - **Flip** `test_fetch_events_empty_body_returns_empty_dataframe` →
    `test_fetch_events_raises_on_empty_body` (respx 200 + empty/whitespace-only bodies,
    parametrized; expect `AppsFlyerAPIError` matching "empty response body"). Comment cites the
    probe result so the behavior change is self-documenting.
- `tests/test_transform.py`:
  - New legit-empty test using the **real 81-column header line captured by the probe, BOM
    included, as bytes** run through `pl.read_csv(..., infer_schema_length=0)` (mirrors the
    production parse; pins polars' BOM handling) → `transform_events` returns `[]` for both
    attribution types.
  - Parametrized anomalous 0-row cases → `TransformError` matching "missing expected column":
    one-line error text (parses to `(0, 1)`), headers-only with a renamed required column, and a
    literal `pl.DataFrame()`.
- `tests/test_pipeline.py`:
  - One respx test in the existing isolation-test pattern: error-text 200 for one combo, valid
    CSV for the rest → exactly that window fails (`TransformError` in `WindowResult.error`),
    others succeed, `load_events` never called for the failed one.
- Existing #10 wipe-WARNING tests (`tests/test_loader_integration.py`) are untouched — the
  legitimate-wipe path still exists and still warns.
- Coverage: all new branches exercised; the CI `--cov-fail-under=98` gate is unaffected.

## Delivery

- Branch `issue-26-empty-response-guard`, worked in an isolated worktree per the established
  flow; this spec and its implementation plan ride with it.
- Merge only on the user's explicit authorization, per established workflow. The merge commit
  message carries `Fixes #26` (auto-closes on push to main, per repo convention — single issue,
  so the one-keyword-per-issue rule is moot).
- Server deploy after merge via RUNBOOK §13 (`git pull && uv sync --frozen --no-dev`; code-only
  change), gated on its own explicit user go-ahead (mixed-automation pattern: production-touching
  actions stay with the controller).
- Live verification: the next scheduled 05:00 fire — `com.yesimmobile`/`retargeting` exercises
  the legit-empty path nightly at **zero extra quota**; the anomalous paths cannot be provoked
  against the real API on demand and are pinned by the respx tests instead.
- Project-board discipline: issue #26 → "In Progress" when the branch starts, "Done" on close
  (board setup itself is pending the `project` token scope).
