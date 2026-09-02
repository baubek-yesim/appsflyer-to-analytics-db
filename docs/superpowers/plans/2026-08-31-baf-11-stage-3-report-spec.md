# BAF-11 Stage 3: ReportSpec Refactor (No Behavior Change) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce `ReportSpec` (Этап 3 of the BAF-11 spec) as a new module,
`src/appsflyer_pipeline/reports.py`, with a `REPORTS` registry covering exactly the two report
types that exist today (in-app-events non_organic + retargeting), and thread it through
`appsflyer_client.py` / `transform.py` / `loader.py` / `pipeline.py` / `cli.py` /
`scripts/load_csv.py` **without changing any observable behavior**. This is scaffolding for BAF-11
stages 5-6 (the `installs` report and its own table) — it does not add installs, does not add a
second table, and does not change what any existing command prints, requests, or writes.

**Architecture:** One new module (`reports.py`) that owns, as data, everything that today is
scattered across five files as a mix of module-level constants (`appsflyer_client._ENDPOINT_BY_ATTRIBUTION`,
`transform._COLUMN_MAP`/`_TIMESTAMP_COLUMNS`/`_REQUIRED_NOT_NULL`, `loader._INSERT_COLUMNS`) and a
tuple constant (`pipeline.ATTRIBUTION_TYPES`). Every one of those call sites starts taking a
`ReportSpec` instead, sourced from the new `REPORTS` registry. `_dedupe_rows` (the install-time
conflict resolution added in BAF-2/BAF-11 stage 2) is deliberately **not** touched — dedup-key
policy is Этап 8b's problem, not this stage's.

**Tech Stack:** Python 3.12, pydantic-settings, httpx, polars, SQLAlchemy+PyMySQL, pytest (TDD,
`pytest-cov` branch coverage).

**Spec:** `docs/superpowers/plans/2026-08-13-baf-11-full-raw-export.md`, section "Предлагаемая
архитектура" (`ReportSpec` shape, per-layer changes) and "Этап 3. ReportSpec (рефакторинг без
изменения поведения)". **That document's file:line references are stale** (stages 1-2 changed line
numbers throughout `config.py`/`appsflyer_client.py`/`transform.py`/`pipeline.py`) — every
`Files:`/`Interfaces:` block below cites line numbers read live from this worktree on 2026-08-31,
not the master spec's numbers.

## Global Constraints

- Python 3.12, `uv`-managed (`uv sync`, `uv run ...`) — never bare `python`/`pip`.
- Gates after every task: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy`
  (strict — `files=["src","tests"]`), `uv run pytest`. Run `uv run pre-commit run --all-files`
  before the final PR.
- Commit after each step that says "Commit" — small, reviewable commits, not one commit per task.
- Work happens on a new branch `baf-11-stage-3-report-spec`, based on current `main` (stage 1 PR
  #57 and stage 2 PR #59 already merged). Do not touch `main` directly.
- **This is a pure refactor.** The acceptance bar (from the master spec's Этап 3 criterion): every
  existing test stays green, and a `git diff` of the test suite touches **only call
  signatures/kwargs** — never an expected SQL string, HTTP param, log substring, or row value. Where
  a step below changes a test file, it says exactly which lines and why; if a step doesn't mention a
  test file, that file needs no changes for this task.
- **TDD framing for a signature refactor, not a feature:** there is no new behavior to drive out
  with a failing test. Instead, each production-code step is followed by "run the affected test
  file, confirm it now fails" (characterization: the existing tests ARE the safety net) and then a
  test-migration step that makes it pass again with the **same** assertions, new call syntax only.
  Treat a red run after a signature change as expected, not a bug — the bug would be a *different*
  kind of failure (an assertion value changing, or a test staying green when it shouldn't).
- `AGENTS.md`/`CLAUDE.md`'s "Build stages" tracking table update is out of scope for this plan
  (matches stage-2 precedent) — whoever merges this stage's PR adds the entry.

## Architecture decisions made concretely for this stage (resolving the master spec's open points)

The master spec's proposed `ReportSpec` (section "Предлагаемая архитектура") lists more fields than
this stage wires up. Decided here, against the actual current code:

1. **`ReportSpec` field set.** Included: `name`, `endpoint`, `attribution_type`, `sends_event_name`,
   `sends_media_source`, `additional_fields`, `retention_days`, `column_map`, `timestamp_columns`,
   `required_not_null`, `table`, `insert_columns`, `window_column`. **Omitted, deliberately:**
   `max_chunk_days` (today there is exactly one global, operator-configurable chunk size —
   `Settings.appsflyer_chunk_days` — and both registered specs share it; a per-spec override has no
   current consumer and would just be dead data until a report needs a different ceiling),
   `decimal_columns` (today exactly one column, `event_revenue`, is Decimal-parsed by a single
   explicit call to `_parse_revenue` — generalizing to a list is speculative and unexercised),
   `dedupe_key`/full dedupe parameterization (explicitly Этап 8b's scope per this plan's Out-of-scope
   section — `_dedupe_rows` keeps its current hardcoded 4-column key, including the
   never-persisted `Event Value` discriminator), and `partition_columns` as a generalized DELETE-predicate
   builder (both registered specs partition identically on `app_id` + `attribution_type`; only
   `window_column` — the time-bound column, `"event_time"` today — is threaded through, replacing the
   one hardcoded column name that genuinely varies by report per the master spec's own note
   ("installs" will window on `install_time`, not `event_time`)).
2. **`WindowResult.attribution_type` stays `AttributionType` (not widened to `str`).** The master
   spec flagged this as a maybe ("может потребоваться расширить"). Resolved: no widening. Both
   registered `ReportSpec`s' `attribution_type` values are within the existing
   `Literal["non_organic", "retargeting"]` — BAF-11 decision #4 confirms `installs` reuses the same
   two attribution types, it doesn't add new ones — so there is no value that needs to escape the
   Literal in this stage. Widening now would just turn off mypy's exhaustiveness checking for no
   present benefit; revisit only if a future report genuinely introduces a third attribution-type
   value.
3. **Circular import.** `reports.py` needs `AttributionType`/`MAX_RETENTION_DAYS` from
   `appsflyer_client.py` (real import). `appsflyer_client.py` and `transform.py` need the `ReportSpec`
   *type* for their function signatures — importing it for real would cycle back to `reports.py`.
   Both already carry `from __future__ import annotations` (confirmed: `appsflyer_client.py:11`,
   `transform.py:8`), so annotations are strings at runtime — the standard fix is a
   `TYPE_CHECKING`-guarded import (`if TYPE_CHECKING: from appsflyer_pipeline.reports import
   ReportSpec`), which mypy resolves but Python never executes, so there is no real cycle.
   `loader.py` needs `ReportSpec` too, but nothing in `reports.py` needs anything from `loader.py`,
   so that import is a plain, unconditional one.
4. **`_iter_work_items` yields `(spec, app_id, chunk_start, chunk_end)`**, replacing
   `(app_id, attribution_type, chunk_start, chunk_end)`. The nested-loop order stays **app_id outer,
   report inner** (unchanged from today's app_id-outer/attribution_type-inner) rather than switching
   to the master spec's prose order ("report × app_id × чанк") — preserving the exact prior
   enumeration order is strictly safer for a "no behavior change" stage and costs nothing, since
   nothing about the two REPORTS entries requires reordering to work.
5. **`cli.py` check-connection/create-table and `pipeline._run_window`'s preflight** generalize from
   "the one table, `settings.db_table`" to "every distinct table any registered `ReportSpec` points
   at" (`{spec.table(settings) for spec in REPORTS.values()}`, deduplicated via a `set` since both
   specs share one table today). This is deliberately **not** paired with the master spec's proposed
   `--report` CLI flag (needed for targeted per-report runs at cutover/quota-triage time) — that flag
   has no use until a second report/table exists, so it is out of scope here (Этап 5).
6. **Retention floor.** `pipeline._warn_if_before_retention_floor` takes an explicit
   `retention_days: int` instead of reading the module-level `MAX_RETENTION_DAYS` import. Callers
   (`run_backfill`, `run_daily`) resolve it via a new `_active_retention_days()` helper —
   `min(spec.retention_days for spec in REPORTS.values())`. Both registered specs set
   `retention_days=MAX_RETENTION_DAYS` (90), so `_active_retention_days()` returns exactly 90 today —
   byte-identical to the current behavior — while the *shape* is now per-report, ready for a future
   60-day `installs` spec to correctly narrow it via `min(...)` without a further signature change.

## Out of scope (left for later BAF-11 stages, tracked in the spec doc)

- The `installs`/`installs-retarget` report definitions and their new table (Этапы 5-6) — this is
  the **next** plan, not this one. `REPORTS` in this plan contains exactly the two report types that
  already exist.
- The `--report` CLI flag for targeted per-report runs (part of Этап 5, needs a second report to be
  meaningful).
- Throughput/observability work — batched INSERT, streaming transform, the systemd timeout, a real
  alert (Этап 4).
- The production schema migration for `appsflyer_events_fb` (Этап 7).
- The exact-duplicate-collapsing / dedupe-key policy question — `_dedupe_rows` and its 4-column key
  (including the `Event Value` discriminator) are untouched (Этап 8b).
- Cutover (Этап 9) and the production filter flip (media_source/event_names still default to
  BAF-2's filtered behavior on the real server; this plan doesn't touch `.env`/deploy templates).
- `Settings.appsflyer_chunk_days` staying a single global setting (not per-report) — see decision 1
  above.

---

### Task 1: `reports.py` module + `REPORTS` registry

**Files:**
- Create: `src/appsflyer_pipeline/reports.py`
- Create: `tests/test_reports.py`

**Interfaces:**
- Consumes: `appsflyer_pipeline.appsflyer_client.AttributionType` (`appsflyer_client.py:22`),
  `appsflyer_pipeline.appsflyer_client.MAX_RETENTION_DAYS` (`appsflyer_client.py:34`),
  `appsflyer_pipeline.config.Settings` (`config.py:41`).
- Produces: `ReportSpec` (frozen dataclass), `REPORTS: dict[str, ReportSpec]` with keys
  `"in_app_events_non_organic"` / `"in_app_events_retargeting"`. Nothing else imports from
  `reports.py` yet — this task is self-contained.

- [ ] **Step 1: Write `reports.py`**

Create `src/appsflyer_pipeline/reports.py`:

```python
"""ReportSpec: one AppsFlyer report, as data (BAF-11 stage 3).

Pure refactor -- REPORTS holds only the two report definitions that already
exist (in-app-events non_organic + retargeting); nothing about what any
existing command requests, transforms, or writes changes. Structurally
separates "which report" (endpoint, request params, column mapping, target
table) from the rest of the pipeline so a second report (installs, BAF-11
stage 5/6) can be added by registering a new ReportSpec instead of threading
a new branch through appsflyer_client.py/transform.py/loader.py/pipeline.py.

See docs/superpowers/plans/2026-08-31-baf-11-stage-3-report-spec.md's
"Architecture decisions" section for which fields of the master spec's
proposed ReportSpec shape this stage deliberately does NOT wire up yet
(max_chunk_days, decimal_columns, dedupe_key, partition_columns) and why.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from appsflyer_pipeline.appsflyer_client import MAX_RETENTION_DAYS, AttributionType
from appsflyer_pipeline.config import Settings


@dataclass(frozen=True)
class ReportSpec:
    """One AppsFlyer report as data: request shape, column mapping, target table."""

    name: str
    endpoint: str
    attribution_type: AttributionType
    sends_event_name: bool
    sends_media_source: bool
    additional_fields: tuple[str, ...]
    retention_days: int
    column_map: Mapping[str, str]
    timestamp_columns: tuple[str, ...]
    required_not_null: tuple[str, ...]
    # A callable, not a settings attribute name string: getattr(settings, name)
    # would type-check as Any under mypy strict and silently swallow a typo.
    table: Callable[[Settings], str]
    insert_columns: tuple[str, ...]
    window_column: str


def _in_app_events_table(settings: Settings) -> str:
    return settings.db_table


# Raw AppsFlyer column -> target table column. Moved here from transform.py
# (BAF-11 stage 3): ReportSpec, not the transform module, now owns "which
# columns this report maps" so a future all-fields report (installs,
# column_map treated as "map everything") can differ per spec instead of per
# module constant. Values confirmed byte-identical to the pre-refactor
# transform._COLUMN_MAP by this task's Step 3.
_IN_APP_EVENTS_COLUMN_MAP: dict[str, str] = {
    "Event Time": "event_time",
    "Install Time": "install_time",
    "Attributed Touch Time": "attributed_touch_time",
    "Event Name": "event_name",
    "Event Revenue": "event_revenue",
    "Media Source": "media_source",
    "Channel": "channel",
    "Campaign": "campaign",
    "Campaign ID": "campaign_id",
    "Adset": "adset",
    "Adset ID": "adset_id",
    "Ad": "ad",
    "Ad ID": "ad_id",
    "AppsFlyer ID": "appsflyer_id",
    "Customer User ID": "customer_user_id",
}
_IN_APP_EVENTS_TIMESTAMP_COLUMNS = ("event_time", "install_time", "attributed_touch_time")
_IN_APP_EVENTS_REQUIRED_NOT_NULL = ("event_time", "event_name", "appsflyer_id")

# Column order for INSERT -- moved here from loader._INSERT_COLUMNS (BAF-11
# stage 3). Must match the keys transform.transform_events() produces for
# this spec; pinned by test_reports.py's
# test_insert_columns_match_transform_events_output_keys_for_every_report.
_IN_APP_EVENTS_INSERT_COLUMNS = (
    "event_time",
    "install_time",
    "attributed_touch_time",
    "event_name",
    "event_revenue",
    "media_source",
    "channel",
    "campaign",
    "campaign_id",
    "adset",
    "adset_id",
    "ad",
    "ad_id",
    "appsflyer_id",
    "customer_user_id",
    "attribution_type",
    "app_id",
)

REPORTS: dict[str, ReportSpec] = {
    "in_app_events_non_organic": ReportSpec(
        name="in_app_events",
        endpoint="in_app_events_report",
        attribution_type="non_organic",
        sends_event_name=True,
        sends_media_source=True,
        additional_fields=(),
        retention_days=MAX_RETENTION_DAYS,
        column_map=_IN_APP_EVENTS_COLUMN_MAP,
        timestamp_columns=_IN_APP_EVENTS_TIMESTAMP_COLUMNS,
        required_not_null=_IN_APP_EVENTS_REQUIRED_NOT_NULL,
        table=_in_app_events_table,
        insert_columns=_IN_APP_EVENTS_INSERT_COLUMNS,
        window_column="event_time",
    ),
    "in_app_events_retargeting": ReportSpec(
        name="in_app_events",
        endpoint="in-app-events-retarget",
        attribution_type="retargeting",
        sends_event_name=True,
        sends_media_source=True,
        additional_fields=(),
        retention_days=MAX_RETENTION_DAYS,
        column_map=_IN_APP_EVENTS_COLUMN_MAP,
        timestamp_columns=_IN_APP_EVENTS_TIMESTAMP_COLUMNS,
        required_not_null=_IN_APP_EVENTS_REQUIRED_NOT_NULL,
        table=_in_app_events_table,
        insert_columns=_IN_APP_EVENTS_INSERT_COLUMNS,
        window_column="event_time",
    ),
}
```

- [ ] **Step 2: Write `tests/test_reports.py`'s registry-shape tests**

Create `tests/test_reports.py`:

```python
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
```

- [ ] **Step 3: Run the tests, and cross-check the copied constants against the modules they will replace**

Run: `uv run pytest tests/test_reports.py -v`
Expected: PASS — all 6 tests (the registry is self-contained; nothing else needed touching yet).

Then, before trusting the copied `_IN_APP_EVENTS_COLUMN_MAP`/`_IN_APP_EVENTS_INSERT_COLUMNS` by eye,
verify them against the modules Task 3/4 will remove these constants from (both still exist,
untouched, at this point):

```bash
uv run python -c "
from appsflyer_pipeline.reports import REPORTS
from appsflyer_pipeline.transform import _COLUMN_MAP
from appsflyer_pipeline.loader import _INSERT_COLUMNS

spec = REPORTS['in_app_events_non_organic']
assert dict(spec.column_map) == _COLUMN_MAP, 'column_map drifted from transform._COLUMN_MAP'
assert spec.insert_columns == _INSERT_COLUMNS, 'insert_columns drifted from loader._INSERT_COLUMNS'
assert spec.timestamp_columns == ('event_time', 'install_time', 'attributed_touch_time')
assert spec.required_not_null == ('event_time', 'event_name', 'appsflyer_id')
print('OK: reports.py constants match the modules they will replace')
"
```

Expected output: `OK: reports.py constants match the modules they will replace`. If any `assert`
fires, fix the typo in `reports.py` before continuing — do not fix it by changing the module being
compared against.

- [ ] **Step 4: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
Expected: all green.

```bash
git add src/appsflyer_pipeline/reports.py tests/test_reports.py
git commit -m "BAF-11 stage 3: add ReportSpec + REPORTS registry (in-app-events only)"
```

---

### Task 2: Thread `ReportSpec` through `appsflyer_client.py`

**Files:**
- Modify: `src/appsflyer_pipeline/appsflyer_client.py:11-27,65-279`
- Modify: `tests/test_appsflyer_client.py`

**Interfaces:**
- Consumes: `appsflyer_pipeline.reports.ReportSpec` (`TYPE_CHECKING`-only import — see Architecture
  decision 3), `REPORTS` (in tests only).
- Produces: `_fetch_csv`/`_fetch_and_parse`/`fetch_events` all replace their `attribution_type:
  AttributionType` keyword-only parameter with `spec: ReportSpec`; `_ENDPOINT_BY_ATTRIBUTION`
  (`appsflyer_client.py:24-27`) is deleted (endpoint now comes from `spec.endpoint`). `AttributionType`
  itself (`appsflyer_client.py:22`) is untouched — `reports.py` and `transform.py` still import it.

- [ ] **Step 1: Add the `TYPE_CHECKING` import and drop `_ENDPOINT_BY_ATTRIBUTION`**

In `src/appsflyer_pipeline/appsflyer_client.py`, change line 16 from:

```python
from typing import Literal
```

to:

```python
from typing import TYPE_CHECKING, Literal
```

and add, right after the `AttributionType` definition (after line 22):

```python
if TYPE_CHECKING:
    from appsflyer_pipeline.reports import ReportSpec
```

Delete `_ENDPOINT_BY_ATTRIBUTION` entirely (lines 24-27):

```python
_ENDPOINT_BY_ATTRIBUTION: dict[AttributionType, str] = {
    "non_organic": "in_app_events_report",
    "retargeting": "in-app-events-retarget",
}
```

- [ ] **Step 2: Change `_fetch_csv`'s signature and endpoint lookup**

In `_fetch_csv` (currently lines 65-109), replace the `attribution_type: AttributionType,`
parameter with `spec: ReportSpec,` (keep its position in the keyword-only arg list), and change the
body's endpoint/params construction from:

```python
    endpoint = _ENDPOINT_BY_ATTRIBUTION[attribution_type]
    url = f"{_BASE_URL}/{app_id}/{endpoint}/v5"
    params: dict[str, str | int] = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "maximum_rows": maximum_rows,
    }
    # BAF-11 stage 1: None means "no server-side filter", which has to be an
    # ABSENT param. Sending `media_source=` (empty) instead would be a filter
    # matching nothing, and AppsFlyer answers that with a valid header-only
    # report -- indistinguishable downstream from a genuinely quiet window, so
    # the idempotent delete-then-insert would wipe it (issue #45's shape).
    if event_names is not None:
        params["event_name"] = ",".join(event_names)
    if media_source is not None:
        params["media_source"] = media_source
    if timezone is not None:
        # Issue #53: without this param AppsFlyer reports in UTC; with it, event
        # times and the from/to day boundaries follow the app's configured zone.
        params["timezone"] = timezone
```

to:

```python
    url = f"{_BASE_URL}/{app_id}/{spec.endpoint}/v5"
    params: dict[str, str | int] = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "maximum_rows": maximum_rows,
    }
    # BAF-11 stage 1: None means "no server-side filter", which has to be an
    # ABSENT param. Sending `media_source=` (empty) instead would be a filter
    # matching nothing, and AppsFlyer answers that with a valid header-only
    # report -- indistinguishable downstream from a genuinely quiet window, so
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
```

- [ ] **Step 3: Change `_fetch_and_parse`'s signature and error messages**

In `_fetch_and_parse` (currently lines 112-173), replace `attribution_type: AttributionType,` with
`spec: ReportSpec,`, pass `spec=spec` into `_fetch_csv` instead of `attribution_type=attribution_type`,
and replace every `{attribution_type}` in an f-string with `{spec.attribution_type}` (three call
sites: the `HTTPStatusError` message, the `TransportError` message, the empty-body message, and the
unparseable-CSV message — four total). The resulting message text is byte-identical for both
registered specs since `spec.attribution_type` holds exactly the value `attribution_type` used to
hold.

- [ ] **Step 4: Change `fetch_events`'s signature, recursive calls, and log/error messages**

In `fetch_events` (currently lines 176-279), replace `attribution_type: AttributionType,` with
`spec: ReportSpec,`. Update:
- The call to `_fetch_and_parse`: `spec=spec` instead of `attribution_type=attribution_type`.
- The single-day-cap error message (currently line 230): `[{attribution_type}]` -> `[{spec.attribution_type}]`.
- The bisection warning (currently lines 236-248): the `%s` positional arg `attribution_type` ->
  `spec.attribution_type`.
- Both recursive `fetch_events(...)` calls (currently lines 249-260 and 261-272): `spec=spec` instead
  of `attribution_type=attribution_type`.
- The concat-failure error message (currently line 277): `[{attribution_type}]` -> `[{spec.attribution_type}]`.

- [ ] **Step 5: Run the client tests, confirm the expected breakage**

Run: `uv run pytest tests/test_appsflyer_client.py -v 2>&1 | tail -30`
Expected: FAIL — every test calling `fetch_events(...)` or `_fetch_csv.retry_with(...)(...)` with
`attribution_type=...` raises `TypeError: fetch_events() got an unexpected keyword argument
'attribution_type'` (or the equivalent for `_fetch_csv`). This is the expected characterization
break, not a regression.

- [ ] **Step 6: Migrate one call site by hand (worked example)**

In `tests/test_appsflyer_client.py`, add the import at the top (after the existing
`appsflyer_pipeline.appsflyer_client` imports):

```python
from appsflyer_pipeline.reports import REPORTS
```

Then change `test_fetch_events_parses_csv` (currently lines 38-56) from:

```python
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
```

to:

```python
        df = fetch_events(
            client,
            app_id="id123",
            spec=REPORTS["in_app_events_non_organic"],
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source="Facebook Ads",
            event_names=["af_purchase", "af_purchase_YC"],
        )
```

Run: `uv run pytest tests/test_appsflyer_client.py::test_fetch_events_parses_csv -v`
Expected: PASS.

- [ ] **Step 7: Migrate the remaining literal-string call sites mechanically**

The same substitution applies to every other `fetch_events(...)`/`_fetch_csv.retry_with(...)(...)`
call site that passes a literal `attribution_type="non_organic"` or `attribution_type="retargeting"`
(16 more sites — confirmed by `grep -c 'attribution_type=' tests/test_appsflyer_client.py` returning
17 before this step, 1 of which Step 6 already migrated by hand):

```bash
cd /path/to/repo
sed -i '' 's/attribution_type="non_organic",/spec=REPORTS["in_app_events_non_organic"],/' tests/test_appsflyer_client.py
sed -i '' 's/attribution_type="retargeting",/spec=REPORTS["in_app_events_retargeting"],/' tests/test_appsflyer_client.py
```

Run: `grep -n 'attribution_type=' tests/test_appsflyer_client.py`
Expected: exactly one remaining hit — `attribution_type=attribution_type,` inside
`test_fetch_events_never_sends_additional_fields` (the loop-variable case the sed patterns can't
match, handled next).

- [ ] **Step 8: Fix the one loop-variable call site by hand**

In `test_fetch_events_never_sends_additional_fields` (currently lines 190-217), change:

```python
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
```

to:

```python
    with httpx.Client() as client:
        for spec in REPORTS.values():
            fetch_events(
                client,
                app_id="id123",
                spec=spec,
                from_date=datetime.date(2026, 5, 20),
                to_date=datetime.date(2026, 5, 20),
                api_token="token",
                media_source="Facebook Ads",
                event_names=["af_purchase"],
            )
```

`AttributionType` is now unused in this file (it was only referenced by the deleted
`attribution_types` annotation — confirmed by `grep -n AttributionType
tests/test_appsflyer_client.py` showing exactly the import line and that one annotation). Remove it
from the import block at the top (currently lines 13-20):

```python
from appsflyer_pipeline.appsflyer_client import (
    AppsFlyerAPIError,
    AttributionType,
    _fetch_csv,
    _is_retryable,
    chunk_date_range,
    fetch_events,
)
```

to:

```python
from appsflyer_pipeline.appsflyer_client import (
    AppsFlyerAPIError,
    _fetch_csv,
    _is_retryable,
    chunk_date_range,
    fetch_events,
)
```

- [ ] **Step 9: Run the full client test file, confirm green**

Run: `uv run pytest tests/test_appsflyer_client.py -v`
Expected: PASS — all tests, same count as before this task, same assertion values (only `spec=`
replaced `attribution_type=` in call sites).

- [ ] **Step 10: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
Expected: all green.

```bash
git add src/appsflyer_pipeline/appsflyer_client.py tests/test_appsflyer_client.py
git commit -m "BAF-11 stage 3: thread ReportSpec through appsflyer_client.py"
```

---

### Task 3: Thread `ReportSpec` through `transform.py`

**Files:**
- Modify: `src/appsflyer_pipeline/transform.py:1-45,211-299`
- Modify: `tests/test_transform.py`
- Modify: `tests/test_reports.py` (Step 8 adds the master-spec-mandated cross-check test and a new
  `import polars as pl`)

**Interfaces:**
- Consumes: `appsflyer_pipeline.reports.ReportSpec` (`TYPE_CHECKING`-only import).
- Produces: `transform_events` replaces `attribution_type: AttributionType` with `spec: ReportSpec`;
  module constants `_COLUMN_MAP` (`transform.py:26-42`), `_TIMESTAMP_COLUMNS` (`transform.py:44`),
  `_REQUIRED_NOT_NULL` (`transform.py:45`) are deleted (now `spec.column_map`/`spec.timestamp_columns`/
  `spec.required_not_null`). `_dedupe_rows`, `_install_time_rank`, `_DEDUPE_DISCRIMINATOR_RAW_COLUMN`,
  `_DEDUPE_DISCRIMINATOR_ROW_KEY`, `_parse_timestamp`, `_parse_revenue`, `TransformError` are
  untouched — dedupe policy is out of scope (see Architecture decisions).

- [ ] **Step 1: Add the `TYPE_CHECKING` import, delete the three moved constants**

In `src/appsflyer_pipeline/transform.py`, change line 13 from:

```python
from typing import Any
```

to:

```python
from typing import TYPE_CHECKING, Any
```

and add, after the `logger = logging.getLogger(__name__)` line (line 19):

```python
if TYPE_CHECKING:
    from appsflyer_pipeline.reports import ReportSpec
```

Delete `_COLUMN_MAP` (lines 26-42, including its docstring comment at lines 23-25), `_TIMESTAMP_COLUMNS`
(line 44), and `_REQUIRED_NOT_NULL` (line 45). Leave `_TIMESTAMP_FORMAT` (line 21),
`_DEDUPE_DISCRIMINATOR_RAW_COLUMN`/`_DEDUPE_DISCRIMINATOR_ROW_KEY` (lines 57-58, with their comment
47-56), and everything from `class TransformError` (line 61) through `_dedupe_rows` (ending line 208)
exactly as they are.

- [ ] **Step 2: Change `transform_events`'s signature and body**

Change the signature (currently lines 211-218) from:

```python
def transform_events(
    df: pl.DataFrame,
    *,
    attribution_type: AttributionType,
    app_id: str,
    media_source_filter: str | None,
    event_names_filter: list[str] | None,
) -> list[dict[str, Any]]:
```

to:

```python
def transform_events(
    df: pl.DataFrame,
    *,
    spec: ReportSpec,
    app_id: str,
    media_source_filter: str | None,
    event_names_filter: list[str] | None,
) -> list[dict[str, Any]]:
```

Inside the body (currently lines 238-299), replace every use of the module constants and the old
parameter with the spec-derived equivalents. The two existing inline comments — "Issue #26: this
early-return must stay BELOW the column check..." (currently lines 247-251, explaining why the
empty-check's position is safety-critical) and "BAF-11 stage 1: an unset filter drops its predicate
entirely..." (currently lines 255-258, explaining why organic rows' empty Media Source matters) —
are **preserved as-is**, immediately above the `if df.is_empty():` and `predicates: list[pl.Expr] =
[]` lines respectively, exactly as they appear in the quoted block below:

```python
    attribution_type = spec.attribution_type
    column_map = spec.column_map
    missing = [
        raw for raw in (*column_map, _DEDUPE_DISCRIMINATOR_RAW_COLUMN) if raw not in df.columns
    ]
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

    # BAF-11 stage 1: an unset filter drops its predicate entirely rather than
    # comparing against None. This is not cosmetic — organic rows carry an EMPTY
    # Media Source, so any predicate built from a missing filter would discard
    # exactly the rows the full raw export exists to capture.
    predicates: list[pl.Expr] = []
    if media_source_filter is not None:
        predicates.append(pl.col("Media Source").eq(media_source_filter))
    if event_names_filter is not None:
        predicates.append(pl.col("Event Name").is_in(event_names_filter))

    filtered = df
    for predicate in predicates:
        filtered = filtered.filter(predicate)

    rows: list[dict[str, Any]] = []
    skipped_missing_required = 0
    select_columns = [*column_map, _DEDUPE_DISCRIMINATOR_RAW_COLUMN]
    for raw_row in filtered.select(select_columns).iter_rows(named=True):
        row: dict[str, Any] = {target: raw_row[raw] for raw, target in column_map.items()}
        row[_DEDUPE_DISCRIMINATOR_ROW_KEY] = raw_row[_DEDUPE_DISCRIMINATOR_RAW_COLUMN]
        for ts_col in spec.timestamp_columns:
            row[ts_col] = _parse_timestamp(row[ts_col])
        row["event_revenue"] = _parse_revenue(row["event_revenue"])
        row["attribution_type"] = attribution_type
        row["app_id"] = app_id

        if any(not row[required] for required in spec.required_not_null):
            skipped_missing_required += 1
            continue

        rows.append(row)

    if skipped_missing_required:
        logger.warning(
            "skipped %d row(s) missing a required field (%s): attribution_type=%s app_id=%s",
            skipped_missing_required,
            ", ".join(spec.required_not_null),
            attribution_type,
            app_id,
        )

    deduped = _dedupe_rows(rows, attribution_type=attribution_type, app_id=app_id)
    for row in deduped:
        del row[_DEDUPE_DISCRIMINATOR_ROW_KEY]
    return deduped
```

The docstring (currently lines 219-237) is unaffected content-wise; only its `attribution_type`
cross-reference needs no change since the parameter is gone and the prose already talks about
`_dedupe_rows`'s key, not the parameter name.

- [ ] **Step 3: Run the transform tests, confirm the expected breakage**

Run: `uv run pytest tests/test_transform.py -v 2>&1 | tail -20`
Expected: FAIL — every call to `transform_events(...)` passing `attribution_type=...` raises
`TypeError: transform_events() got an unexpected keyword argument 'attribution_type'`.

- [ ] **Step 4: Migrate one call site by hand (worked example)**

In `tests/test_transform.py`, add the import at the top (after the existing
`appsflyer_pipeline.transform` import):

```python
from appsflyer_pipeline.reports import REPORTS
```

Change `test_transform_maps_columns_and_adds_attribution_app_id` (currently lines 68-89) from:

```python
    rows = transform_events(
        df,
        attribution_type="non_organic",
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase", "af_purchase_YC"],
    )
```

to:

```python
    rows = transform_events(
        df,
        spec=REPORTS["in_app_events_non_organic"],
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase", "af_purchase_YC"],
    )
```

Run: `uv run pytest tests/test_transform.py::test_transform_maps_columns_and_adds_attribution_app_id -v`
Expected: PASS — including the unchanged assertion `assert row["attribution_type"] == "non_organic"`.

- [ ] **Step 5: Migrate the remaining literal-string call sites mechanically**

```bash
sed -i '' 's/attribution_type="non_organic",/spec=REPORTS["in_app_events_non_organic"],/' tests/test_transform.py
sed -i '' 's/attribution_type="retargeting",/spec=REPORTS["in_app_events_retargeting"],/' tests/test_transform.py
```

Run: `grep -n 'attribution_type=' tests/test_transform.py`
Expected: exactly 2 remaining hits — the two `@pytest.mark.parametrize("attribution_type", [...])`
tests (`test_transform_never_requires_the_flag_column`,
`test_transform_headers_only_response_returns_empty`), both passing `attribution_type=attribution_type,`
(the loop-variable case the sed patterns can't match).

- [ ] **Step 6: Fix the two parametrized call sites by hand**

Add, right after the `REPORTS` import at the top of `tests/test_transform.py`:

```python
_SPEC_BY_ATTRIBUTION = {
    "non_organic": REPORTS["in_app_events_non_organic"],
    "retargeting": REPORTS["in_app_events_retargeting"],
}
```

In `test_transform_never_requires_the_flag_column`, change:

```python
    rows = transform_events(
        df,
        attribution_type=attribution_type,
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase"],
    )
```

to:

```python
    rows = transform_events(
        df,
        spec=_SPEC_BY_ATTRIBUTION[attribution_type],
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase"],
    )
```

Apply the identical change in `test_transform_headers_only_response_returns_empty`. Both functions
keep their `@pytest.mark.parametrize("attribution_type", ["non_organic", "retargeting"])` decorator
and `attribution_type: AttributionType` parameter unchanged — only the body's `transform_events(...)`
call changes.

- [ ] **Step 7: Run the full transform test file, confirm green**

Run: `uv run pytest tests/test_transform.py -v`
Expected: PASS — all 25 (now-migrated) tests plus everything else in the file, same assertion values.

- [ ] **Step 8: Add the master-spec-mandated cross-check test**

Add to `tests/test_reports.py` (this is the test the master spec's Этап 3 criterion explicitly calls
for: "для каждого spec `insert_columns` совпадают с ключами, которые производит `transform_events`"
— it can only be written now that both `reports.py` and the threaded `transform.py` exist):

```python
from appsflyer_pipeline.transform import transform_events


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
```

This needs `import polars as pl` added at the top of `tests/test_reports.py` alongside the existing
imports.

Run: `uv run pytest tests/test_reports.py -v`
Expected: PASS — including the new test, for both registered specs.

- [ ] **Step 9: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
Expected: all green.

```bash
git add src/appsflyer_pipeline/transform.py tests/test_transform.py tests/test_reports.py
git commit -m "BAF-11 stage 3: thread ReportSpec through transform.py"
```

---

### Task 4: Thread `ReportSpec` through `loader.py`, migrate `scripts/load_csv.py`

**Files:**
- Modify: `src/appsflyer_pipeline/loader.py:1-21,138-230`
- Modify: `scripts/load_csv.py:56-66`
- Modify: `tests/test_loader.py:1-16,55-67`
- Modify: `tests/test_loader_integration.py:1-23,66-232`

**Interfaces:**
- Consumes: `appsflyer_pipeline.reports.ReportSpec` (plain import — no cycle, see Architecture
  decision 3).
- Produces: `load_events` replaces `attribution_type: str` with a `spec: ReportSpec` positional
  parameter (placed right after `engine`, before `table_name`) and drops the module-level
  `_INSERT_COLUMNS` constant (`loader.py:139-157`) in favor of `spec.insert_columns`; the DELETE
  predicate's time-bound column comes from `spec.window_column` instead of the literal `event_time`.
  `create_table`/`check_connection`/`_validate_identifier`/`create_engine`/`PipelineError` are
  untouched (they only ever took a plain `table_name: str`, and still do).

- [ ] **Step 1: Add the `ReportSpec` import**

In `src/appsflyer_pipeline/loader.py`, add after line 19 (`from appsflyer_pipeline.config import
Settings`):

```python
from appsflyer_pipeline.reports import ReportSpec
```

- [ ] **Step 2: Delete `_INSERT_COLUMNS`, change `load_events`'s signature and body**

Delete the module constant and its comment (currently lines 138-157):

```python
# Column order for INSERT — must match the keys transform.transform_events() produces.
_INSERT_COLUMNS = (
    "event_time",
    "install_time",
    "attributed_touch_time",
    "event_name",
    "event_revenue",
    "media_source",
    "channel",
    "campaign",
    "campaign_id",
    "adset",
    "adset_id",
    "ad",
    "ad_id",
    "appsflyer_id",
    "customer_user_id",
    "attribution_type",
    "app_id",
)
```

Change `load_events`'s signature (currently lines 160-169) from:

```python
def load_events(
    engine: Engine,
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    app_id: str,
    attribution_type: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> int:
```

to:

```python
def load_events(
    engine: Engine,
    spec: ReportSpec,
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    app_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> int:
```

Change the body (currently lines 176-187) from:

```python
    table_name = _validate_identifier(table_name)
    window_start = datetime.datetime.combine(start_date, datetime.time.min)
    window_end = datetime.datetime.combine(end_date + datetime.timedelta(days=1), datetime.time.min)

    delete_stmt = text(
        f"DELETE FROM `{table_name}` "
        "WHERE app_id = :app_id AND attribution_type = :attribution_type "
        "AND event_time >= :window_start AND event_time < :window_end"
    )
    columns_sql = ", ".join(f"`{c}`" for c in _INSERT_COLUMNS)
    placeholders_sql = ", ".join(f":{c}" for c in _INSERT_COLUMNS)
    insert_stmt = text(f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({placeholders_sql})")
```

to:

```python
    table_name = _validate_identifier(table_name)
    window_column = _validate_identifier(spec.window_column)
    attribution_type = spec.attribution_type
    window_start = datetime.datetime.combine(start_date, datetime.time.min)
    window_end = datetime.datetime.combine(end_date + datetime.timedelta(days=1), datetime.time.min)

    delete_stmt = text(
        f"DELETE FROM `{table_name}` "
        "WHERE app_id = :app_id AND attribution_type = :attribution_type "
        f"AND `{window_column}` >= :window_start AND `{window_column}` < :window_end"
    )
    columns_sql = ", ".join(f"`{c}`" for c in spec.insert_columns)
    placeholders_sql = ", ".join(f":{c}" for c in spec.insert_columns)
    insert_stmt = text(f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({placeholders_sql})")
```

The rest of the function (the `try`/`except SQLAlchemyError`, the `logger.info`, the wipe-warning
`logger.warning`, and `return len(rows)`, currently lines 189-230) needs no further edits — they
already read `attribution_type` as a local name, which now comes from `spec.attribution_type` instead
of the deleted parameter, with the exact same log/error text since the value is identical.

- [ ] **Step 3: Run the loader tests, confirm the expected breakage**

Run: `uv run pytest tests/test_loader.py tests/test_loader_integration.py -v 2>&1 | tail -20`
Expected: FAIL — `test_load_events_wraps_sqlalchemy_error` (and, if a reachable DB is configured
locally, the integration tests) raise `TypeError: load_events() missing 1 required positional
argument: 'spec'` or an unexpected-`attribution_type`-keyword error.

- [ ] **Step 4: Migrate `tests/test_loader.py`'s one call site**

Add the import at the top of `tests/test_loader.py` (after the existing `appsflyer_pipeline.loader`
import block):

```python
from appsflyer_pipeline.reports import REPORTS
```

Change `test_load_events_wraps_sqlalchemy_error` (currently lines 55-67) from:

```python
def test_load_events_wraps_sqlalchemy_error() -> None:
    engine = _unreachable_engine()
    with pytest.raises(PipelineError, match="Could not load events") as excinfo:
        load_events(
            engine,
            "some_table",
            [],
            app_id="app1",
            attribution_type="non_organic",
            start_date=datetime.date(2020, 1, 1),
            end_date=datetime.date(2020, 1, 1),
        )
    assert isinstance(excinfo.value.__cause__, SQLAlchemyError)
```

to:

```python
def test_load_events_wraps_sqlalchemy_error() -> None:
    engine = _unreachable_engine()
    with pytest.raises(PipelineError, match="Could not load events") as excinfo:
        load_events(
            engine,
            REPORTS["in_app_events_non_organic"],
            "some_table",
            [],
            app_id="app1",
            start_date=datetime.date(2020, 1, 1),
            end_date=datetime.date(2020, 1, 1),
        )
    assert isinstance(excinfo.value.__cause__, SQLAlchemyError)
```

Run: `uv run pytest tests/test_loader.py -v`
Expected: PASS — all tests, same error-message assertion.

- [ ] **Step 5: Migrate `tests/test_loader_integration.py`'s six call sites**

Add the import at the top (after the existing `appsflyer_pipeline.loader` import):

```python
from appsflyer_pipeline.reports import REPORTS
```

This file's 6 `load_events(...)` calls (3 in `test_load_events_is_idempotent_and_isolated`, 3 in
`test_load_events_logs_rowcounts_and_warns_on_wipe`) all follow the exact same shape — positional
`engine, settings.db_table, <rows>,` then `app_id=..., attribution_type=test_attribution,
start_date=..., end_date=...`. Apply the same two edits to each of the 6 call sites: insert
`REPORTS["in_app_events_non_organic"],` as a new positional argument right after `engine,` and before
`settings.db_table,`, and delete the `attribution_type=test_attribution,` keyword line. Worked
example — the first call site (currently lines 104-112) goes from:

```python
        count1 = load_events(
            engine,
            settings.db_table,
            [row],
            app_id=test_app_id,
            attribution_type=test_attribution,
            start_date=window_start,
            end_date=window_end,
        )
```

to:

```python
        count1 = load_events(
            engine,
            REPORTS["in_app_events_non_organic"],
            settings.db_table,
            [row],
            app_id=test_app_id,
            start_date=window_start,
            end_date=window_end,
        )
```

Apply the identical two-line edit (insert the spec argument, delete the `attribution_type=` line) at
the remaining 5 call sites: the `count2 = load_events(...)` block right below it, the `finally:`
cleanup `load_events(...)` in the same test, and all 3 `load_events(...)` calls in
`test_load_events_logs_rowcounts_and_warns_on_wipe` (the first load, the wipe load, and its `finally`
cleanup). None of these tests' log-message or count assertions change — `test_attribution` (the local
variable `"non_organic"`) still flows into `attribution_type` via `spec.attribution_type`, since
`REPORTS["in_app_events_non_organic"].attribution_type == "non_organic"` — so
`test_load_events_logs_rowcounts_and_warns_on_wipe`'s assertions on `"deleted=1"`/`"inserted=0"`/
`"wiped"` in the log text are untouched.

Run (only meaningful with a reachable DB — CI has one, locally either point `.env` at a real DB or
accept the skip):
`uv run pytest tests/test_loader_integration.py -v`
Expected: PASS (or all 6 tests SKIPPED with "no usable database in this environment" if none is
reachable — either outcome is fine locally; CI's `mysql:8` service container makes it PASS there).

- [ ] **Step 6: Migrate `scripts/load_csv.py`'s `_INSERT_COLUMNS` import**

In `scripts/load_csv.py`, change the import block (currently lines 56-66) from:

```python
from appsflyer_pipeline.cli import (
    _format_validation_error,  # same secret-safe rendering (issue #27)
)
from appsflyer_pipeline.config import get_settings
from appsflyer_pipeline.loader import (
    _INSERT_COLUMNS,  # reusing the loader's single source of truth for column order
    PipelineError,
    _validate_identifier,  # same identifier-safety gate loader.py itself uses
    check_connection,
    create_engine,
)
```

to:

```python
from appsflyer_pipeline.cli import (
    _format_validation_error,  # same secret-safe rendering (issue #27)
)
from appsflyer_pipeline.config import get_settings
from appsflyer_pipeline.loader import (
    PipelineError,
    _validate_identifier,  # same identifier-safety gate loader.py itself uses
    check_connection,
    create_engine,
)
from appsflyer_pipeline.reports import REPORTS

# BAF-11 stage 3: _INSERT_COLUMNS moved from loader.py into the ReportSpec
# registry -- this script only ever loads into the in-app-events table, so it
# reads that one spec's column order (identical to the retargeting spec's).
_INSERT_COLUMNS = REPORTS["in_app_events_non_organic"].insert_columns
```

This preserves the local name `_INSERT_COLUMNS`, so its 4 existing use sites (`_read_csv`'s
`missing`/`extra` set comparisons, `execute_inserts`'s `columns_sql`/`placeholders_sql`) need no
further edits. This script has no dedicated test file and isn't part of `pytest`'s coverage
(`tool.coverage.run.source_pkgs = ["appsflyer_pipeline"]` excludes `scripts/`) — verify it by
actually exercising the import path:

Run: `uv run python scripts/load_csv.py --help`
Expected: argparse's usage/help text prints and the process exits 0 — proving every import (including
the new `_INSERT_COLUMNS` source) resolves cleanly, without needing a CSV file or a DB connection.

- [ ] **Step 7: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
Expected: all green.

```bash
git add src/appsflyer_pipeline/loader.py scripts/load_csv.py tests/test_loader.py tests/test_loader_integration.py
git commit -m "BAF-11 stage 3: thread ReportSpec through loader.py, migrate load_csv.py"
```

---

### Task 5: Thread `ReportSpec` through `pipeline.py`

**Files:**
- Modify: `src/appsflyer_pipeline/pipeline.py:1-44,93-341`
- Modify: `tests/test_pipeline.py:102-131,134-163`

**Interfaces:**
- Consumes: `appsflyer_pipeline.reports.REPORTS`, `appsflyer_pipeline.reports.ReportSpec` (plain
  import — pipeline.py already imports concrete things from `loader.py`/`transform.py`, no cycle
  risk from also importing `reports.py`).
- Produces: `ATTRIBUTION_TYPES` (`pipeline.py:44`) is deleted. `_iter_work_items` now returns
  `Iterator[tuple[ReportSpec, str, datetime.date, datetime.date]]` instead of
  `Iterator[tuple[str, AttributionType, datetime.date, datetime.date]]`. `_process_window` replaces
  `attribution_type: AttributionType` with `spec: ReportSpec`. `_warn_if_before_retention_floor`
  gains a required `retention_days: int` keyword-only parameter (drops the `MAX_RETENTION_DAYS`
  module import). New: `_active_retention_days() -> int`. `WindowResult`/`RunSummary`/`run_backfill`/
  `run_daily`'s public signatures are unchanged.

- [ ] **Step 1: Update imports, delete `ATTRIBUTION_TYPES`, add `_active_retention_days`**

In `src/appsflyer_pipeline/pipeline.py`, change the import block (currently lines 31-40) from:

```python
from appsflyer_pipeline.appsflyer_client import (
    MAX_RETENTION_DAYS,
    AppsFlyerAPIError,
    AttributionType,
    chunk_date_range,
    fetch_events,
)
from appsflyer_pipeline.config import Settings, get_settings
from appsflyer_pipeline.loader import PipelineError, check_connection, create_engine, load_events
from appsflyer_pipeline.transform import TransformError, transform_events
```

to:

```python
from appsflyer_pipeline.appsflyer_client import (
    AppsFlyerAPIError,
    AttributionType,
    chunk_date_range,
    fetch_events,
)
from appsflyer_pipeline.config import Settings, get_settings
from appsflyer_pipeline.loader import PipelineError, check_connection, create_engine, load_events
from appsflyer_pipeline.reports import REPORTS, ReportSpec
from appsflyer_pipeline.transform import TransformError, transform_events
```

Delete the module constant (currently line 44):

```python
ATTRIBUTION_TYPES: tuple[AttributionType, ...] = ("non_organic", "retargeting")
```

Add, right after `_today()` (currently lines 47-49):

```python
def _active_retention_days() -> int:
    """Retention floor shared by this run's default window and warnings.

    Every REPORTS entry shares the in-app-events 90-day retention today; this
    takes the minimum across REPORTS so a single run-level window can't
    silently outrun the narrowest report's real data availability once a
    shorter-retention report (installs, 60 days, BAF-11 stage 5/6) is
    registered.
    """
    return min(spec.retention_days for spec in REPORTS.values())
```

`AttributionType` stays imported — `WindowResult.attribution_type: AttributionType` (currently line
55) still needs it (see Architecture decision 2: not widened).

- [ ] **Step 2: Change `_iter_work_items`'s return type and body**

Change (currently lines 93-106) from:

```python
def _iter_work_items(
    settings: Settings, start: datetime.date, end: datetime.date
) -> Iterator[tuple[str, AttributionType, datetime.date, datetime.date]]:
    """(app_id x attribution_type x <=31-day chunk) for the [start, end] window.

    Pure -- no HTTP/DB -- so the exact work-item set and chunk boundaries are
    unit-testable without mocking anything.
    """
    for app_id in settings.appsflyer_app_ids:
        for attribution_type in ATTRIBUTION_TYPES:
            for chunk_start, chunk_end in chunk_date_range(
                start, end, max_days=settings.appsflyer_chunk_days
            ):
                yield app_id, attribution_type, chunk_start, chunk_end
```

to:

```python
def _iter_work_items(
    settings: Settings, start: datetime.date, end: datetime.date
) -> Iterator[tuple[ReportSpec, str, datetime.date, datetime.date]]:
    """(report x app_id x <=chunk_days chunk) for the [start, end] window.

    Pure -- no HTTP/DB -- so the exact work-item set and chunk boundaries are
    unit-testable without mocking anything. Loop order (app_id outer, report
    inner) is unchanged from the pre-ReportSpec app_id/attribution_type
    nesting -- see this plan's Architecture decision 4 for why it wasn't
    reordered to match the master spec's "report x app_id" prose.
    """
    for app_id in settings.appsflyer_app_ids:
        for spec in REPORTS.values():
            for chunk_start, chunk_end in chunk_date_range(
                start, end, max_days=settings.appsflyer_chunk_days
            ):
                yield spec, app_id, chunk_start, chunk_end
```

- [ ] **Step 3: Change `_process_window`'s signature and body**

Change the signature (currently lines 109-119) from:

```python
def _process_window(
    client: httpx.Client,
    engine: Engine,
    settings: Settings,
    *,
    app_id: str,
    attribution_type: AttributionType,
    start_date: datetime.date,
    end_date: datetime.date,
    dry_run: bool,
) -> WindowResult:
```

to:

```python
def _process_window(
    client: httpx.Client,
    engine: Engine,
    settings: Settings,
    *,
    spec: ReportSpec,
    app_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
    dry_run: bool,
) -> WindowResult:
```

At the top of the body, right after the docstring (currently ending line 126), add:

```python
    attribution_type = spec.attribution_type
```

Then change the three calls inside the `try` block (currently lines 135-167) from:

```python
        raw_df = fetch_events(
            client,
            app_id=app_id,
            attribution_type=attribution_type,
            from_date=start_date,
            to_date=end_date,
            api_token=settings.appsflyer_api_token,
            media_source=settings.appsflyer_media_source,
            event_names=settings.appsflyer_event_names,
            timezone=settings.appsflyer_timezone,
        )
        fetched_rows = raw_df.height

        rows: list[dict[str, Any]] = transform_events(
            raw_df,
            attribution_type=attribution_type,
            app_id=app_id,
            media_source_filter=settings.appsflyer_media_source,
            event_names_filter=settings.appsflyer_event_names,
        )

        if dry_run:
            loaded_rows = len(rows)
        else:
            loaded_rows = load_events(
                engine,
                settings.db_table,
                rows,
                app_id=app_id,
                attribution_type=attribution_type,
                start_date=start_date,
                end_date=end_date,
            )
```

to:

```python
        raw_df = fetch_events(
            client,
            app_id=app_id,
            spec=spec,
            from_date=start_date,
            to_date=end_date,
            api_token=settings.appsflyer_api_token,
            media_source=settings.appsflyer_media_source,
            event_names=settings.appsflyer_event_names,
            timezone=settings.appsflyer_timezone,
        )
        fetched_rows = raw_df.height

        rows: list[dict[str, Any]] = transform_events(
            raw_df,
            spec=spec,
            app_id=app_id,
            media_source_filter=settings.appsflyer_media_source,
            event_names_filter=settings.appsflyer_event_names,
        )

        if dry_run:
            loaded_rows = len(rows)
        else:
            loaded_rows = load_events(
                engine,
                spec,
                spec.table(settings),
                rows,
                app_id=app_id,
                start_date=start_date,
                end_date=end_date,
            )
```

The `except`/`logger.error`/`WindowResult(...)` in the `except` branch and the final `logger.info`/
`WindowResult(...)` (currently lines 168-203) need no edits — they already read `attribution_type` as
a local name, which Step 3's first edit now sets from `spec.attribution_type`.

- [ ] **Step 4: Change `_warn_if_before_retention_floor`'s signature**

Change (currently lines 206-224) from:

```python
def _warn_if_before_retention_floor(day: datetime.date, what: str) -> None:
    """Issue #28: the floor anchors to TODAY (the API retains a trailing
    window), never to a caller-provided end date -- an explicit past
    --end-date used to skip this warning for fully-beyond-retention windows.
    Warn-and-proceed is deliberate (RUNBOOK §9's probes rely on it). This is
    the API's documented/HTTP-400 boundary; the *silent* empty-response
    boundary is shorter -- see issue #45.
    """
    retention_floor = _today() - datetime.timedelta(days=MAX_RETENTION_DAYS)
    if day < retention_floor:
        logger.warning(
            "Requested %s %s is earlier than the AppsFlyer Pull API's ~%d-day "
            "retention floor (%s) — requests before the floor may return empty "
            "data or an error. Proceeding anyway.",
            what,
            day,
            MAX_RETENTION_DAYS,
            retention_floor,
        )
```

to:

```python
def _warn_if_before_retention_floor(day: datetime.date, what: str, *, retention_days: int) -> None:
    """Issue #28: the floor anchors to TODAY (the API retains a trailing
    window), never to a caller-provided end date -- an explicit past
    --end-date used to skip this warning for fully-beyond-retention windows.
    Warn-and-proceed is deliberate (RUNBOOK §9's probes rely on it). This is
    the API's documented/HTTP-400 boundary; the *silent* empty-response
    boundary is shorter -- see issue #45.

    `retention_days` is per-run (BAF-11 stage 3), resolved by the caller via
    `_active_retention_days()` -- not a module-level constant, since a future
    report can carry a shorter retention than in-app-events' 90 days.
    """
    retention_floor = _today() - datetime.timedelta(days=retention_days)
    if day < retention_floor:
        logger.warning(
            "Requested %s %s is earlier than the AppsFlyer Pull API's ~%d-day "
            "retention floor (%s) — requests before the floor may return empty "
            "data or an error. Proceeding anyway.",
            what,
            day,
            retention_days,
            retention_floor,
        )
```

- [ ] **Step 5: Update `_run_window`'s preflight loop and the loop unpacking `_iter_work_items`**

Change the preflight (currently lines 257-263) from:

```python
    if not dry_run:
        status = check_connection(engine, settings.db_table)
        if not status.table_exists:
            raise PipelineError(
                f"Target table `{settings.db_table}` does not exist yet — "
                "run `appsflyer-pipeline create-table` first."
            )
```

to:

```python
    if not dry_run:
        for table in sorted({spec.table(settings) for spec in REPORTS.values()}):
            status = check_connection(engine, table)
            if not status.table_exists:
                raise PipelineError(
                    f"Target table `{table}` does not exist yet — "
                    "run `appsflyer-pipeline create-table` first."
                )
```

Change the loop (currently lines 266-281) from:

```python
    with httpx.Client() as client:
        for app_id, attribution_type, chunk_start, chunk_end in _iter_work_items(
            settings, start, end
        ):
            results.append(
                _process_window(
                    client,
                    engine,
                    settings,
                    app_id=app_id,
                    attribution_type=attribution_type,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    dry_run=dry_run,
                )
            )
```

to:

```python
    with httpx.Client() as client:
        for spec, app_id, chunk_start, chunk_end in _iter_work_items(settings, start, end):
            results.append(
                _process_window(
                    client,
                    engine,
                    settings,
                    spec=spec,
                    app_id=app_id,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    dry_run=dry_run,
                )
            )
```

- [ ] **Step 6: Update `run_backfill`/`run_daily`'s three `_warn_if_before_retention_floor` call sites and `run_backfill`'s default-window math**

In `run_backfill` (currently lines 285-307), also update the docstring (lines 291-297, untouched by
every other step in this task) — it still describes a fixed `MAX_RETENTION_DAYS` constant, but the
window below it is changed to derive from `_active_retention_days()`, and this task is otherwise
careful to update every docstring that references the changed mechanism (`_iter_work_items`,
`_warn_if_before_retention_floor`). Change:

```python
    """Historical backfill. Defaults to the full available AppsFlyer window:
    [yesterday - (MAX_RETENTION_DAYS - 1), yesterday].

    If an explicit `start` predates the retention floor (today minus
    MAX_RETENTION_DAYS), this does NOT clamp it — it logs a warning and
    proceeds, so an operator can deliberately probe what AppsFlyer actually
    returns for old dates (see RUNBOOK §9 and issue #45).
    """
    end = end or (_today() - datetime.timedelta(days=1))
    default_start = end - datetime.timedelta(days=MAX_RETENTION_DAYS - 1)
    start = start or default_start

    if start > end:
        raise PipelineError(f"start {start} is after end {end}")
    _warn_if_before_retention_floor(start, "backfill start")
```

to:

```python
    """Historical backfill. Defaults to the full available AppsFlyer window:
    [yesterday - (_active_retention_days() - 1), yesterday].

    If an explicit `start` predates the retention floor (today minus
    _active_retention_days(), the narrowest registered report's retention —
    both registered specs currently agree at 90 days), this does NOT clamp
    it — it logs a warning and proceeds, so an operator can deliberately
    probe what AppsFlyer actually returns for old dates (see RUNBOOK §9 and
    issue #45).
    """
    retention_days = _active_retention_days()
    end = end or (_today() - datetime.timedelta(days=1))
    default_start = end - datetime.timedelta(days=retention_days - 1)
    start = start or default_start

    if start > end:
        raise PipelineError(f"start {start} is after end {end}")
    _warn_if_before_retention_floor(start, "backfill start", retention_days=retention_days)
```

In `run_daily` (currently lines 310-341), change:

```python
    if date is not None:
        _warn_if_before_retention_floor(date, "daily --date")
        return _run_window(date, date, dry_run=dry_run)

    settings = get_settings()
    if settings.appsflyer_event_time_from is not None:
        start = settings.appsflyer_event_time_from
        end = settings.appsflyer_event_time_to or (_today() - datetime.timedelta(days=1))
        if start > end:
            raise PipelineError(f"APPSFLYER_EVENT_TIME_FROM {start} is after the window end {end}")
        _warn_if_before_retention_floor(start, "APPSFLYER_EVENT_TIME_FROM")
        return _run_window(start, end, dry_run=dry_run)
```

to:

```python
    retention_days = _active_retention_days()
    if date is not None:
        _warn_if_before_retention_floor(date, "daily --date", retention_days=retention_days)
        return _run_window(date, date, dry_run=dry_run)

    settings = get_settings()
    if settings.appsflyer_event_time_from is not None:
        start = settings.appsflyer_event_time_from
        end = settings.appsflyer_event_time_to or (_today() - datetime.timedelta(days=1))
        if start > end:
            raise PipelineError(f"APPSFLYER_EVENT_TIME_FROM {start} is after the window end {end}")
        _warn_if_before_retention_floor(
            start, "APPSFLYER_EVENT_TIME_FROM", retention_days=retention_days
        )
        return _run_window(start, end, dry_run=dry_run)
```

The rest of `run_daily` (the flagless lookback-window branch) is unaffected — it never calls
`_warn_if_before_retention_floor`.

- [ ] **Step 7: Run the pipeline tests, confirm the expected breakage**

Run: `uv run pytest tests/test_pipeline.py -v 2>&1 | tail -30`
Expected: FAIL — `test_iter_work_items_yields_expected_matrix` and
`test_iter_work_items_respects_configured_chunk_days` fail on the tuple-unpacking lines (`for a, t, s,
e in items`, now the wrong shape); every test using the `load_spy` fixture fails because
`pipeline.load_events` is monkeypatched with a fake whose signature no longer matches the real one
(`TypeError` about `spec`/`attribution_type`).

- [ ] **Step 8: Fix the `load_spy` fixture**

In `tests/test_pipeline.py`, add the import at the top (after the existing
`appsflyer_pipeline.pipeline` import):

```python
from appsflyer_pipeline.reports import ReportSpec
```

Change `load_spy` (currently lines 102-131) from:

```python
@pytest.fixture
def load_spy(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Replaces load_events with a spy that records calls instead of touching
    a real database; returns the list of calls made to it.
    """
    calls: list[dict[str, Any]] = []

    def _fake_load_events(
        engine: object,
        table_name: str,
        rows: list[dict[str, Any]],
        *,
        app_id: str,
        attribution_type: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> int:
        calls.append(
            {
                "app_id": app_id,
                "attribution_type": attribution_type,
                "start_date": start_date,
                "end_date": end_date,
                "rows": rows,
            }
        )
        return len(rows)

    monkeypatch.setattr(pipeline, "load_events", _fake_load_events)
    return calls
```

to:

```python
@pytest.fixture
def load_spy(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Replaces load_events with a spy that records calls instead of touching
    a real database; returns the list of calls made to it.
    """
    calls: list[dict[str, Any]] = []

    def _fake_load_events(
        engine: object,
        spec: ReportSpec,
        table_name: str,
        rows: list[dict[str, Any]],
        *,
        app_id: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> int:
        calls.append(
            {
                "app_id": app_id,
                "attribution_type": spec.attribution_type,
                "start_date": start_date,
                "end_date": end_date,
                "rows": rows,
            }
        )
        return len(rows)

    monkeypatch.setattr(pipeline, "load_events", _fake_load_events)
    return calls
```

`call["attribution_type"]` keeps carrying the same values (`"non_organic"`/`"retargeting"`), just
sourced from `spec.attribution_type` instead of a removed keyword argument — no test that reads
`load_spy`'s recorded `attribution_type` values needs any further change.

- [ ] **Step 9: Fix `test_iter_work_items_yields_expected_matrix`**

Change (currently lines 134-148) from:

```python
def test_iter_work_items_yields_expected_matrix(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch)
    settings = get_settings()
    start = datetime.date(2026, 1, 1)
    end = datetime.date(2026, 3, 31)  # 89 days -> 3 chunks of <=31 days each
    items = list(_iter_work_items(settings, start, end))

    assert {i[0] for i in items} == set(APP_IDS)
    assert {i[1] for i in items} == set(ATTRIBUTION_TYPES)

    one_series = [(s, e) for a, t, s, e in items if a == "app1" and t == "non_organic"]
    assert one_series[0][0] == start
    assert one_series[-1][1] == end
    assert all((e - s).days < 31 for s, e in one_series)
    assert len(items) == len(APP_IDS) * len(ATTRIBUTION_TYPES) * len(one_series)
```

to:

```python
def test_iter_work_items_yields_expected_matrix(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch)
    settings = get_settings()
    start = datetime.date(2026, 1, 1)
    end = datetime.date(2026, 3, 31)  # 89 days -> 3 chunks of <=31 days each
    items = list(_iter_work_items(settings, start, end))

    assert {app_id for _, app_id, _, _ in items} == set(APP_IDS)
    assert {spec.attribution_type for spec, _, _, _ in items} == set(ATTRIBUTION_TYPES)

    one_series = [
        (s, e) for spec, a, s, e in items if a == "app1" and spec.attribution_type == "non_organic"
    ]
    assert one_series[0][0] == start
    assert one_series[-1][1] == end
    assert all((e - s).days < 31 for s, e in one_series)
    assert len(items) == len(APP_IDS) * len(ATTRIBUTION_TYPES) * len(one_series)
```

- [ ] **Step 10: Fix `test_iter_work_items_respects_configured_chunk_days`**

Change (currently lines 151-163) the `one_series` line from:

```python
    one_series = [(s, e) for a, t, s, e in items if a == "app1" and t == "non_organic"]
```

to:

```python
    one_series = [
        (s, e) for spec, a, s, e in items if a == "app1" and spec.attribution_type == "non_organic"
    ]
```

- [ ] **Step 11: Run the full pipeline test file, confirm green**

Run: `uv run pytest tests/test_pipeline.py -v`
Expected: PASS — all tests, same assertion values throughout (URLs mocked by `_url()` still resolve
the same way since `spec.endpoint` for both registered specs equals the value `_url()`'s own
`if attribution_type == "non_organic"` branch already produces).

- [ ] **Step 12: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
Expected: all green.

```bash
git add src/appsflyer_pipeline/pipeline.py tests/test_pipeline.py
git commit -m "BAF-11 stage 3: thread ReportSpec through pipeline.py"
```

---

### Task 6: Thread `ReportSpec` through `cli.py` (multi-table-ready create-table/check-connection)

**Files:**
- Modify: `src/appsflyer_pipeline/cli.py:1-21,47-76`

**Interfaces:**
- Consumes: `appsflyer_pipeline.reports.REPORTS`.
- Produces: `check_connection_command`/`create_table_command` iterate over
  `sorted({spec.table(settings) for spec in REPORTS.values()})` instead of the single
  `settings.db_table` — one distinct table today, so output is unchanged. No CLI flag added (the
  `--report` flag is Этап 5's, see Architecture decision 5). `backfill`/`daily`/`_print_summary`/
  `version` are untouched (their underlying signatures didn't change in Task 5).

- [ ] **Step 1: Add the `REPORTS` import**

In `src/appsflyer_pipeline/cli.py`, add after line 18 (`from appsflyer_pipeline.config import
Settings, get_settings`):

```python
from appsflyer_pipeline.reports import REPORTS
```

- [ ] **Step 2: Generalize `check_connection_command`**

Change (currently lines 47-62) from:

```python
@app.command(name="check-connection")
def check_connection_command() -> None:
    """Verify connectivity to the analytics MariaDB and report the target table's status."""
    settings = _get_settings_or_exit()
    engine = create_engine(settings)
    try:
        status = check_connection(engine, settings.db_table)
    except PipelineError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Connected. MariaDB server version: {status.server_version}")
    if status.table_exists:
        typer.echo(f"Table `{settings.db_table}` exists ({status.row_count} rows).")
    else:
        typer.echo(f"Table `{settings.db_table}` does not exist yet (run `create-table`).")
```

to:

```python
@app.command(name="check-connection")
def check_connection_command() -> None:
    """Verify connectivity to the analytics MariaDB and report every active
    report's target table status (BAF-11 stage 3: today that's exactly one
    table, appsflyer_events_fb, shared by both registered ReportSpecs).
    """
    settings = _get_settings_or_exit()
    engine = create_engine(settings)
    tables = sorted({spec.table(settings) for spec in REPORTS.values()})
    try:
        statuses = [(table, check_connection(engine, table)) for table in tables]
    except PipelineError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Connected. MariaDB server version: {statuses[0][1].server_version}")
    for table, status in statuses:
        if status.table_exists:
            typer.echo(f"Table `{table}` exists ({status.row_count} rows).")
        else:
            typer.echo(f"Table `{table}` does not exist yet (run `create-table`).")
```

- [ ] **Step 3: Generalize `create_table_command`**

Change (currently lines 65-76) from:

```python
@app.command(name="create-table")
def create_table_command() -> None:
    """Create the target table if it doesn't already exist (idempotent)."""
    settings = _get_settings_or_exit()
    engine = create_engine(settings)
    try:
        create_table(engine, settings.db_table)
    except PipelineError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Table `{settings.db_table}` is ready.")
```

to:

```python
@app.command(name="create-table")
def create_table_command() -> None:
    """Create every active report's target table if it doesn't already exist
    (idempotent). BAF-11 stage 3: today that's exactly one table.
    """
    settings = _get_settings_or_exit()
    engine = create_engine(settings)
    tables = sorted({spec.table(settings) for spec in REPORTS.values()})
    try:
        for table in tables:
            create_table(engine, table)
    except PipelineError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    for table in tables:
        typer.echo(f"Table `{table}` is ready.")
```

- [ ] **Step 4: Run the CLI tests, confirm no breakage**

Run: `uv run pytest tests/test_cli.py -v`
Expected: PASS, with no edits needed — `test_check_connection_reports_status_for_both_branches`
monkeypatches `cli.check_connection` with a lambda that returns the same `ConnectionStatus`
regardless of the table name it's called with, and with exactly one table in the set, it's called
exactly once, producing the same `"exists (42 rows)"`/`"does not exist yet"` output as before.
Likewise `test_create_table_success_reports_ready`/`test_create_table_reports_failure`: one table,
one call, same `"is ready."` / `FAILED:` output. If this run shows any failure, stop and diff the
actual vs. expected `result.output` before changing anything — a failure here means the "N=1 table
today, so this generalization must be byte-identical" premise this task's Interfaces block asserts
was wrong, not that a test needs updating.

- [ ] **Step 5: Run the full suite once more (cli.py is the last production file this plan touches)**

Run: `uv run pytest -v`
Expected: PASS — every test in the suite (same count as this plan's Task 1 baseline of 160 passed +
5 skipped locally, or all passing under CI's `mysql:8` service container).

- [ ] **Step 6: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
Expected: all green.

```bash
git add src/appsflyer_pipeline/cli.py
git commit -m "BAF-11 stage 3: thread ReportSpec through cli.py (multi-table-ready create-table/check-connection)"
```

---

### Task 7: Full regression gate, diff audit, PR

**Files:** none (verification only).

- [ ] **Step 1: Full gate**

Run: `uv run pre-commit run --all-files`
Expected: clean — ruff, ruff-format, mypy all pass (this is what CI's single "Lint + format + type
check" step runs).

Run: `uv run pytest --cov-fail-under=98`
Expected: PASS at the CI-gated coverage threshold. Locally, `loader.py`'s coverage depends on the 6
integration tests in `tests/test_loader_integration.py` running against a reachable DB (they skip,
not fail, without one) — if they skip locally, coverage may read below 98% on `loader.py` alone
without failing the overall gate the way CI's `mysql:8` service container ensures; if the overall
`--cov-fail-under=98` fails locally purely because of that skip, that is expected and matches this
repo's existing precedent (`CLAUDE.md`: "gated at `--cov-fail-under=98` in CI only") — do not chase it
locally, let CI's service container confirm it.

- [ ] **Step 2: Diff audit against the acceptance criterion**

Run: `git diff main --stat` to see every changed file, then specifically audit the test files for any
value (not signature) change:

```bash
git diff main -- tests/ | grep -E '^[+-]' | grep -vE '^(\+\+\+|---)' | grep -viE 'spec=|REPORTS\[|_SPEC_BY_ATTRIBUTION|from appsflyer_pipeline\.reports|^\+$|^-$'
```

Expected: every remaining line is one of the structural edits this plan specified by hand (the
`load_spy` fixture's new `spec: ReportSpec` parameter, the `_iter_work_items` tuple-unpacking fixes,
the `_INSERT_COLUMNS` docstring-comment rewording in `scripts/load_csv.py`, new test functions added
in `tests/test_reports.py`) — **no** line should be an existing assertion's expected value (a URL, a
log substring, a row count, an SQL fragment) changing. If anything else shows up, stop and reconcile
it against this plan's Architecture decisions before proceeding — this grep is a coarse filter, not a
proof, so read what it returns.

- [ ] **Step 3: Confirm commit history matches the plan's tasks**

Run: `git log --oneline main..HEAD`
Expected: 6 commits (Tasks 1-6; Task 7 has no commit of its own), each matching one of this plan's
`git commit -m "BAF-11 stage 3: ..."` messages, no stray commits.

- [ ] **Step 4: Push and open the PR**

```bash
git push -u origin baf-11-stage-3-report-spec
gh pr create --title "BAF-11 stage 3: ReportSpec refactor (no behavior change)" --body "$(cat <<'EOF'
## Summary
- Introduces `src/appsflyer_pipeline/reports.py` (`ReportSpec` + `REPORTS` registry) covering the
  two report types that already exist (in-app-events non_organic + retargeting).
- Threads `ReportSpec` through `appsflyer_client.py`, `transform.py`, `loader.py`, `pipeline.py`,
  `cli.py`, and `scripts/load_csv.py`, replacing the scattered `attribution_type`-keyed constants
  (`_ENDPOINT_BY_ATTRIBUTION`, `_COLUMN_MAP`, `_INSERT_COLUMNS`, `ATTRIBUTION_TYPES`) with one
  source of truth per report.
- Pure refactor: no request parameter, SQL statement, log message, or CLI output changes for any
  existing command. Only call signatures in the test suite changed.
- Prep for BAF-11 stages 5-6 (the `installs` report + its own table) — not part of this PR.

## Test plan
- [ ] `uv run pre-commit run --all-files` clean
- [ ] `uv run pytest --cov-fail-under=98` passes in CI (mysql:8 service container)
- [ ] Manual: `uv run appsflyer-pipeline check-connection` and `create-table` against a real DB still
      report exactly one table (`appsflyer_events_fb`), same as before this PR
EOF
)"
```

Return the PR URL to the user.

---

## Final check before opening the PR

- [ ] `uv run pre-commit run --all-files` clean.
- [ ] `uv run pytest --cov-fail-under=98` passes (CI's real gate).
- [ ] `git diff main --stat` shows exactly: `src/appsflyer_pipeline/{reports.py (new),
  appsflyer_client.py, transform.py, loader.py, pipeline.py, cli.py}`, `scripts/load_csv.py`,
  `tests/{test_reports.py (new), test_appsflyer_client.py, test_transform.py, test_loader.py,
  test_loader_integration.py, test_pipeline.py}` — no `config.py`, no `docs/`, no deploy/env files.
- [ ] No test's expected SQL string, HTTP param value, log substring, or row/column value changed —
  confirmed by Task 7 Step 2's diff audit.
- [ ] `git log --oneline main..HEAD` shows exactly the 6 commits from Tasks 1-6.
