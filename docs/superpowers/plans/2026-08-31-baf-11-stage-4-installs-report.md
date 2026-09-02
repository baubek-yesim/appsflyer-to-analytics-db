# BAF-11 Stage 4: Installs Table + Report (Этапы 5-6) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `installs`/`installs_retarget` their own table and `ReportSpec`s (Этапы 5-6 of the
BAF-11 spec): `db_table_installs`, a 128-column DDL sized to the live column-sizing measurement,
`create-table` covering every registered report's table, and the two new specs wired end-to-end
(fetch -> transform -> load) with their own dedupe-key strategy and a **hard** 60-day retention
clamp. In-app-events' table, schema, dedupe policy, and warn-only retention behavior are
byte-for-byte unchanged.

**Architecture:** No new modules. Extends `reports.py`'s `REPORTS` registry (Stage 3) with two more
entries and widens `ReportSpec` by exactly the fields the master spec's "full pass-through, own
dedupe key" requirement forces: `column_map` becomes `Mapping[str, str] | None` (installs sets it
`None` — normalize every raw column instead of a hand-written dict), `dedupe_key` becomes a
per-spec `Callable` (installs gets its own 2-column key, in-app-events' key is reproduced verbatim,
byte-for-byte), and `hard_clamp_retention: bool` (installs `True`, in-app-events `False` —
preserves the existing warn-and-proceed behavior for in-app-events). `loader.py` gains a second DDL
template, selected by `spec.name` (`"in_app_events"` vs `"installs"`) rather than by table name, so
`create_table` stays a two-argument-plus-template function instead of guessing schema from a string.

**Tech Stack:** Python 3.12, pydantic-settings, httpx, polars, SQLAlchemy+PyMySQL, pytest (TDD,
`pytest-cov` branch coverage).

**Spec:** `docs/superpowers/plans/2026-08-13-baf-11-full-raw-export.md`, sections "Этап 5. Вторая
таблица и маршрутизация" and "Этап 6. Отчёт installs". `docs/superpowers/specs/2026-08-13-baf-11-column-sizing.md`
for the exact 128-column list and measured lengths. **Ground truth for every interface this plan
touches is `docs/superpowers/plans/2026-08-31-baf-11-stage-3-report-spec.md`** — `ReportSpec`'s
field set, `REPORTS`' two existing entries, and every threaded call site (`fetch_events(spec=...)`,
`transform_events(spec=...)`, `load_events(engine, spec, table_name, rows, ...)`) are exactly as
that plan defines them, assumed merged unchanged. **Precondition:** work happens on branch
`baf-11-stage-4-installs-report`, branched from `main` **after** `baf-11-stage-3-report-spec` (PR
for the plan above) is merged. Every `Files:`/`Interfaces:` block below describes the file as it
will exist post-Stage-3 (reconstructed from that plan's exact diffs, since this worktree does not
have Stage 3 applied yet) — re-read the real file on the stage-4 branch before editing and treat any
divergence from what's quoted here as a stop-and-reconcile signal, not something to paper over.

## Out of scope (left for later BAF-11 stages, tracked in the spec doc)

- **Cutover (Этап 9)** — flipping production's `APPSFLYER_MEDIA_SOURCE`/`APPSFLYER_EVENT_NAMES` off,
  enabling installs in the scheduled timer, the RUNBOOK §15 procedure. This plan only makes installs
  *available* to run manually/`--dry-run`; nothing here changes what the deployed systemd timer
  actually pulls.
- **The production PK/index migration for the EXISTING `appsflyer_events_fb` table (Этап 7).**
  Unrelated to installs' own new index, which this plan does add (Task 3) — Этап 7 is about the
  *already-provisioned* production in-app-events table lacking `id`/PRIMARY KEY/`idx_app_attr_time`
  today.
- **Acceptance/reconciliation (Этап 10)** — no live-data comparison against a manual AppsFlyer UI
  export.
- **The exact-duplicate-collapsing policy question for in-app-events (Этап 8b)** — `_dedupe_rows`'s
  behavior for in-app-events (conflict → keep latest `install_time`, drop the loser; exact
  duplicates collapse silently) is not relitigated. This plan generalizes `_dedupe_rows` to accept a
  per-report key function so installs can have its own key *without* touching in-app-events'
  runtime behavior — see Architecture decision 2. installs' own dedupe key **is** in scope (the
  master spec assigns it to this stage, Этап 6) and is a genuinely new, unreviewed decision — flagged
  explicitly below for Mark/data-analytics sign-off, the same way Этап 8b flags in-app-events'.

## Global Constraints

- Python 3.12, `uv`-managed (`uv sync`, `uv run ...`) — never bare `python`/`pip`.
- Gates after every task: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy`
  (strict — `files=["src","tests"]`), `uv run pytest`. Run `uv run pre-commit run --all-files`
  before the final PR.
- Commit after each step that says "Commit" — small, reviewable commits, not one commit per task.
- Work happens on branch `baf-11-stage-4-installs-report`, based on `main` **after** Stage 3's PR
  merges. Do not touch `main` directly.
- **Regression bar:** every existing test for in-app-events must stay green with **no change to its
  expected value** (SQL string, HTTP param, log substring, row/column value) — only where adding a
  third/fourth `ReportSpec` to `REPORTS` mechanically changes an assumption a test made about "there
  are exactly 2 specs" (see Architecture decision 5) is a test allowed to change, and only its
  iteration/count logic, never its per-spec expected values.
- `CLAUDE.md`'s "Build stages" tracking table update is out of scope for this plan (matches Stage 2/3
  precedent) — whoever merges this stage's PR adds the entry.

## Architecture decisions made concretely for this stage

1. **`ReportSpec.column_map` widens to `Mapping[str, str] | None`.** `None` means full pass-through:
   `transform_events` normalizes every raw column AppsFlyer actually returned via a new
   `transform.normalize_column_name()` instead of consulting a fixed dict, then validates the
   *produced* target-column set against `spec.insert_columns` (already mandatory, already the DDL's
   source of truth) rather than skip validation — an AppsFlyer schema change (column renamed, added,
   dropped) still fails loudly, it just isn't checked against a hand-maintained raw-name dict anymore.
   `normalize_column_name` reproduces every existing `_IN_APP_EVENTS_COLUMN_MAP` mapping exactly
   (lowercase, spaces → underscores — confirmed against all 15 existing entries by
   `test_normalize_matches_every_in_app_events_column_map_entry` in Task 2), with one explicit
   override: raw `"App ID"` (AppsFlyer's own numeric/bundle app identifier, one of the 81 default
   columns) would otherwise normalize to `"app_id"` — colliding with the pipeline's own **injected**
   `app_id` column (the queried app, added by `transform_events` itself, same name used by
   in-app-events' target schema and by `loader`'s DELETE predicate). Same real-world value in
   practice (one app queried at a time), but two different sources writing one physical column is a
   landmine for any future multi-app-aggregate report — renamed to `appsflyer_app_id` instead.
2. **`ReportSpec.dedupe_key: Callable[[dict[str, Any]], tuple[Any, ...]]`** (new field, mirrors the
   existing `table: Callable[[Settings], str]` field's rationale from Stage 3: a callable, not a
   column-name tuple, so a typo in a key column name is a `mypy`/runtime error at the call site, not
   a silently-wrong tuple). `_dedupe_rows` takes `key_fn` instead of a hardcoded 4-tuple construction;
   in-app-events' two specs pass a function that builds the **exact same** `(event_time, event_name,
   appsflyer_id, <Event Value discriminator>)` key `_dedupe_rows` already builds today — verified
   byte-identical by running the full existing `test_transform.py` suite unmodified (Task 4 Step 8).
   **installs' own key is `(appsflyer_id, event_time)`** — see the dedicated rationale below.
3. **`ReportSpec.hard_clamp_retention: bool`** (new field). `_iter_work_items` (`pipeline.py`) now
   clamps a spec's *effective* start date up to `today - spec.retention_days` **only when this flag
   is `True`**; in-app-events sets it `False` (both existing specs), preserving the exact current
   behavior — `_iter_work_items` still yields `chunk_start == start` for them regardless of how old
   `start` is, and the only floor-crossing signal stays `_warn_if_before_retention_floor`'s
   run-level WARNING (RUNBOOK §9 relies on this "warn and proceed" shape for its probes — Stage 3
   preserved it deliberately, see that plan's Architecture decision 6, and this plan must not
   regress it). installs sets it `True`: a response for a date past AppsFlyer's *real* (undocumented,
   shorter-than-90-day) retention boundary can come back as a valid, header-only **empty** report
   (issue #45's shape) — the idempotent delete-then-insert would then wipe nothing (nothing was ever
   there) or, worse on a second run, wipe a window that a wider ticket-vs-vendor-doc mismatch
   actually did have data for. A warning that's easy to miss in journald is not an acceptable
   mitigation for a class of bug this codebase has already hit once (issue #45) — hence "hard", not
   "warn".

   **This flag only controls `_iter_work_items`'s per-spec chunk clamp — it does NOT, by itself, fix
   `run_backfill`/`run_daily`'s caller-facing default-window and warn-threshold arithmetic.** Stage 3
   already wired both functions' `default_start`/`_warn_if_before_retention_floor` calls off a single
   run-level `_active_retention_days() = min(spec.retention_days for spec in REPORTS.values())`
   (`docs/superpowers/plans/2026-08-31-baf-11-stage-3-report-spec.md`, its Step 1 and Step 6). That
   `min()` is 90 today (both existing in-app-events specs share `MAX_RETENTION_DAYS`) but becomes 60
   the instant this stage's `installs_non_organic`/`installs_retargeting` specs (`retention_days=60`)
   are registered in Task 2 — silently narrowing in-app-events' no-args default backfill window and
   warn threshold from 90 days to 60, weeks before Task 5 (which owns `pipeline.py`) ever touches the
   file. That is a real regression in in-app-events' behavior (contradicts this plan's Goal and the
   Global Constraints' regression bar), not merely a mechanical "REPORTS grew" ripple, and it is a
   different code path from this decision's `_iter_work_items` clamp: `hard_clamp_retention` per-spec
   correctly keeps in-app-events' *explicit* `--start-date`/`--date` chunks unclamped, but it does
   nothing about what `start`/threshold `run_backfill`/`run_daily` compute in the no-args case.
   **Fix, implemented in Task 5:** decouple `run_backfill`/`run_daily`'s default-window and
   warn-threshold math from `_active_retention_days()`'s cross-`REPORTS` minimum — use
   `MAX_RETENTION_DAYS` there instead, since it is what those two caller-facing computations are
   actually about (the widest retention among `hard_clamp_retention=False` specs, today just
   in-app-events); installs narrows its own effective start inside `_iter_work_items` regardless of
   what default the caller's no-args `start` resolves to, so the run-level default no longer needs to
   track installs' narrower retention at all. `_active_retention_days()` itself is untouched (Stage 3
   ground truth, still `min(...)`, still correctly returns 60 once installs is registered) — this
   fix only changes which specs' retention feeds the two call sites in `pipeline.py`, not the helper's
   own definition.
4. **`db_table_installs` selects the DDL template by `spec.name`, not by table name.** `loader.py`
   gains `_CREATE_TABLE_TEMPLATE_BY_REPORT_NAME: dict[str, str]` (keys: `"in_app_events"`,
   `"installs"` — `ReportSpec.name`, already a Stage-3 field) and `create_table` gains a third
   parameter, `report_name: str`, used only to look up the template. This keeps schema selection
   explicit and typed instead of inferring "which columns does this table have" from the
   operator-configured table-name string, which could be anything.
5. **The `REPORTS`-registry-growth ripple.** Every existing test that assumed `REPORTS` holds
   *exactly* the two in-app-events specs (e.g. `test_pipeline.py`'s `one_series` filters keyed only
   on `attribution_type`, not also `spec.name`; `_mock_all_ok()`'s two-URL respx setup;
   `test_appsflyer_client.py`'s `test_fetch_events_never_sends_additional_fields` looping
   `REPORTS.values()`) breaks the moment `REPORTS` grows to four entries — not because behavior
   regressed, but because the test's *own* assumption ("there are 2 specs total") stopped holding.
   Task 5/6 fix these by narrowing the iteration/assertion to what the test actually means (e.g.
   `spec.name == "in_app_events"`, or "specs with `additional_fields == ()`") — never by loosening an
   assertion's expected *value*. This is the mechanical fallout Stage 3 explicitly deferred ("`REPORTS`
   in this plan contains exactly the two report types that already exist... this is the next plan").
6. **DDL type assignment for the 47 always-empty-in-sample columns.** The column-sizing spec's
   `maxlen` is a measured **lower bound**, not truth, and is `?` (unknown) for columns with zero
   non-null values in the one-day sample. Per the master spec's instruction ("типы назначаются по
   field dictionary"), Task 3 assigns types by **semantic category** rather than guessing per column:
   revenue/cost fields → `DECIMAL(18,4)`; currency codes → `VARCHAR(16)`; boolean/flag-shaped fields
   (`Is Receipt Validated`, `GDPR Applies`, `ATT`, ...) → `VARCHAR(16)`; device/advertising
   identifiers (`IDFA`, `IDFV`, `IMEI`, `Android ID`, `Amazon Fire ID`) → `VARCHAR(128)`, matching the
   already-populated `Advertising ID`'s measured size; every `Contributor {1,2,3} X` column → the
   same type as its non-contributor twin (`Contributor 2 Campaign` → same as `Campaign`, etc.),
   since they're the same kind of data, just for a secondary/tertiary attribution touch; everything
   else unknown → `VARCHAR(64)`. This deliberately avoids the master spec's warned-against trap
   ("однородный `VARCHAR(255)` не создастся вообще") while still fitting comfortably (Task 3's
   byte-budget table: ~28.4 KB of 65,535). `customer_user_id` is sized `VARCHAR(255)` (not the
   sample's `VARCHAR(16)`) specifically to match the *existing* `appsflyer_events_fb.customer_user_id`
   column's size for the same field — an app-defined arbitrary string, same risk profile, same
   precedent.
7. **installs' dedupe key — `(appsflyer_id, event_time)`, not in-app-events' key verbatim.**
   In-app-events' 4-column key (`event_time`, `event_name`, `appsflyer_id`, the `Event Value`
   discriminator added in BAF-11 stage 2) doesn't transfer:
   - `Event Value` is **always empty** for installs (confirmed live — column-sizing spec: 0 non-null
     across both `installs_report` and `installs-retarget`, 2,790 combined rows). Including it in the
     key would be a permanent no-op, not a discriminator.
   - `Event Name` is a near-constant per report (`"install"`-shaped for `installs_report`, a fixed
     small vocabulary for `installs-retarget`, per the sizing spec's `maxlen` of 7 and 14
     respectively) — it doesn't discriminate between two real, distinct rows the way it does for
     in-app-events' open-ended purchase/screen-view event names.
   - `Install Time`, which in-app-events uses only as the **tiebreak** (not the key), is the
     *original* install timestamp and is constant across every row for one device in the
     `installs-retarget` report — every re-engagement by the same device shares it. Relying on it to
     discriminate (or even to break ties usefully) between two real re-engagement events is exactly
     backwards for that report.
   - `AppsFlyer ID` is the closest thing installs has to a row-identity column — one ID names one
     specific app-instance; `installs-retarget`'s distinct re-engagement events for that device still
     carry it.
   - `Event Time`, unlike `Install Time`, **is** the per-touch timestamp for a `installs-retarget` row
     — it's the field that actually varies per distinct re-engagement, per AppsFlyer's report
     semantics. `(appsflyer_id, event_time)` therefore keeps two genuinely different re-engagement
     rows apart while still collapsing an exact re-fetch of the same window (identical key, identical
     row → the existing exact-duplicate-collapse path, untouched).
   - **Tiebreak on a true key collision** (same `appsflyer_id` **and** same `event_time`,
     disagreeing on some other field) still resolves via the existing `_install_time_rank`-based
     "latest `install_time` wins" helper, reused as-is for interface consistency with in-app-events —
     but for installs this will almost always be a **tie** (`install_time` is constant per device),
     degenerating in practice to "last row in report order wins." That's an accepted, low-information
     fallback for a conflict class with **no live-measured instance yet** — unlike in-app-events'
     `Event Value` fix (BAF-11 stage 2), which had an actual measured 5-row conflict group to design
     against.
   - **Open risk, flagged for Mark/data-analytics sign-off, not resolved here:** if AppsFlyer ever
     reports two genuinely different engagement events for the same device in the exact same second
     (same `appsflyer_id` + same `event_time`) — plausible for `installs-retarget` specifically, since
     re-engagement can in principle fire more than once — this key silently collapses them to one
     row, chosen by report order, discarding a real row. No live measurement has confirmed whether
     this actually occurs (unlike the in-app-events `Event Value` conflict, which was measured:
     5 rows in one group). **Recommendation, not automated by this plan:** after installs runs for
     1-2 weeks, run a `GROUP BY appsflyer_id, event_time HAVING COUNT(*) > 1` against the raw fetched
     rows (before dedup) for `installs-retarget` specifically, and revisit this key if it ever fires.
     This mirrors how Этап 8b flags the *existing*, unresolved in-app-events exact-duplicate-collapse
     question — this is installs' equivalent open question, not a solved one.

---

### Task 1: `db_table_installs` config field + env-fixture ripple

**Files:**
- Modify: `src/appsflyer_pipeline/config.py`
- Modify: `.github/workflows/ci.yml:44-52`
- Modify: `.env.example`, `deploy/appsflyer.env.example`, `deploy/user-level/appsflyer.env.example`
- Modify: `tests/test_config.py`, `tests/test_pipeline.py`, `tests/test_cli.py`

**Interfaces:**
- Produces: `Settings.db_table_installs: RequiredStr` — same validation pattern as the existing
  `db_table` field (`config.py:52`: `RequiredStr`, i.e. stripped + `min_length=1`, so a truncated
  EnvironmentFile line fails startup loudly, matching issue #29's rationale for every other
  required scalar).

- [ ] **Step 1: Write the failing config test**

Add to `tests/test_config.py`, right after `test_loads_required_fields_from_env` (after line 50):

```python
def test_loads_db_table_installs_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(monkeypatch, DB_TABLE_INSTALLS="appsflyer_installs_fb")
    assert settings.db_table_installs == "appsflyer_installs_fb"


@pytest.mark.parametrize("raw", ["", "   "])
def test_empty_db_table_installs_rejected(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    with pytest.raises(ValidationError):
        _settings(monkeypatch, DB_TABLE_INSTALLS=raw)
```

Add `"DB_TABLE_INSTALLS": "appsflyer_installs_events"` to `BASE_ENV` (`tests/test_config.py:10-19`)
so every other existing test (which doesn't care about this field) still constructs a valid
`Settings`. This mirrors `DB_TABLE`'s own entry in the same dict.

- [ ] **Step 2: Run it, confirm it fails**

Run: `uv run pytest tests/test_config.py -k db_table_installs -v`
Expected: FAIL — `test_loads_db_table_installs_from_env` fails with a pydantic "extra fields not
permitted"-adjacent error is NOT what happens here (`extra="ignore"` in `model_config`) — instead
`settings.db_table_installs` raises `AttributeError` (the field doesn't exist yet).

- [ ] **Step 3: Add the field**

In `src/appsflyer_pipeline/config.py`, add right after `db_table: RequiredStr` (line 52):

```python
    # BAF-11 stage 4: installs/installs_retarget get their own table (ticket
    # decision #2 — in-app-events stays on the same 17-column schema/table;
    # installs' 128-column full-fields shape is a new table, not a migration
    # of the existing one). Same validation as db_table: a truncated
    # EnvironmentFile line must fail startup loudly (issue #29), not degrade
    # to writing installs data into an empty-string table name.
    db_table_installs: RequiredStr
```

- [ ] **Step 4: Run the config tests, confirm green**

Run: `uv run pytest tests/test_config.py -v`
Expected: PASS — all tests, including the two new ones.

- [ ] **Step 5: Propagate `DB_TABLE_INSTALLS` to every other fixture that constructs `Settings`**

Every env dict below constructs a real `Settings` (directly, or indirectly via `get_settings()`),
so each needs the new required field or the whole file's tests fail at collection/setup, not just
the ones that care about installs. Add `"DB_TABLE_INSTALLS": "appsflyer_installs_events"` (or a
file-appropriate value) as a new key to each:

- `tests/test_pipeline.py`'s `BASE_ENV` (currently lines 27-36).
- `tests/test_cli.py`'s `UNREACHABLE_ENV` (currently lines 18-26) — `CLI_ENV` inherits it via
  `{**UNREACHABLE_ENV, ...}` (line 30), no separate edit needed there.
- `tests/test_cli.py`'s `test_format_validation_error_never_includes_input_values` (starts line
  150): this test deliberately `monkeypatch.delenv`s most fields to exercise the "missing fields"
  error path, then sets only `DB_HOST`/`DB_PORT`/`DB_USER`/`DB_PASSWORD` — it does **not** set
  `DB_TABLE_INSTALLS`, which is fine (the test's whole point is that required fields are missing) as
  long as `DB_TABLE_INSTALLS` doesn't accidentally leak in from the developer's ambient shell/`.env`
  and mask the error. Add `"DB_TABLE_INSTALLS"` to that test's `monkeypatch.delenv(...)` loop (the
  tuple at lines 161-169, alongside `"DB_TABLE"` at line 163).
- `tests/test_config.py`'s `test_empty_scalar_rejected`'s `@pytest.mark.parametrize("field", [...])`
  list (lines 111-115, inside the decorator starting line 108): add `"DB_TABLE_INSTALLS"` alongside
  `"DB_TABLE"` (line 114) — same "a truncated line must fail loudly" contract this test already pins
  for every other required scalar.
- `tests/test_loader.py`'s `_unreachable_engine()` helper (currently lines 30-44): this one doesn't
  go through env vars at all — it constructs `Settings(...)` directly with explicit keyword
  arguments. Add `db_table_installs="some_installs_table"` to that call, alongside the existing
  `db_table="some_table"` line, or every test using this helper (`test_create_table_wraps_sqlalchemy_error`,
  `test_load_events_wraps_sqlalchemy_error`, and Task 3's new
  `test_load_events_deletes_on_install_time_for_installs_spec`) fails at `Settings` construction with
  a missing-field `ValidationError`, not the `SQLAlchemyError`/`PipelineError` each of them is
  actually testing for.

Run: `uv run pytest -q`
Expected: PASS — every test in the suite (this step is pure fixture propagation, no new assertions).

- [ ] **Step 6: Add `DB_TABLE_INSTALLS` to CI**

In `.github/workflows/ci.yml`, add right after `DB_TABLE: appsflyer_purchase_events` (line 50):

```yaml
          DB_TABLE_INSTALLS: appsflyer_installs_fb
```

- [ ] **Step 7: Document the new var in the env-example files**

In `.env.example`, add right after `DB_TABLE=appsflyer_events_fb` (line 11):

```
DB_TABLE_INSTALLS=appsflyer_installs_fb
```

Add the identical line to `deploy/appsflyer.env.example` (after its own `DB_TABLE=appsflyer_events_fb`,
line 24) and `deploy/user-level/appsflyer.env.example` (after its own `DB_TABLE=appsflyer_events_fb`,
line 35).

- [ ] **Step 8: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
Expected: all green.

```bash
git add src/appsflyer_pipeline/config.py .github/workflows/ci.yml .env.example \
  deploy/appsflyer.env.example deploy/user-level/appsflyer.env.example \
  tests/test_config.py tests/test_pipeline.py tests/test_cli.py
git commit -m "BAF-11 stage 4: add DB_TABLE_INSTALLS config field"
```

---

### Task 2: `reports.py` — normalize_column_name, widened `ReportSpec`, two new specs

**Files:**
- Modify: `src/appsflyer_pipeline/transform.py` (add `normalize_column_name`, export
  `DEDUPE_DISCRIMINATOR_ROW_KEY` publicly — prerequisite for reports.py's import in this task; the
  rest of transform.py's threading is Task 4)
- Modify: `src/appsflyer_pipeline/reports.py`
- Modify: `tests/test_reports.py`
- Modify: `tests/test_transform.py` (just the rename + the new normalize function's tests)

**Interfaces:**
- Consumes (new, from `transform.py`): `normalize_column_name(raw: str) -> str`,
  `DEDUPE_DISCRIMINATOR_ROW_KEY: str` (renamed from the Stage-3/2 private
  `_DEDUPE_DISCRIMINATOR_ROW_KEY` — now crosses a module boundary, so it loses its leading
  underscore; `transform.py`'s internal use sites are updated to the new name, no behavior change).
  This is a real, non-`TYPE_CHECKING` import (`reports.py` -> `transform.py`) — safe: `transform.py`'s
  only reference to `reports.py` is `TYPE_CHECKING`-guarded (Stage 3 Architecture decision 3), so
  `transform.py` imports nothing from `reports.py` at runtime, and no cycle exists.
- Produces: `ReportSpec` gains `dedupe_key: Callable[[dict[str, Any]], tuple[Any, ...]]` and
  `hard_clamp_retention: bool` (both new, appended after `window_column` to keep the diff additive);
  `column_map` widens from `Mapping[str, str]` to `Mapping[str, str] | None`. `REPORTS` grows from 2
  to 4 entries: `installs_non_organic` (endpoint `installs_report`), `installs_retargeting`
  (endpoint `installs-retarget`). New public constants: `INSTALLS_ADDITIONAL_FIELDS: tuple[str, ...]`
  (47 names), `INSTALLS_RAW_COLUMNS: tuple[str, ...]` (128 names, default+additional, in AppsFlyer's
  own column order) — the latter is public specifically so Task 5/6's tests can build a realistic
  128-column fixture CSV without hand-typing the list a second time.

- [ ] **Step 1: Write the failing `normalize_column_name` tests**

Add to `tests/test_transform.py` (new section, anywhere after the existing imports):

```python
from appsflyer_pipeline.transform import normalize_column_name


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Event Time", "event_time"),
        ("Install Time", "install_time"),
        ("Attributed Touch Time", "attributed_touch_time"),
        ("Event Name", "event_name"),
        ("Event Revenue", "event_revenue"),
        ("Media Source", "media_source"),
        ("Channel", "channel"),
        ("Campaign", "campaign"),
        ("Campaign ID", "campaign_id"),
        ("Adset", "adset"),
        ("Adset ID", "adset_id"),
        ("Ad", "ad"),
        ("Ad ID", "ad_id"),
        ("AppsFlyer ID", "appsflyer_id"),
        ("Customer User ID", "customer_user_id"),
    ],
)
def test_normalize_matches_every_in_app_events_column_map_entry(raw: str, expected: str) -> None:
    """BAF-11 stage 4: normalize_column_name must reproduce every existing
    hand-written _IN_APP_EVENTS_COLUMN_MAP entry exactly -- it's about to
    become the ONLY mapping mechanism for installs' full pass-through mode,
    so a divergence here would silently rename a column relative to
    in-app-events' precedent.
    """
    assert normalize_column_name(raw) == expected


def test_normalize_handles_digits_in_contributor_columns() -> None:
    assert normalize_column_name("Contributor 1 Touch Time") == "contributor_1_touch_time"
    assert normalize_column_name("Contributor 2 Media Source") == "contributor_2_media_source"


def test_normalize_renames_raw_app_id_to_avoid_collision() -> None:
    """AppsFlyer's own "App ID" field would otherwise normalize to "app_id",
    colliding with the pipeline's OWN injected app_id column (the queried
    app, added by transform_events after mapping).
    """
    assert normalize_column_name("App ID") == "appsflyer_app_id"
```

- [ ] **Step 2: Run it, confirm it fails**

Run: `uv run pytest tests/test_transform.py -k normalize -v`
Expected: FAIL — `ImportError: cannot import name 'normalize_column_name'`.

- [ ] **Step 3: Implement `normalize_column_name` and rename the discriminator key export**

In `src/appsflyer_pipeline/transform.py`, change (post-Stage-3) `_DEDUPE_DISCRIMINATOR_ROW_KEY =
"__dedupe_event_value"` to:

```python
# BAF-11 stage 4: exported (no leading underscore) -- reports.py's in-app-events
# dedupe_key function needs the exact same internal row key transform_events()
# writes it under, so the two stay in sync by construction instead of by two
# separately-maintained string literals.
DEDUPE_DISCRIMINATOR_ROW_KEY = "__dedupe_event_value"
```

Update every internal use site in `transform.py` (the `row[_DEDUPE_DISCRIMINATOR_ROW_KEY] = ...`
assignment and the `del row[_DEDUPE_DISCRIMINATOR_ROW_KEY]` in `transform_events`, and inside
`_dedupe_rows` if Stage 2's key-construction line still references it directly — Task 4 replaces
that particular line anyway, but rename it here first so this task's diff compiles standalone) to
the new name, dropping the leading underscore.

Add, right after `_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"`:

```python
# BAF-11 stage 4 (installs' full pass-through mode, ReportSpec.column_map=None):
# raw AppsFlyer header -> target snake_case column name, with no per-report
# hand-written dict. Reproduces every existing _IN_APP_EVENTS_COLUMN_MAP entry
# exactly (pinned by test_transform.py's
# test_normalize_matches_every_in_app_events_column_map_entry) -- the one
# override below exists ONLY to dodge a real name collision (see the comment
# on it), not to change behavior for anything already mapped by hand.
_RAW_COLUMN_NAME_OVERRIDES: dict[str, str] = {
    "App ID": "appsflyer_app_id",
}


def normalize_column_name(raw: str) -> str:
    """Raw AppsFlyer CSV header -> target snake_case column name."""
    if raw in _RAW_COLUMN_NAME_OVERRIDES:
        return _RAW_COLUMN_NAME_OVERRIDES[raw]
    return raw.strip().lower().replace(" ", "_")
```

- [ ] **Step 4: Run the transform tests, confirm green**

Run: `uv run pytest tests/test_transform.py -v`
Expected: PASS — all tests, including the new `normalize_column_name` ones. (`_dedupe_rows`'s own
tests still reference the renamed constant only internally at this point — Task 4 changes its
signature; this step just confirms the rename alone didn't break anything.)

- [ ] **Step 5: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
Expected: all green.

```bash
git add src/appsflyer_pipeline/transform.py tests/test_transform.py
git commit -m "BAF-11 stage 4: add normalize_column_name, export DEDUPE_DISCRIMINATOR_ROW_KEY"
```

- [ ] **Step 6: Write the failing registry-shape tests for the two new specs**

Add to `tests/test_reports.py`:

```python
def test_registry_covers_installs_alongside_in_app_events() -> None:
    assert set(REPORTS) == {
        "in_app_events_non_organic",
        "in_app_events_retargeting",
        "installs_non_organic",
        "installs_retargeting",
    }


def test_installs_non_organic_spec_matches_the_confirmed_live_endpoint() -> None:
    spec = REPORTS["installs_non_organic"]
    assert spec.name == "installs"
    assert spec.endpoint == "installs_report"
    assert spec.attribution_type == "non_organic"


def test_installs_retargeting_spec_matches_the_confirmed_live_endpoint() -> None:
    spec = REPORTS["installs_retargeting"]
    assert spec.name == "installs"
    assert spec.endpoint == "installs-retarget"
    assert spec.attribution_type == "retargeting"


def test_both_installs_specs_never_send_event_name_but_send_47_additional_fields() -> None:
    for key in ("installs_non_organic", "installs_retargeting"):
        spec = REPORTS[key]
        assert spec.sends_event_name is False
        assert len(spec.additional_fields) == 47
        assert spec.additional_fields == INSTALLS_ADDITIONAL_FIELDS


def test_both_installs_specs_have_a_hard_clamped_60_day_retention() -> None:
    for key in ("installs_non_organic", "installs_retargeting"):
        spec = REPORTS[key]
        assert spec.retention_days == 60
        assert spec.hard_clamp_retention is True
        assert spec.window_column == "install_time"


def test_in_app_events_specs_keep_warn_only_retention_unchanged() -> None:
    for key in ("in_app_events_non_organic", "in_app_events_retargeting"):
        assert REPORTS[key].hard_clamp_retention is False


def test_both_installs_specs_have_column_map_none() -> None:
    for key in ("installs_non_organic", "installs_retargeting"):
        assert REPORTS[key].column_map is None


def test_installs_table_callable_reads_settings_db_table_installs() -> None:
    class _FakeSettings:
        db_table_installs = "appsflyer_installs_fb"

    for key in ("installs_non_organic", "installs_retargeting"):
        assert REPORTS[key].table(_FakeSettings()) == "appsflyer_installs_fb"  # type: ignore[arg-type]


def test_installs_raw_columns_has_128_entries_matching_the_column_sizing_measurement() -> None:
    """Pinned against docs/superpowers/specs/2026-08-13-baf-11-column-sizing.md's
    128-column installs_report/installs-retarget measurement (2026-08-13,
    com.yesimmobile, 2026-08-11: 1,872 and 918 rows).
    """
    assert len(INSTALLS_RAW_COLUMNS) == 128
    assert len(set(INSTALLS_RAW_COLUMNS)) == 128  # no duplicate raw names
    assert "Event Value" in INSTALLS_RAW_COLUMNS
    assert "App ID" in INSTALLS_RAW_COLUMNS


def test_installs_insert_columns_has_no_naming_collisions() -> None:
    spec = REPORTS["installs_non_organic"]
    assert len(spec.insert_columns) == len(set(spec.insert_columns)) == 130  # 128 + 2 injected
    assert "app_id" in spec.insert_columns  # the pipeline's OWN injected column
    assert "appsflyer_app_id" in spec.insert_columns  # AppsFlyer's raw "App ID", renamed
```

Add `from appsflyer_pipeline.reports import INSTALLS_ADDITIONAL_FIELDS, INSTALLS_RAW_COLUMNS` to
this file's existing `from appsflyer_pipeline.reports import REPORTS` import line.

- [ ] **Step 7: Run it, confirm it fails**

Run: `uv run pytest tests/test_reports.py -v 2>&1 | tail -20`
Expected: FAIL — `ImportError` (the new names don't exist in `reports.py` yet) or `KeyError` on the
new `REPORTS` keys.

- [ ] **Step 8: Widen `ReportSpec`, add the installs field lists, the two new specs**

In `src/appsflyer_pipeline/reports.py`, change the top-of-file import block to add `Any`:

```python
from typing import Any
```

(alongside the existing `from collections.abc import Callable, Mapping` and
`from dataclasses import dataclass` lines.)

Add, right after the existing `from appsflyer_pipeline.config import Settings` import:

```python
from appsflyer_pipeline.transform import DEDUPE_DISCRIMINATOR_ROW_KEY, normalize_column_name
```

Widen the dataclass field and append the two new ones (`ReportSpec`'s field list, post-Stage-3):

```python
    column_map: Mapping[str, str] | None
    timestamp_columns: tuple[str, ...]
    required_not_null: tuple[str, ...]
    table: Callable[[Settings], str]
    insert_columns: tuple[str, ...]
    window_column: str
    # BAF-11 stage 4 (installs): a callable, not a column-name tuple, for the
    # same reason `table` is a callable (Stage 3 decision) -- a typo in a key
    # column name is a mypy/runtime error at the definition site, not a
    # silently-wrong tuple discovered downstream.
    dedupe_key: Callable[[dict[str, Any]], tuple[Any, ...]]
    # True only for installs -- see this plan's Architecture decision 3.
    # In-app-events stays False: _iter_work_items must keep yielding
    # chunk_start == the caller's requested start regardless of how old it
    # is, preserving the existing warn-and-proceed-past-the-floor behavior
    # RUNBOOK §9's probes rely on (Stage 3 Architecture decision 6).
    hard_clamp_retention: bool
```

Add the two dedupe-key functions, right after the existing `_in_app_events_table` function:

```python
def _in_app_events_dedupe_key(row: dict[str, Any]) -> tuple[Any, ...]:
    """Reproduces _dedupe_rows' pre-Stage-4 hardcoded key exactly -- BAF-11
    stage 2's (event_time, event_name, appsflyer_id, Event Value) key, byte
    for byte. See this plan's Architecture decision 7 for why installs gets
    its own key instead of reusing this one.
    """
    return (
        row["event_time"],
        row["event_name"],
        row["appsflyer_id"],
        row[DEDUPE_DISCRIMINATOR_ROW_KEY],
    )


def _installs_table(settings: Settings) -> str:
    return settings.db_table_installs


def _installs_dedupe_key(row: dict[str, Any]) -> tuple[Any, ...]:
    """installs' own key (BAF-11 stage 4) -- (appsflyer_id, event_time), NOT
    in-app-events' key. See this plan's Architecture decision 7 for the full
    rationale (Event Value is always empty for installs; Event Name barely
    varies; Install Time is constant per device across installs-retarget's
    re-engagement rows, so it can't discriminate the way it does as
    in-app-events' tiebreak; Event Time is the field that actually varies per
    distinct re-engagement touch). Flagged there as an open question for
    Mark/data-analytics sign-off, not a settled policy.
    """
    return (row["appsflyer_id"], row["event_time"])
```

Add the installs field lists and `insert_columns` derivation, right after the existing
`_IN_APP_EVENTS_INSERT_COLUMNS` tuple:

```python
# The 81 fields AppsFlyer always returns for installs_report/installs-retarget
# regardless of additional_fields -- confirmed identical, same order, to
# in_app_events_report's own 81 default columns (column-sizing spec, both
# sections). Kept as a separate tuple (not reused from in-app-events) because
# the two report families' column sets are conceptually independent even
# though today's measurement happens to make them equal.
_INSTALLS_DEFAULT_FIELDS: tuple[str, ...] = (
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
    "Keywords",
    "Campaign",
    "Campaign ID",
    "Adset",
    "Adset ID",
    "Ad",
    "Ad ID",
    "Ad Type",
    "Site ID",
    "Sub Site ID",
    "Sub Param 1",
    "Sub Param 2",
    "Sub Param 3",
    "Sub Param 4",
    "Sub Param 5",
    "Cost Model",
    "Cost Value",
    "Cost Currency",
    "Contributor 1 Partner",
    "Contributor 1 Media Source",
    "Contributor 1 Campaign",
    "Contributor 1 Touch Type",
    "Contributor 1 Touch Time",
    "Contributor 2 Partner",
    "Contributor 2 Media Source",
    "Contributor 2 Campaign",
    "Contributor 2 Touch Type",
    "Contributor 2 Touch Time",
    "Contributor 3 Partner",
    "Contributor 3 Media Source",
    "Contributor 3 Campaign",
    "Contributor 3 Touch Type",
    "Contributor 3 Touch Time",
    "Region",
    "Country Code",
    "State",
    "City",
    "Postal Code",
    "DMA",
    "IP",
    "WIFI",
    "Operator",
    "Carrier",
    "Language",
    "AppsFlyer ID",
    "Advertising ID",
    "IDFA",
    "Android ID",
    "Customer User ID",
    "IMEI",
    "IDFV",
    "Platform",
    "Device Type",
    "OS Version",
    "App Version",
    "SDK Version",
    "App ID",
    "App Name",
    "Bundle ID",
    "Is Retargeting",
    "Retargeting Conversion Type",
    "Attribution Lookback",
    "Reengagement Window",
    "Is Primary Attribution",
    "User Agent",
    "HTTP Referrer",
    "Original URL",
)

# The 47 fields requested via the API's additional_fields param (confirmed
# live 2026-08-13 -- no 400, all 47 land). Public: reused as-is for
# ReportSpec.additional_fields below, and importable by tests that need to
# build a realistic 128-column fixture CSV without a second hand-typed list.
INSTALLS_ADDITIONAL_FIELDS: tuple[str, ...] = (
    "Store Reinstall",
    "Impressions",
    "Contributor 3 Match Type",
    "Custom Dimension",
    "Conversion Type",
    "Google Play Click Time",
    "Match Type",
    "Mediation Network",
    "OAID",
    "Deeplink URL",
    "Blocked Reason",
    "Blocked Sub Reason",
    "Google Play Broadcast Referrer",
    "Google Play Install Begin Time",
    "Campaign Type",
    "Custom Data",
    "Rejected Reason",
    "Device Download Time",
    "Keyword Match Type",
    "Contributor 1 Match Type",
    "Contributor 2 Match Type",
    "Device Model",
    "Monetization Network",
    "Segment",
    "Is LAT",
    "Google Play Referrer",
    "Blocked Reason Value",
    "Store Product Page",
    "Device Category",
    "App Type",
    "Rejected Reason Value",
    "Ad Unit",
    "Keyword ID",
    "Placement",
    "Network Account ID",
    "Install App Store",
    "Amazon Fire ID",
    "ATT",
    "Engagement Type",
    "Contributor 1 Engagement Type",
    "Contributor 2 Engagement Type",
    "Contributor 3 Engagement Type",
    "GDPR Applies",
    "Ad User Data Enabled",
    "Ad Personalization Enabled",
    "Total Candidates",
    "Engagement Destination",
)

# Public: default + additional, in AppsFlyer's own column order -- the single
# source of truth for "every raw column installs' full pass-through mode
# expects." Reused to derive insert_columns below and by
# sql/create_table_installs.sql / loader.py's DDL template (Task 3), which
# MUST list its columns in this exact order.
INSTALLS_RAW_COLUMNS: tuple[str, ...] = (*_INSTALLS_DEFAULT_FIELDS, *INSTALLS_ADDITIONAL_FIELDS)

_INSTALLS_INSERT_COLUMNS: tuple[str, ...] = tuple(
    normalize_column_name(raw) for raw in INSTALLS_RAW_COLUMNS
) + ("attribution_type", "app_id")
```

Update the two existing `REPORTS` entries to add the two new required fields (dataclass fields
without defaults must be supplied on every instantiation):

```python
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
        dedupe_key=_in_app_events_dedupe_key,
        hard_clamp_retention=False,
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
        dedupe_key=_in_app_events_dedupe_key,
        hard_clamp_retention=False,
    ),
    "installs_non_organic": ReportSpec(
        name="installs",
        endpoint="installs_report",
        attribution_type="non_organic",
        sends_event_name=False,
        sends_media_source=True,
        additional_fields=INSTALLS_ADDITIONAL_FIELDS,
        retention_days=60,
        column_map=None,
        timestamp_columns=("event_time", "install_time", "attributed_touch_time"),
        required_not_null=("appsflyer_id", "install_time", "event_time"),
        table=_installs_table,
        insert_columns=_INSTALLS_INSERT_COLUMNS,
        window_column="install_time",
        dedupe_key=_installs_dedupe_key,
        hard_clamp_retention=True,
    ),
    "installs_retargeting": ReportSpec(
        name="installs",
        endpoint="installs-retarget",
        attribution_type="retargeting",
        sends_event_name=False,
        sends_media_source=True,
        additional_fields=INSTALLS_ADDITIONAL_FIELDS,
        retention_days=60,
        column_map=None,
        timestamp_columns=("event_time", "install_time", "attributed_touch_time"),
        required_not_null=("appsflyer_id", "install_time", "event_time"),
        table=_installs_table,
        insert_columns=_INSTALLS_INSERT_COLUMNS,
        window_column="install_time",
        dedupe_key=_installs_dedupe_key,
        hard_clamp_retention=True,
    ),
```

- [ ] **Step 9: Run the reports tests, confirm green**

Run: `uv run pytest tests/test_reports.py -v`
Expected: PASS — all tests, including every new one from Step 6. (`test_insert_columns_match_...`
from Stage 3, which iterates `REPORTS.values()` and calls `transform_events`, will FAIL at this
point for the two installs specs — `transform_events` doesn't understand `column_map=None` yet.
That's Task 4's job; narrow this run to `-k "not insert_columns_match"` if it's in the way, and
confirm it goes green again after Task 4.)

- [ ] **Step 10: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest -k "not insert_columns_match and not test_run_backfill_default_window_is_90_days"`
Expected: all green except the two known-pending tests excluded above.
`test_insert_columns_match_...` (`test_reports.py`) is fixed by Task 4. **The moment this task
registers installs' `retention_days=60` specs in `REPORTS`, `test_run_backfill_default_window_is_90_days`
(`tests/test_pipeline.py`) also starts failing** — Stage 3's unmodified `_active_retention_days()`
(a `min(...)` over `REPORTS`) drops from 90 to 60, and `run_backfill`'s Stage-3 default-window math
already consumes it, even though this task never touches `pipeline.py`; Task 5 fixes it (see that
task's Architecture decision 3 addendum).

```bash
git add src/appsflyer_pipeline/reports.py tests/test_reports.py
git commit -m "BAF-11 stage 4: add installs/installs_retarget ReportSpecs"
```

---

### Task 3: installs DDL + `create_table`'s per-report template + CLI multi-schema `create-table`

**Files:**
- Modify: `src/appsflyer_pipeline/loader.py`
- Create: `sql/create_table_installs.sql`
- Modify: `src/appsflyer_pipeline/cli.py`
- Modify: `tests/test_loader.py`, `tests/test_loader_integration.py`, `tests/test_cli.py`

**Interfaces:**
- Consumes: `appsflyer_pipeline.reports.REPORTS`, `ReportSpec.name`/`.table`.
- Produces: `create_table(engine: Engine, table_name: str, report_name: str) -> None` (was
  `(engine, table_name)` — gains the third parameter; see Architecture decision 4).
  `check_connection`/`load_events`/`_validate_identifier` are untouched (Stage 3 already made
  `load_events` fully generic via `spec.window_column`/`spec.insert_columns` — no further change
  needed here). `cli.py`'s `check_connection_command`/`create_table_command` (Stage 3-generalized to
  iterate `{spec.table(settings) for spec in REPORTS.values()}`) now iterate two distinct tables
  instead of one.

**Byte-budget check (Architecture decision 6):** 128 mapped columns + `id`/`attribution_type`/
`app_id` sized as below sums to **~28,371 bytes** of MySQL/MariaDB's 65,535-byte row limit — 3
`DATETIME` (24B) + 3 `DECIMAL(18,4)` (27B) + 8 `TEXT` (~96B, off-page pointers) + 24 `VARCHAR(16)`
(1,584B) + 28 `VARCHAR(32)` (3,640B) + 41 `VARCHAR(64)` (10,578B) + 19 `VARCHAR(128)` (9,766B) + 2
`VARCHAR(255)` (2,044B) = 27,759B, + `id BIGINT UNSIGNED` (8B) + `attribution_type VARCHAR(50)`
(202B) + `app_id VARCHAR(100)` (402B) = **28,371B total**, comfortably under the limit with room to
widen individual columns later if a longer real value ever truncates.

- [ ] **Step 1: Write the failing DDL-shape test**

Add to `tests/test_loader.py`:

```python
from appsflyer_pipeline.reports import REPORTS


def test_create_table_installs_ddl_covers_every_insert_column() -> None:
    """Regression test for the master spec's Этап 6 acceptance criterion:
    the installs DDL must cover exactly the 130-column set (128 mapped +
    attribution_type + app_id) transform_events() will actually produce.
    """
    spec = REPORTS["installs_non_organic"]
    ddl = _CREATE_TABLE_TEMPLATE_BY_REPORT_NAME["installs"].format(table="t")
    for column in spec.insert_columns:
        assert f"`{column}`" in ddl, f"DDL is missing column {column!r}"
    assert "PRIMARY KEY (`id`)" in ddl
    assert "idx_app_attr_install" in ddl
    assert "install_time" in ddl


def test_create_table_template_registry_covers_every_report_name() -> None:
    for spec in REPORTS.values():
        assert spec.name in _CREATE_TABLE_TEMPLATE_BY_REPORT_NAME
```

Add `from appsflyer_pipeline.loader import _CREATE_TABLE_TEMPLATE_BY_REPORT_NAME` to this file's
existing `loader` import block.

- [ ] **Step 2: Run it, confirm it fails**

Run: `uv run pytest tests/test_loader.py -k create_table_installs -v`
Expected: FAIL — `ImportError` (`_CREATE_TABLE_TEMPLATE_BY_REPORT_NAME` doesn't exist yet).

- [ ] **Step 3: Rename the existing template, add the installs template, generalize `create_table`**

In `src/appsflyer_pipeline/loader.py`, rename `_CREATE_TABLE_TEMPLATE` to
`_IN_APP_EVENTS_CREATE_TABLE_TEMPLATE` (its content is unchanged — this is a pure rename to make
room for a second template). Add the installs template right after it:

```python
# installs/installs_retarget DDL (BAF-11 stage 5/6) -- 128 columns per the
# live column-sizing measurement (docs/superpowers/specs/2026-08-13-baf-11-column-sizing.md),
# mirrored in sql/create_table_installs.sql. Column order matches
# reports.INSTALLS_RAW_COLUMNS exactly (normalized). Empty-in-sample columns
# are typed by semantic category, not guessed per-column -- see this plan's
# Architecture decision 6 for the categorization rule and the byte-budget
# arithmetic (~28.4 KB of the 65,535-byte row limit).
# `install_time`/`event_time`/`appsflyer_id` are NOT NULL: guaranteed
# non-blank by ReportSpec.required_not_null before a row ever reaches here
# (transform.transform_events skips-and-counts a row missing any of them).
_INSTALLS_CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS `{table}` (
    `id`                              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `attributed_touch_type`           VARCHAR(32)    NULL,
    `attributed_touch_time`           DATETIME       NULL,
    `install_time`                    DATETIME       NOT NULL,
    `event_time`                      DATETIME       NOT NULL,
    `event_name`                      VARCHAR(32)    NULL,
    `event_value`                     TEXT           NULL,
    `event_revenue`                   DECIMAL(18,4)  NULL,
    `event_revenue_currency`          VARCHAR(16)    NULL,
    `event_revenue_usd`               DECIMAL(18,4)  NULL,
    `event_source`                    VARCHAR(16)    NULL,
    `is_receipt_validated`            VARCHAR(16)    NULL,
    `partner`                         VARCHAR(64)    NULL,
    `media_source`                    VARCHAR(64)    NULL,
    `channel`                         VARCHAR(32)    NULL,
    `keywords`                        VARCHAR(128)   NULL,
    `campaign`                        VARCHAR(128)   NULL,
    `campaign_id`                     VARCHAR(64)    NULL,
    `adset`                           VARCHAR(128)   NULL,
    `adset_id`                        VARCHAR(64)    NULL,
    `ad`                              VARCHAR(128)   NULL,
    `ad_id`                           VARCHAR(64)    NULL,
    `ad_type`                         VARCHAR(64)    NULL,
    `site_id`                         VARCHAR(128)   NULL,
    `sub_site_id`                     VARCHAR(64)    NULL,
    `sub_param_1`                     VARCHAR(16)    NULL,
    `sub_param_2`                     VARCHAR(64)    NULL,
    `sub_param_3`                     VARCHAR(64)    NULL,
    `sub_param_4`                     VARCHAR(255)   NULL,
    `sub_param_5`                     VARCHAR(128)   NULL,
    `cost_model`                      VARCHAR(32)    NULL,
    `cost_value`                      DECIMAL(18,4)  NULL,
    `cost_currency`                   VARCHAR(16)    NULL,
    `contributor_1_partner`           VARCHAR(64)    NULL,
    `contributor_1_media_source`      VARCHAR(64)    NULL,
    `contributor_1_campaign`          VARCHAR(128)   NULL,
    `contributor_1_touch_type`        VARCHAR(32)    NULL,
    `contributor_1_touch_time`        VARCHAR(64)    NULL,
    `contributor_2_partner`           VARCHAR(64)    NULL,
    `contributor_2_media_source`      VARCHAR(64)    NULL,
    `contributor_2_campaign`          VARCHAR(128)   NULL,
    `contributor_2_touch_type`        VARCHAR(32)    NULL,
    `contributor_2_touch_time`        VARCHAR(64)    NULL,
    `contributor_3_partner`           VARCHAR(64)    NULL,
    `contributor_3_media_source`      VARCHAR(64)    NULL,
    `contributor_3_campaign`          VARCHAR(128)   NULL,
    `contributor_3_touch_type`        VARCHAR(32)    NULL,
    `contributor_3_touch_time`        VARCHAR(64)    NULL,
    `region`                          VARCHAR(16)    NULL,
    `country_code`                    VARCHAR(16)    NULL,
    `state`                           VARCHAR(32)    NULL,
    `city`                            VARCHAR(64)    NULL,
    `postal_code`                     VARCHAR(32)    NULL,
    `dma`                             VARCHAR(16)    NULL,
    `ip`                              VARCHAR(32)    NULL,
    `wifi`                            VARCHAR(16)    NULL,
    `operator`                        VARCHAR(128)   NULL,
    `carrier`                         VARCHAR(128)   NULL,
    `language`                        VARCHAR(32)    NULL,
    `appsflyer_id`                    VARCHAR(128)   NOT NULL,
    `advertising_id`                  VARCHAR(128)   NULL,
    `idfa`                            VARCHAR(128)   NULL,
    `android_id`                      VARCHAR(128)   NULL,
    `customer_user_id`                VARCHAR(255)   NULL,
    `imei`                            VARCHAR(64)    NULL,
    `idfv`                            VARCHAR(128)   NULL,
    `platform`                        VARCHAR(16)    NULL,
    `device_type`                     VARCHAR(32)    NULL,
    `os_version`                      VARCHAR(16)    NULL,
    `app_version`                     VARCHAR(32)    NULL,
    `sdk_version`                     VARCHAR(16)    NULL,
    `appsflyer_app_id`                VARCHAR(32)    NULL,
    `app_name`                        VARCHAR(128)   NULL,
    `bundle_id`                       VARCHAR(64)    NULL,
    `is_retargeting`                  VARCHAR(16)    NULL,
    `retargeting_conversion_type`     VARCHAR(32)    NULL,
    `attribution_lookback`            VARCHAR(16)    NULL,
    `reengagement_window`             VARCHAR(16)    NULL,
    `is_primary_attribution`          VARCHAR(16)    NULL,
    `user_agent`                      TEXT           NULL,
    `http_referrer`                   TEXT           NULL,
    `original_url`                    TEXT           NULL,
    `store_reinstall`                 VARCHAR(16)    NULL,
    `impressions`                     VARCHAR(32)    NULL,
    `contributor_3_match_type`        VARCHAR(32)    NULL,
    `custom_dimension`                VARCHAR(64)    NULL,
    `conversion_type`                 VARCHAR(32)    NULL,
    `google_play_click_time`          VARCHAR(64)    NULL,
    `match_type`                      VARCHAR(32)    NULL,
    `mediation_network`               VARCHAR(64)    NULL,
    `oaid`                            VARCHAR(64)    NULL,
    `deeplink_url`                    TEXT           NULL,
    `blocked_reason`                  VARCHAR(64)    NULL,
    `blocked_sub_reason`              VARCHAR(64)    NULL,
    `google_play_broadcast_referrer`  TEXT           NULL,
    `google_play_install_begin_time`  VARCHAR(64)    NULL,
    `campaign_type`                   VARCHAR(32)    NULL,
    `custom_data`                     TEXT           NULL,
    `rejected_reason`                 VARCHAR(64)    NULL,
    `device_download_time`            VARCHAR(64)    NULL,
    `keyword_match_type`              VARCHAR(16)    NULL,
    `contributor_1_match_type`        VARCHAR(32)    NULL,
    `contributor_2_match_type`        VARCHAR(32)    NULL,
    `device_model`                    VARCHAR(128)   NULL,
    `monetization_network`            VARCHAR(64)    NULL,
    `segment`                         VARCHAR(64)    NULL,
    `is_lat`                          VARCHAR(16)    NULL,
    `google_play_referrer`            TEXT           NULL,
    `blocked_reason_value`            VARCHAR(32)    NULL,
    `store_product_page`              VARCHAR(32)    NULL,
    `device_category`                 VARCHAR(64)    NULL,
    `app_type`                        VARCHAR(32)    NULL,
    `rejected_reason_value`           VARCHAR(32)    NULL,
    `ad_unit`                         VARCHAR(64)    NULL,
    `keyword_id`                      VARCHAR(64)    NULL,
    `placement`                       VARCHAR(64)    NULL,
    `network_account_id`              VARCHAR(64)    NULL,
    `install_app_store`               VARCHAR(32)    NULL,
    `amazon_fire_id`                  VARCHAR(128)   NULL,
    `att`                             VARCHAR(16)    NULL,
    `engagement_type`                 VARCHAR(64)    NULL,
    `contributor_1_engagement_type`   VARCHAR(64)    NULL,
    `contributor_2_engagement_type`   VARCHAR(64)    NULL,
    `contributor_3_engagement_type`   VARCHAR(64)    NULL,
    `gdpr_applies`                    VARCHAR(16)    NULL,
    `ad_user_data_enabled`            VARCHAR(16)    NULL,
    `ad_personalization_enabled`      VARCHAR(16)    NULL,
    `total_candidates`                VARCHAR(16)    NULL,
    `engagement_destination`          VARCHAR(32)    NULL,
    `attribution_type`                VARCHAR(50)    NOT NULL,
    `app_id`                          VARCHAR(100)   NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_app_attr_install` (`app_id`, `attribution_type`, `install_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

# BAF-11 stage 4: selects the DDL template by ReportSpec.name (not by table
# name -- an operator-configured string tells us nothing about which columns
# a table should have). Every REPORTS entry's `.name` must be a key here;
# pinned by test_loader.py's test_create_table_template_registry_covers_every_report_name.
_CREATE_TABLE_TEMPLATE_BY_REPORT_NAME: dict[str, str] = {
    "in_app_events": _IN_APP_EVENTS_CREATE_TABLE_TEMPLATE,
    "installs": _INSTALLS_CREATE_TABLE_TEMPLATE,
}
```

Change `create_table`'s signature and body from:

```python
def create_table(engine: Engine, table_name: str) -> None:
    """Create the target table if it doesn't already exist (idempotent)."""
    table_name = _validate_identifier(table_name)
    ddl = _CREATE_TABLE_TEMPLATE.format(table=table_name)
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
    except SQLAlchemyError as exc:
        raise PipelineError(f"Could not create table `{table_name}`: {exc}") from exc
```

to:

```python
def create_table(engine: Engine, table_name: str, report_name: str) -> None:
    """Create the target table if it doesn't already exist (idempotent).

    `report_name` selects the DDL shape (ReportSpec.name -- "in_app_events" or
    "installs" today) -- the two report families have different schemas.
    """
    table_name = _validate_identifier(table_name)
    try:
        ddl_template = _CREATE_TABLE_TEMPLATE_BY_REPORT_NAME[report_name]
    except KeyError as exc:
        raise PipelineError(f"No DDL template registered for report {report_name!r}") from exc
    ddl = ddl_template.format(table=table_name)
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
    except SQLAlchemyError as exc:
        raise PipelineError(f"Could not create table `{table_name}`: {exc}") from exc
```

- [ ] **Step 4: Run the loader tests, confirm the expected breakage then fix it**

Run: `uv run pytest tests/test_loader.py -v 2>&1 | tail -20`
Expected: FAIL — any existing call to `create_table(engine, table_name)` (two positional args) now
raises `TypeError: create_table() missing 1 required positional argument: 'report_name'`.

Fix every such call site in `tests/test_loader.py` and `tests/test_loader_integration.py` by adding
the report name. `tests/test_loader_integration.py`'s three `create_table(engine, settings.db_table,
...)`-shaped calls (in `test_check_connection_reports_missing_table` — no, that one only calls
`check_connection`, unaffected; the real call sites are `test_create_table_is_idempotent`'s two
calls) become:

```python
def test_create_table_is_idempotent() -> None:
    try:
        settings = get_settings()
        engine = create_engine(settings)
        create_table(engine, settings.db_table, REPORTS["in_app_events_non_organic"].name)
        create_table(engine, settings.db_table, REPORTS["in_app_events_non_organic"].name)
        status = check_connection(engine, settings.db_table)
    except Exception as exc:
        pytest.skip(f"no usable database in this environment: {exc}")

    assert status.table_exists is True
```

Add `from appsflyer_pipeline.reports import REPORTS` to this file's imports. Any lambda in
`tests/test_loader.py` monkeypatching `create_table` with a 2-arg signature (there are none as of
Stage 3 — `create_table` isn't monkeypatched at the loader-test level, only via `cli.py` in
`test_cli.py`, handled in Step 6 below) needs no change here.

- [ ] **Step 5: Run the loader tests again, confirm green**

Run: `uv run pytest tests/test_loader.py tests/test_loader_integration.py -v`
Expected: PASS (integration tests PASS against CI's `mysql:8`, or SKIP locally without a reachable
DB — either is fine per this repo's established precedent).

- [ ] **Step 6: Create `sql/create_table_installs.sql`, mirroring the loader template**

Create `sql/create_table_installs.sql`:

```sql
-- BAF-11 stage 4: installs/installs_retarget (installs_report + installs-retarget v5
-- endpoints), full 128-column pass-through. Column set/types per the live measurement in
-- docs/superpowers/specs/2026-08-13-baf-11-column-sizing.md (2026-08-13, com.yesimmobile,
-- 2026-08-11: 1,872 installs_report rows, 918 installs-retarget rows). Empty-in-sample
-- columns are typed by semantic category, not guessed per-column -- see
-- docs/superpowers/plans/2026-08-31-baf-11-stage-4-installs-report.md's Architecture
-- decision 6 for the categorization rule and the byte-budget arithmetic (~28.4 KB of the
-- 65,535-byte row limit).
--
-- `install_time`/`event_time`/`appsflyer_id` are NOT NULL: ReportSpec.required_not_null
-- guarantees this before a row ever reaches the INSERT (a row missing any of them is
-- skipped-and-counted, not persisted with a blank).
--
-- installs' own dedupe key is (appsflyer_id, event_time) -- NOT in-app-events' key. See
-- the plan above (Architecture decision 7) for the full rationale and the open risk
-- flagged for Mark/data-analytics sign-off.
--
-- This file documents the schema for reference and manual execution; `appsflyer-pipeline
-- create-table` creates it programmatically (idempotent) using the table name configured
-- via DB_TABLE_INSTALLS -- keep the two in sync.

CREATE TABLE IF NOT EXISTS `appsflyer_installs_fb` (
    -- (column list identical to loader._INSTALLS_CREATE_TABLE_TEMPLATE above, with
    --  `{table}` replaced by the literal table name)
    `id`                              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `attributed_touch_type`           VARCHAR(32)    NULL,
    `attributed_touch_time`           DATETIME       NULL,
    `install_time`                    DATETIME       NOT NULL,
    `event_time`                      DATETIME       NOT NULL,
    `event_name`                      VARCHAR(32)    NULL,
    `event_value`                     TEXT           NULL,
    `event_revenue`                   DECIMAL(18,4)  NULL,
    `event_revenue_currency`          VARCHAR(16)    NULL,
    `event_revenue_usd`               DECIMAL(18,4)  NULL,
    `event_source`                    VARCHAR(16)    NULL,
    `is_receipt_validated`            VARCHAR(16)    NULL,
    `partner`                         VARCHAR(64)    NULL,
    `media_source`                    VARCHAR(64)    NULL,
    `channel`                         VARCHAR(32)    NULL,
    `keywords`                        VARCHAR(128)   NULL,
    `campaign`                        VARCHAR(128)   NULL,
    `campaign_id`                     VARCHAR(64)    NULL,
    `adset`                           VARCHAR(128)   NULL,
    `adset_id`                        VARCHAR(64)    NULL,
    `ad`                              VARCHAR(128)   NULL,
    `ad_id`                           VARCHAR(64)    NULL,
    `ad_type`                         VARCHAR(64)    NULL,
    `site_id`                         VARCHAR(128)   NULL,
    `sub_site_id`                     VARCHAR(64)    NULL,
    `sub_param_1`                     VARCHAR(16)    NULL,
    `sub_param_2`                     VARCHAR(64)    NULL,
    `sub_param_3`                     VARCHAR(64)    NULL,
    `sub_param_4`                     VARCHAR(255)   NULL,
    `sub_param_5`                     VARCHAR(128)   NULL,
    `cost_model`                      VARCHAR(32)    NULL,
    `cost_value`                      DECIMAL(18,4)  NULL,
    `cost_currency`                   VARCHAR(16)    NULL,
    `contributor_1_partner`           VARCHAR(64)    NULL,
    `contributor_1_media_source`      VARCHAR(64)    NULL,
    `contributor_1_campaign`          VARCHAR(128)   NULL,
    `contributor_1_touch_type`        VARCHAR(32)    NULL,
    `contributor_1_touch_time`        VARCHAR(64)    NULL,
    `contributor_2_partner`           VARCHAR(64)    NULL,
    `contributor_2_media_source`      VARCHAR(64)    NULL,
    `contributor_2_campaign`          VARCHAR(128)   NULL,
    `contributor_2_touch_type`        VARCHAR(32)    NULL,
    `contributor_2_touch_time`        VARCHAR(64)    NULL,
    `contributor_3_partner`           VARCHAR(64)    NULL,
    `contributor_3_media_source`      VARCHAR(64)    NULL,
    `contributor_3_campaign`          VARCHAR(128)   NULL,
    `contributor_3_touch_type`        VARCHAR(32)    NULL,
    `contributor_3_touch_time`        VARCHAR(64)    NULL,
    `region`                          VARCHAR(16)    NULL,
    `country_code`                    VARCHAR(16)    NULL,
    `state`                           VARCHAR(32)    NULL,
    `city`                            VARCHAR(64)    NULL,
    `postal_code`                     VARCHAR(32)    NULL,
    `dma`                             VARCHAR(16)    NULL,
    `ip`                              VARCHAR(32)    NULL,
    `wifi`                            VARCHAR(16)    NULL,
    `operator`                        VARCHAR(128)   NULL,
    `carrier`                         VARCHAR(128)   NULL,
    `language`                        VARCHAR(32)    NULL,
    `appsflyer_id`                    VARCHAR(128)   NOT NULL,
    `advertising_id`                  VARCHAR(128)   NULL,
    `idfa`                            VARCHAR(128)   NULL,
    `android_id`                      VARCHAR(128)   NULL,
    `customer_user_id`                VARCHAR(255)   NULL,
    `imei`                            VARCHAR(64)    NULL,
    `idfv`                            VARCHAR(128)   NULL,
    `platform`                        VARCHAR(16)    NULL,
    `device_type`                     VARCHAR(32)    NULL,
    `os_version`                      VARCHAR(16)    NULL,
    `app_version`                     VARCHAR(32)    NULL,
    `sdk_version`                     VARCHAR(16)    NULL,
    `appsflyer_app_id`                VARCHAR(32)    NULL,
    `app_name`                        VARCHAR(128)   NULL,
    `bundle_id`                       VARCHAR(64)    NULL,
    `is_retargeting`                  VARCHAR(16)    NULL,
    `retargeting_conversion_type`     VARCHAR(32)    NULL,
    `attribution_lookback`            VARCHAR(16)    NULL,
    `reengagement_window`             VARCHAR(16)    NULL,
    `is_primary_attribution`          VARCHAR(16)    NULL,
    `user_agent`                      TEXT           NULL,
    `http_referrer`                   TEXT           NULL,
    `original_url`                    TEXT           NULL,
    `store_reinstall`                 VARCHAR(16)    NULL,
    `impressions`                     VARCHAR(32)    NULL,
    `contributor_3_match_type`        VARCHAR(32)    NULL,
    `custom_dimension`                VARCHAR(64)    NULL,
    `conversion_type`                 VARCHAR(32)    NULL,
    `google_play_click_time`          VARCHAR(64)    NULL,
    `match_type`                      VARCHAR(32)    NULL,
    `mediation_network`               VARCHAR(64)    NULL,
    `oaid`                            VARCHAR(64)    NULL,
    `deeplink_url`                    TEXT           NULL,
    `blocked_reason`                  VARCHAR(64)    NULL,
    `blocked_sub_reason`              VARCHAR(64)    NULL,
    `google_play_broadcast_referrer`  TEXT           NULL,
    `google_play_install_begin_time`  VARCHAR(64)    NULL,
    `campaign_type`                   VARCHAR(32)    NULL,
    `custom_data`                     TEXT           NULL,
    `rejected_reason`                 VARCHAR(64)    NULL,
    `device_download_time`            VARCHAR(64)    NULL,
    `keyword_match_type`              VARCHAR(16)    NULL,
    `contributor_1_match_type`        VARCHAR(32)    NULL,
    `contributor_2_match_type`        VARCHAR(32)    NULL,
    `device_model`                    VARCHAR(128)   NULL,
    `monetization_network`            VARCHAR(64)    NULL,
    `segment`                         VARCHAR(64)    NULL,
    `is_lat`                          VARCHAR(16)    NULL,
    `google_play_referrer`            TEXT           NULL,
    `blocked_reason_value`            VARCHAR(32)    NULL,
    `store_product_page`              VARCHAR(32)    NULL,
    `device_category`                 VARCHAR(64)    NULL,
    `app_type`                        VARCHAR(32)    NULL,
    `rejected_reason_value`           VARCHAR(32)    NULL,
    `ad_unit`                         VARCHAR(64)    NULL,
    `keyword_id`                      VARCHAR(64)    NULL,
    `placement`                       VARCHAR(64)    NULL,
    `network_account_id`              VARCHAR(64)    NULL,
    `install_app_store`               VARCHAR(32)    NULL,
    `amazon_fire_id`                  VARCHAR(128)   NULL,
    `att`                             VARCHAR(16)    NULL,
    `engagement_type`                 VARCHAR(64)    NULL,
    `contributor_1_engagement_type`   VARCHAR(64)    NULL,
    `contributor_2_engagement_type`   VARCHAR(64)    NULL,
    `contributor_3_engagement_type`   VARCHAR(64)    NULL,
    `gdpr_applies`                    VARCHAR(16)    NULL,
    `ad_user_data_enabled`            VARCHAR(16)    NULL,
    `ad_personalization_enabled`      VARCHAR(16)    NULL,
    `total_candidates`                VARCHAR(16)    NULL,
    `engagement_destination`          VARCHAR(32)    NULL,
    `attribution_type`                VARCHAR(50)    NOT NULL,
    `app_id`                          VARCHAR(100)   NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_app_attr_install` (`app_id`, `attribution_type`, `install_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

- [ ] **Step 7: Generalize `cli.py`'s `create_table_command` to pass `report_name`**

In `src/appsflyer_pipeline/cli.py`, change `create_table_command` (Stage-3-generalized shape) from:

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

to:

```python
@app.command(name="create-table")
def create_table_command() -> None:
    """Create every active report's target table if it doesn't already exist
    (idempotent). BAF-11 stage 4: two distinct tables -- in-app-events'
    17-column schema and installs' 128-column one.
    """
    settings = _get_settings_or_exit()
    engine = create_engine(settings)
    tables = sorted({(spec.table(settings), spec.name) for spec in REPORTS.values()})
    try:
        for table, report_name in tables:
            create_table(engine, table, report_name)
    except PipelineError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    for table, _report_name in tables:
        typer.echo(f"Table `{table}` is ready.")
```

`check_connection_command` needs **no change** — it already iterates `{spec.table(settings) for
spec in REPORTS.values()}` (a set of table names alone, `check_connection`'s signature untouched by
this task), and now naturally produces two distinct rows of output instead of one.

- [ ] **Step 8: Run the CLI tests, fix the `create_table` monkeypatch signatures**

Run: `uv run pytest tests/test_cli.py -v 2>&1 | tail -30`
Expected: FAIL — `test_create_table_success_reports_ready` and `test_create_table_reports_failure`
monkeypatch `cli.create_table` with a 2-arg lambda (`lambda engine, table_name: None`), which now
raises `TypeError` when called with 3 positional args.

Fix both in `tests/test_cli.py`:

```python
def test_create_table_success_reports_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_cli_env(monkeypatch)
    monkeypatch.setattr(cli, "create_table", lambda engine, table_name, report_name: None)

    result = runner.invoke(app, ["create-table"])

    get_settings.cache_clear()
    assert result.exit_code == 0
    assert "is ready." in result.output
    assert result.output.count("is ready.") == 2  # BAF-11 stage 4: two distinct tables now
```

```python
def test_create_table_reports_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_cli_env(monkeypatch)

    def _raise(engine: object, table_name: str, report_name: str) -> None:
        raise PipelineError(f"Could not create table `{table_name}`: boom")

    monkeypatch.setattr(cli, "create_table", _raise)

    result = runner.invoke(app, ["create-table"])

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAILED" in result.output
```

`test_check_connection_reports_status_for_both_branches` needs no change (its `check_connection`
lambda's signature — `(engine, table_name)` — is untouched); its assertion
(`expected_fragment in result.output`) still holds trivially with two rows of output instead of one.

- [ ] **Step 9: Run the full CLI test file, confirm green**

Run: `uv run pytest tests/test_cli.py -v`
Expected: PASS — all tests.

- [ ] **Step 10: Regression test — DELETE predicate built on `install_time`, not `event_time`**

This confirms Stage 3's `load_events(engine, spec, table_name, rows, ...)` (already generic via
`spec.window_column`) produces the right SQL for installs specifically — no new production code,
test-only. `load_events` builds `delete_stmt = text(...)` (Stage 3: using `spec.window_column`, not
a hardcoded `event_time`) **before** it ever opens a connection, so monkeypatching `loader.text` to
record its argument captures the real DELETE SQL even though the connection itself then fails
against `_unreachable_engine()`. Add `from appsflyer_pipeline import loader` to this file's imports
(alongside the existing `from appsflyer_pipeline.loader import ...` block), then add to
`tests/test_loader.py`:

```python
def test_load_events_deletes_on_install_time_for_installs_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression test for the master spec's Этап 6 test list: installs'
    DELETE predicate must key on install_time (spec.window_column), not
    event_time -- the two report families disagree on which timestamp bounds
    their window.
    """
    captured_sql: list[str] = []
    real_text = loader.text

    def _capturing_text(sql_string: str) -> object:
        captured_sql.append(sql_string)
        return real_text(sql_string)

    monkeypatch.setattr(loader, "text", _capturing_text)
    engine = _unreachable_engine()
    spec = REPORTS["installs_non_organic"]

    with pytest.raises(PipelineError):
        load_events(
            engine,
            spec,
            "installs_table",
            [],
            app_id="app1",
            start_date=datetime.date(2026, 1, 1),
            end_date=datetime.date(2026, 1, 1),
        )

    delete_sql = captured_sql[0]  # text() is called for DELETE before INSERT
    assert "`install_time`" in delete_sql
    assert "`event_time`" not in delete_sql
```

- [ ] **Step 11: Run the full loader + CLI suite once more**

Run: `uv run pytest tests/test_loader.py tests/test_loader_integration.py tests/test_cli.py -v`
Expected: PASS.

- [ ] **Step 12: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest -k "not insert_columns_match and not test_run_backfill_default_window_is_90_days"`
Expected: all green (the two Task-2-deferred `test_insert_columns_match_...` cases in
`test_reports.py` are still pending — Task 4 fixes them; `test_run_backfill_default_window_is_90_days`
is still pending too — Task 5 fixes it; if this run isn't green because of exactly those three, that's
expected at this point).

```bash
git add src/appsflyer_pipeline/loader.py src/appsflyer_pipeline/cli.py sql/create_table_installs.sql \
  tests/test_loader.py tests/test_loader_integration.py tests/test_cli.py
git commit -m "BAF-11 stage 4: installs DDL, per-report create_table, multi-schema create-table CLI"
```

---

### Task 4: `transform.py` — full pass-through mode + generalized `_dedupe_rows`

**Files:**
- Modify: `src/appsflyer_pipeline/transform.py`
- Modify: `tests/test_transform.py`, `tests/test_reports.py`

**Interfaces:**
- Consumes: `spec.column_map` (now possibly `None`), `spec.dedupe_key` (new), `spec.insert_columns`
  (used for pass-through validation, not just the INSERT column list).
- Produces: `_dedupe_rows(rows, *, key_fn, attribution_type, app_id)` (was `(rows, *,
  attribution_type, app_id)` with a hardcoded key). `transform_events`'s public signature is
  unchanged (`spec`/`app_id`/`media_source_filter`/`event_names_filter`, from Stage 3) — only its
  body branches on `spec.column_map is None`.

- [ ] **Step 1: Write the failing pass-through mapping test**

Add to `tests/test_transform.py`:

```python
from appsflyer_pipeline.reports import REPORTS, INSTALLS_RAW_COLUMNS


def _installs_csv_row(**overrides: str) -> str:
    """Builds a full 128-column installs CSV (header + one row) from
    reports.INSTALLS_RAW_COLUMNS, so this fixture can never silently drift
    from the real column set -- every value defaults to empty (matching the
    live measurement's mostly-empty additional_fields) except the handful
    overridden below or via **overrides.
    """
    values: dict[str, str] = dict.fromkeys(INSTALLS_RAW_COLUMNS, "")
    values.update(
        {
            "AppsFlyer ID": "af-installs-1",
            "Install Time": "2026-05-19 09:30:00",
            "Event Time": "2026-05-19 09:30:00",
            "Attributed Touch Time": "2026-05-19 09:00:00",
            "Event Name": "install",
            "Media Source": "Facebook Ads",
        }
    )
    values.update(overrides)
    header = ",".join(INSTALLS_RAW_COLUMNS)
    row = ",".join(values[c] for c in INSTALLS_RAW_COLUMNS)
    return f"{header}\n{row}\n"


def _installs_df(**overrides: str) -> pl.DataFrame:
    csv_text = _installs_csv_row(**overrides)
    return pl.read_csv(io.StringIO(csv_text), infer_schema_length=0)


def test_transform_installs_maps_all_128_columns_via_normalization() -> None:
    """Master spec Этап 6 acceptance criterion: all 128 columns land in the
    row -- proven against reports.INSTALLS_RAW_COLUMNS, not a second
    hand-typed list.
    """
    df = _installs_df()
    rows = transform_events(
        df,
        spec=REPORTS["installs_non_organic"],
        app_id="com.yesimmobile",
        media_source_filter=None,
        event_names_filter=None,
    )
    assert len(rows) == 1
    assert set(rows[0]) == set(REPORTS["installs_non_organic"].insert_columns)
    assert rows[0]["appsflyer_id"] == "af-installs-1"
    assert rows[0]["app_id"] == "com.yesimmobile"  # the pipeline's OWN injected value
    assert rows[0]["attribution_type"] == "non_organic"


def test_transform_installs_renames_raw_app_id_avoiding_collision() -> None:
    df = _installs_df(**{"App ID": "1458505230"})
    rows = transform_events(
        df,
        spec=REPORTS["installs_non_organic"],
        app_id="com.yesimmobile",
        media_source_filter=None,
        event_names_filter=None,
    )
    assert rows[0]["appsflyer_app_id"] == "1458505230"
    assert rows[0]["app_id"] == "com.yesimmobile"  # unaffected by the raw column's value


def test_transform_installs_raises_on_unexpected_column_set() -> None:
    """A response missing a column (or carrying an extra, unrecognized one)
    must fail loudly, not silently drift from the installs table's DDL --
    the whole point of validating produced-columns against insert_columns
    even in pass-through mode.
    """
    # Drop one raw column entirely -- a genuinely column-short response.
    csv_text = _installs_csv_row()
    header_line, row_line = csv_text.splitlines()
    headers = header_line.split(",")
    values = row_line.split(",")
    drop_index = headers.index("Campaign")
    del headers[drop_index]
    del values[drop_index]
    bad_csv = ",".join(headers) + "\n" + ",".join(values) + "\n"
    df = pl.read_csv(io.StringIO(bad_csv), infer_schema_length=0)

    with pytest.raises(TransformError, match="does not match the expected installs schema"):
        transform_events(
            df,
            spec=REPORTS["installs_non_organic"],
            app_id="com.yesimmobile",
            media_source_filter=None,
            event_names_filter=None,
        )
```

Add `import io` to this file's imports if not already present.

- [ ] **Step 2: Run it, confirm it fails**

Run: `uv run pytest tests/test_transform.py -k installs -v 2>&1 | tail -30`
Expected: FAIL — `transform_events` still does `_COLUMN_MAP`-shaped (Stage-3: `spec.column_map`)
unconditional dict iteration; with `column_map=None`, `(*column_map, ...)` raises `TypeError:
argument of type 'NoneType' is not iterable`.

- [ ] **Step 3: Write the failing dedupe-key tests**

Add to `tests/test_transform.py`:

```python
def test_transform_installs_keeps_two_rows_for_different_appsflyer_ids() -> None:
    df = _installs_df(**{"AppsFlyer ID": "af-1"})
    df2 = _installs_df(**{"AppsFlyer ID": "af-2"})
    combined = pl.concat([df, df2])
    rows = transform_events(
        combined,
        spec=REPORTS["installs_non_organic"],
        app_id="com.yesimmobile",
        media_source_filter=None,
        event_names_filter=None,
    )
    assert len(rows) == 2


def test_transform_installs_collapses_exact_repeat_of_same_appsflyer_id_and_event_time() -> None:
    """Same key, identical row -- the existing exact-duplicate-collapse path,
    unaffected by installs having its own key function.
    """
    df = _installs_df()
    combined = pl.concat([df, df])
    rows = transform_events(
        combined,
        spec=REPORTS["installs_non_organic"],
        app_id="com.yesimmobile",
        media_source_filter=None,
        event_names_filter=None,
    )
    assert len(rows) == 1


def test_transform_installs_key_does_not_include_event_name_or_event_value() -> None:
    """Architecture decision 7: two rows sharing (appsflyer_id, event_time)
    but differing ONLY in Event Name/Event Value must still collapse to one
    (both fields are always-empty-or-near-constant for installs and
    deliberately excluded from the key) -- this pins that the key really is
    (appsflyer_id, event_time), not a wider tuple that happens to look right
    on the "different AppsFlyer ID" test above.
    """
    df = _installs_df(**{"Event Name": "install"})
    df2 = _installs_df(**{"Event Name": "re-engagement"})  # same appsflyer_id, event_time
    combined = pl.concat([df, df2])
    rows = transform_events(
        combined,
        spec=REPORTS["installs_non_organic"],
        app_id="com.yesimmobile",
        media_source_filter=None,
        event_names_filter=None,
    )
    assert len(rows) == 1
```

- [ ] **Step 4: Run them, confirm they fail**

Run: `uv run pytest tests/test_transform.py -k "installs and (appsflyer_ids or collapses or does_not_include)" -v`
Expected: FAIL (still blocked by the same `TypeError` from Step 2 — `column_map=None` isn't handled
yet).

- [ ] **Step 5: Generalize `_dedupe_rows` to take `key_fn`**

In `src/appsflyer_pipeline/transform.py`, add `from collections.abc import Callable` to imports.
Change `_dedupe_rows`'s signature (post-Stage-2 shape) from:

```python
def _dedupe_rows(
    rows: list[dict[str, Any]], *, attribution_type: AttributionType, app_id: str
) -> list[dict[str, Any]]:
```

to:

```python
def _dedupe_rows(
    rows: list[dict[str, Any]],
    *,
    key_fn: Callable[[dict[str, Any]], tuple[Any, ...]],
    attribution_type: AttributionType,
    app_id: str,
) -> list[dict[str, Any]]:
```

Update the docstring's opening line from `"""Keep exactly ONE row per (event_time, event_name,
appsflyer_id, Event Value) key: the one with the latest \`install_time\`.` to:

```python
    """Keep exactly ONE row per `key_fn(row)`'s key: the one with the latest
    `install_time`. `key_fn` is per-report (BAF-11 stage 4, ReportSpec.dedupe_key)
    -- in-app-events' key_fn reproduces this function's original hardcoded
    (event_time, event_name, appsflyer_id, Event Value) key exactly; installs'
    key_fn is (appsflyer_id, event_time) instead -- see
    docs/superpowers/plans/2026-08-31-baf-11-stage-4-installs-report.md's
    Architecture decision 7 for the rationale and the open risk it flags.
```

Change the key-construction (widen the `slot_of_key` type and replace the inline 4-tuple):

```python
    slot_of_key: dict[tuple[Any, ...], int] = {}
```

```python
    for row in rows:
        key = key_fn(row)
```

- [ ] **Step 6: Add the `column_map is None` branch to `transform_events`**

Change `transform_events`'s body (post-Stage-3 shape). Its first block currently reads:

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
```

Replace it with:

```python
    attribution_type = spec.attribution_type

    if spec.column_map is not None:
        column_map = dict(spec.column_map)
        missing = [
            raw for raw in (*column_map, _DEDUPE_DISCRIMINATOR_RAW_COLUMN) if raw not in df.columns
        ]
        if missing:
            raise TransformError(
                f"AppsFlyer response is missing expected column(s): {missing} "
                f"(attribution_type={attribution_type}, app_id={app_id})"
            )
    else:
        # BAF-11 stage 4 (installs, ReportSpec.column_map=None): full
        # pass-through -- normalize whatever raw columns AppsFlyer actually
        # returned instead of consulting a fixed dict, so a 128-field report
        # doesn't need one hand-maintained mapping entry per column. Still
        # validated, not blindly trusted: the normalized column set (plus the
        # two injected columns) must equal spec.insert_columns exactly, so an
        # AppsFlyer schema change (a column renamed, added, or dropped) fails
        # loudly here instead of silently drifting from the installs table's
        # DDL.
        if _DEDUPE_DISCRIMINATOR_RAW_COLUMN not in df.columns:
            raise TransformError(
                f"AppsFlyer response is missing expected column(s): "
                f"['{_DEDUPE_DISCRIMINATOR_RAW_COLUMN}'] "
                f"(attribution_type={attribution_type}, app_id={app_id})"
            )
        # raw AppsFlyer header -> normalized target name (NOT the reverse --
        # a swapped key/value here makes every downstream row-building line
        # look up a raw header string where it expects a snake_case name,
        # caught immediately by Step 7's test run).
        column_map = {raw: normalize_column_name(raw) for raw in df.columns}
        produced = set(column_map.values()) | {"attribution_type", "app_id"}
        expected = set(spec.insert_columns)
        if produced != expected:
            raise TransformError(
                "AppsFlyer response's column set does not match the expected "
                f"installs schema -- missing: {sorted(expected - produced)}, "
                f"unexpected: {sorted(produced - expected)} "
                f"(attribution_type={attribution_type}, app_id={app_id})"
            )
```

Change the `select_columns` line further down from:

```python
    select_columns = [*_COLUMN_MAP, _DEDUPE_DISCRIMINATOR_RAW_COLUMN]
```

to:

```python
    select_columns = list(dict.fromkeys([*column_map, _DEDUPE_DISCRIMINATOR_RAW_COLUMN]))
```

(`dict.fromkeys` dedupes while preserving order — needed because in pass-through mode, `"Event
Value"` is already one of `column_map`'s keys, so appending it again would otherwise ask polars to
`.select()` the same column twice.)

Change every remaining `_COLUMN_MAP` reference in the function body (the `row: dict[str, Any] =
{target: raw_row[raw] for raw, target in _COLUMN_MAP.items()}` line) to `column_map.items()`.

Change the dedupe call at the end from:

```python
    deduped = _dedupe_rows(rows, attribution_type=attribution_type, app_id=app_id)
    for row in deduped:
        del row[_DEDUPE_DISCRIMINATOR_ROW_KEY]
    return deduped
```

to:

```python
    deduped = _dedupe_rows(
        rows, key_fn=spec.dedupe_key, attribution_type=attribution_type, app_id=app_id
    )
    for row in deduped:
        del row[DEDUPE_DISCRIMINATOR_ROW_KEY]
    return deduped
```

(Every other `_DEDUPE_DISCRIMINATOR_ROW_KEY` reference in the function — the
`row[_DEDUPE_DISCRIMINATOR_ROW_KEY] = raw_row[_DEDUPE_DISCRIMINATOR_RAW_COLUMN]` assignment — was
already renamed in Task 2 Step 3; if it wasn't (e.g. this task lands standalone against a
not-yet-Task-2 tree), rename it here too.)

- [ ] **Step 7: Run the transform tests, confirm they now pass**

Run: `uv run pytest tests/test_transform.py -v`
Expected: PASS — every test, including all the new installs ones from Steps 1/3.

- [ ] **Step 8: Run the FULL suite to confirm in-app-events' dedupe behavior is untouched**

Run: `uv run pytest tests/test_transform.py -k "not installs" -v`
Expected: PASS — every pre-existing in-app-events test (including
`test_transform_keeps_only_the_latest_install_time_on_conflict` and
`test_transform_keeps_both_rows_when_event_value_differs` from Stage 2) with **no assertion
changes** — this is the regression proof that `_in_app_events_dedupe_key` reproduces the old
hardcoded key exactly.

- [ ] **Step 9: Run the Stage-3 registry cross-check test, now unblocked**

Run: `uv run pytest tests/test_reports.py -v`
Expected: PASS — including `test_insert_columns_match_transform_events_output_keys_for_every_report`
(Stage 3), which now exercises all **four** registered specs, not two. That test's own fixture
builder (`_raw_row_for`, Stage 3) only ever built rows shaped for a `Mapping`-based `column_map`; if
it errors on the two installs specs (`column_map=None` has no `.items()`), extend it: when
`spec.column_map is None`, build the raw row from `spec.insert_columns` minus `{"attribution_type",
"app_id"}` reverse-mapped through a small local raw-name lookup, OR — simpler and consistent with
this task's own fixtures — special-case installs specs in that test to use
`tests/test_transform.py`'s `_installs_csv_row`-style construction instead. Prefer the second: add

```python
from appsflyer_pipeline.reports import INSTALLS_RAW_COLUMNS


def _installs_raw_row() -> dict[str, str]:
    row: dict[str, str] = dict.fromkeys(INSTALLS_RAW_COLUMNS, "")
    row.update(
        {
            "AppsFlyer ID": "af-id-1",
            "Install Time": "2026-05-19 09:30:00",
            "Event Time": "2026-05-20 10:05:00",
            "Attributed Touch Time": "2026-05-19 09:00:00",
        }
    )
    return row
```

and change `test_insert_columns_match_transform_events_output_keys_for_every_report`'s loop body to
branch: `raw_row = _installs_raw_row() if spec.column_map is None else _raw_row_for(dict(spec.column_map))`.

- [ ] **Step 10: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest -k "not test_run_backfill_default_window_is_90_days"`
Expected: all green (this task doesn't touch `pipeline.py`, so
`test_run_backfill_default_window_is_90_days` — pending since Task 2 registered installs' 60-day-
retention specs — is still the one known exception; Task 5 fixes it).

```bash
git add src/appsflyer_pipeline/transform.py tests/test_transform.py tests/test_reports.py
git commit -m "BAF-11 stage 4: transform_events full pass-through mode, generalized dedupe key"
```

---

### Task 5: `pipeline.py` — hard-clamped installs retention + fix the `REPORTS`-growth ripple

**Files:**
- Modify: `src/appsflyer_pipeline/pipeline.py`
- Modify: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `spec.hard_clamp_retention`, `spec.retention_days` (both new/existing `ReportSpec`
  fields).
- Produces: `_iter_work_items`'s per-spec chunk generation now clamps `start` up to
  `_today() - spec.retention_days` **only when `spec.hard_clamp_retention` is `True`**. No change to
  `_iter_work_items`'s return type (`Iterator[tuple[ReportSpec, str, datetime.date, datetime.date]]`,
  from Stage 3) or to `_active_retention_days()`'s own definition (Stage 3's `min(...)`-based
  `_active_retention_days()` already narrows correctly once installs' `60` enters the `REPORTS` pool —
  no change needed to the helper itself, confirmed by a test in this task). **`run_backfill` and
  `run_daily` DO change**, though: their `default_start`/`_warn_if_before_retention_floor` threshold
  arithmetic switches from consuming `_active_retention_days()`'s cross-`REPORTS` minimum to
  `MAX_RETENTION_DAYS` — see Architecture decision 3's addendum above for why leaving them wired to
  the minimum would silently narrow in-app-events' no-args default window from 90 days to 60 the
  moment installs joins `REPORTS`, and why installs doesn't need the run-level default to track its
  narrower retention (it clamps itself, per-spec, inside `_iter_work_items`).

- [ ] **Step 1: Write the failing hard-clamp tests**

Add to `tests/test_pipeline.py`:

```python
def test_iter_work_items_hard_clamps_installs_but_not_in_app_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Architecture decision 3: installs (hard_clamp_retention=True, 60 days)
    gets its start date clamped up to the retention floor; in-app-events
    (hard_clamp_retention=False, 90 days) keeps warn-and-proceed -- its
    chunks still start at the caller's requested start regardless.
    """
    _set_env(monkeypatch)
    settings = get_settings()
    fixed_today = datetime.date(2026, 8, 31)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    start = fixed_today - datetime.timedelta(days=100)  # 100 days back
    end = fixed_today - datetime.timedelta(days=1)

    items = list(_iter_work_items(settings, start, end))

    in_app_events_starts = {
        s for spec, a, s, e in items if a == "app1" and spec.name == "in_app_events"
    }
    installs_starts = {s for spec, a, s, e in items if a == "app1" and spec.name == "installs"}

    assert min(in_app_events_starts) == start  # unclamped -- 100 days back, unchanged
    assert min(installs_starts) == fixed_today - datetime.timedelta(days=60)  # hard-clamped


def test_iter_work_items_skips_installs_entirely_when_window_is_fully_before_the_floor(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _set_env(monkeypatch)
    settings = get_settings()
    fixed_today = datetime.date(2026, 8, 31)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    start = fixed_today - datetime.timedelta(days=200)
    end = fixed_today - datetime.timedelta(days=150)  # entirely before the 60-day floor

    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.pipeline"):
        items = list(_iter_work_items(settings, start, end))

    installs_items = [i for i in items if i[0].name == "installs"]
    in_app_events_items = [i for i in items if i[0].name == "in_app_events"]
    assert installs_items == []  # nothing to fetch -- entirely before the hard floor
    assert len(in_app_events_items) > 0  # in-app-events is unaffected (90-day retention)
    assert any("entirely before" in r.message for r in caplog.records)


def test_active_retention_days_narrows_to_installs_60_once_registered() -> None:
    """No code change needed for this -- Stage 3's min(...)-based
    _active_retention_days() already does the right thing once REPORTS grows
    to include a 60-day spec. This test just locks in that it actually does.
    """
    from appsflyer_pipeline.pipeline import _active_retention_days

    assert _active_retention_days() == 60
```

Add `import logging` to `tests/test_pipeline.py`'s imports if not already present.

- [ ] **Step 2: Run them, confirm the first two fail**

Run: `uv run pytest tests/test_pipeline.py -k "hard_clamp or fully_before or narrows_to_installs" -v`
Expected: `test_active_retention_days_narrows_to_installs_60_once_registered` already PASSES (no code
change needed, per Stage 3's design). The other two FAIL — `_iter_work_items` doesn't clamp anything
yet, so `installs_starts` still equals `{start}` (100 days back) instead of the clamped floor, and
nothing is skipped for the fully-before-the-floor case.

- [ ] **Step 3: Implement the hard clamp in `_iter_work_items`**

Change `_iter_work_items` (post-Stage-3 shape) from:

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

to:

```python
def _iter_work_items(
    settings: Settings, start: datetime.date, end: datetime.date
) -> Iterator[tuple[ReportSpec, str, datetime.date, datetime.date]]:
    """(report x app_id x <=chunk_days chunk) for the [start, end] window.

    Mostly pure -- the one exception (BAF-11 stage 4) is `spec.hard_clamp_retention`
    specs, which read `_today()` to clamp their effective start date up to the
    retention floor; monkeypatch `_today` for a deterministic test, same
    pattern already used elsewhere in this module. Loop order (app_id outer,
    report inner) is unchanged from the pre-ReportSpec app_id/attribution_type
    nesting -- see the Stage 3 plan's Architecture decision 4 for why it
    wasn't reordered to match the master spec's "report x app_id" prose.
    """
    for app_id in settings.appsflyer_app_ids:
        for spec in REPORTS.values():
            spec_start = start
            if spec.hard_clamp_retention:
                # BAF-11 stage 4 (installs): a response past the real
                # retention boundary can come back as a valid, header-only
                # EMPTY report (issue #45's shape) -- the idempotent
                # delete-then-insert would then wipe a window that may have
                # had real data. In-app-events (hard_clamp_retention=False)
                # deliberately keeps warn-and-proceed instead -- see this
                # stage's plan, Architecture decision 3.
                retention_floor = _today() - datetime.timedelta(days=spec.retention_days)
                spec_start = max(start, retention_floor)
                if spec_start > end:
                    logger.warning(
                        "skipping %s for app_id=%s: requested window [%s, %s] is entirely "
                        "before the %d-day retention floor (%s) -- nothing to fetch",
                        spec.name,
                        app_id,
                        start,
                        end,
                        spec.retention_days,
                        retention_floor,
                    )
                    continue
                if spec_start > start:
                    logger.warning(
                        "clamping %s for app_id=%s: requested start %s is before the "
                        "%d-day retention floor -- fetching from %s instead (a response "
                        "for dates before the floor can come back silently empty, and the "
                        "idempotent delete-then-insert would wipe any already-loaded data "
                        "there)",
                        spec.name,
                        app_id,
                        start,
                        spec.retention_days,
                        spec_start,
                    )
            for chunk_start, chunk_end in chunk_date_range(
                spec_start, end, max_days=settings.appsflyer_chunk_days
            ):
                yield spec, app_id, chunk_start, chunk_end
```

- [ ] **Step 4: Run the new tests, confirm they pass**

Run: `uv run pytest tests/test_pipeline.py -k "hard_clamp or fully_before or narrows_to_installs" -v`
Expected: PASS.

- [ ] **Step 5: Run the full pipeline test file, fix the `REPORTS`-growth ripple (Architecture decision 5)**

Run: `uv run pytest tests/test_pipeline.py -v 2>&1 | tail -60`
Expected: FAIL in several pre-existing tests, for two distinct reasons — not because behavior
regressed, but because these tests' own filters/mocks assumed exactly 2 specs (a third, unrelated
failure — `test_run_backfill_default_window_is_90_days`, pending since Task 2 — is a genuine behavior
change, not a filter/mock assumption; it's handled separately in Steps 7-9 below, not here):

1. `test_iter_work_items_yields_expected_matrix` and
   `test_iter_work_items_respects_configured_chunk_days`: their `one_series` filter
   (`if a == "app1" and spec.attribution_type == "non_organic"` / `t == "non_organic"`) now matches
   BOTH `in_app_events_non_organic` and `installs_non_organic` (installs' `hard_clamp_retention`
   doesn't affect these two tests' windows if they use recent-enough dates, but the *count* doubles
   regardless). Fix: narrow the filter to also pin `spec.name == "in_app_events"`:

   ```python
   one_series = [
       (s, e)
       for spec, a, s, e in items
       if a == "app1" and spec.name == "in_app_events" and spec.attribution_type == "non_organic"
   ]
   ```

   And fix `len(items) == len(APP_IDS) * len(ATTRIBUTION_TYPES) * len(one_series)` (Stage 3 shape)
   to `len(items) == len(APP_IDS) * len(REPORTS) * len(one_series)` — **only** valid if every spec
   in `REPORTS` produces the same number of chunks for the test's chosen date range; if
   `installs`' hard clamp narrows its window relative to in-app-events' for that specific date range,
   this equality breaks structurally, not just numerically. Pick a date range for these two tests
   that's recent enough (e.g. within the last 60 days, using the `_set_env`/`monkeypatch.setattr(pipeline,
   "_today", ...)` pattern from Step 1's tests) that `installs`' clamp is a no-op, so the equality
   holds; otherwise replace the equality with two separate assertions (one per `spec.name` group).

2. `_mock_all_ok()` and `_url()`: both are keyed only by `(app_id, attribution_type)`, assuming
   exactly 2 possible endpoints. Any end-to-end `run_backfill`/`run_daily` test that relies on
   `_mock_all_ok()` now needs the 2 installs URLs mocked too, or respx raises "no matching route"
   for those requests. Rewrite both to iterate `REPORTS` directly:

   ```python
   def _url_for_spec(spec: ReportSpec, app_id: str) -> str:
       return f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/{spec.endpoint}/v5"


   def _installs_sample_csv() -> str:
       from appsflyer_pipeline.reports import INSTALLS_RAW_COLUMNS

       values = dict.fromkeys(INSTALLS_RAW_COLUMNS, "")
       values.update(
           {
               "AppsFlyer ID": "af-installs-1",
               "Install Time": "2026-05-19 09:30:00",
               "Event Time": "2026-05-19 09:30:00",
               "Attributed Touch Time": "2026-05-19 09:00:00",
               "Media Source": "Facebook Ads",
           }
       )
       header = ",".join(INSTALLS_RAW_COLUMNS)
       row = ",".join(values[c] for c in INSTALLS_RAW_COLUMNS)
       return f"{header}\n{row}\n"


   def _mock_all_ok() -> None:
       from appsflyer_pipeline.reports import REPORTS as _REPORTS

       installs_csv = _installs_sample_csv()
       for app_id in APP_IDS:
           for spec in _REPORTS.values():
               csv_text = SAMPLE_CSV if spec.name == "in_app_events" else installs_csv
               respx.get(_url_for_spec(spec, app_id)).mock(
                   return_value=httpx.Response(200, text=csv_text)
               )
   ```

   Leave `_url(app_id, attribution_type)` in place for whatever existing tests key their respx mocks
   directly by attribution type for in-app-events only (still correct for that narrower use) —
   `_url_for_spec` is additive, not a replacement, so no existing in-app-events-only test's call site
   needs to change.

   Any test using `_mock_all_ok()` inside a date range affected by installs' hard clamp (e.g. a
   `run_backfill` test using very old dates) needs its assertions checked against the now-clamped
   installs windows specifically — read each failure's actual output before patching the assertion,
   per this plan's regression bar.

- [ ] **Step 6: Run the full pipeline test file again, confirm green**

Run: `uv run pytest tests/test_pipeline.py -v`
Expected: **`test_run_backfill_default_window_is_90_days` (currently `tests/test_pipeline.py:356-372`)
now FAILS** — not from Steps 3/5's changes directly, but because `REPORTS` already grew to include
installs in Task 2, and Stage 3's unmodified `run_backfill` already computes its no-args
`default_start` off `_active_retention_days()`'s cross-`REPORTS` minimum, which is `60` now that
installs (`retention_days=60`) is registered. Concretely: `expected_start = expected_end -
timedelta(days=MAX_RETENTION_DAYS - 1)` (90 days back) but `run_backfill`'s real `default_start` is
now 60 days back, so `min(starts)` — computed across **all** `summary.results`, i.e. all four
specs — no longer equals `expected_start`; installs' own start (clamped by Step 3's `_iter_work_items`
change to 60 days back, a *later* date than the 90-day-back one in-app-events should be defaulting to)
is not what's wrong here, in-app-events' own default is. See Architecture decision 3's addendum and
this task's Interfaces block for the mechanism and the fix, implemented next. Every other test in the
file should already be PASS at this point.

- [ ] **Step 7: Write the failing regression test pinning `run_backfill`'s default-window decoupling**

Note: `WindowResult` (`src/appsflyer_pipeline/pipeline.py`, currently lines 53-60) carries
`app_id`/`attribution_type`/dates/counts only — no report-family identity (Stage 3's own Architecture
decision 2 keeps `attribution_type: AttributionType`, not widened, and adds no `spec.name`-equivalent
field to `WindowResult`). `attribution_type` alone can't disambiguate in-app-events from installs
(both share `"non_organic"`/`"retargeting"`), so `summary.results` cannot be filtered by report
family — the pre-existing `test_run_backfill_default_window_is_90_days` works around this by taking
`min(starts)` over *all* results, relying on in-app-events' unclamped start being the overall
earliest date. Rather than widening `WindowResult` (out of scope for this fix), pin the decoupling
directly at its source: capture what `start`/`end` `run_backfill` actually passes into
`_iter_work_items`, independent of `WindowResult`'s shape entirely.

Add to `tests/test_pipeline.py`:

```python
def test_run_backfill_default_window_uses_max_retention_days_not_the_cross_report_minimum(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """Pins run_backfill's no-args default_start computation directly, by
    capturing the start/end it actually passes to _iter_work_items --
    independent of WindowResult, which carries no per-report-family identity
    (see the note above). Proves Architecture decision 3's addendum: the
    default resolves off MAX_RETENTION_DAYS (90), not
    _active_retention_days()'s cross-REPORTS minimum, which drops to 60 the
    moment installs (retention_days=60) is registered alongside in-app-events.
    """
    _set_env(monkeypatch)
    fixed_today = datetime.date(2026, 7, 7)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    expected_end = fixed_today - datetime.timedelta(days=1)
    expected_start = expected_end - datetime.timedelta(days=MAX_RETENTION_DAYS - 1)

    captured: dict[str, datetime.date] = {}
    real_iter_work_items = pipeline._iter_work_items

    def _capturing_iter_work_items(
        settings: Settings, start: datetime.date, end: datetime.date
    ) -> Any:
        captured["start"] = start
        captured["end"] = end
        return real_iter_work_items(settings, start, end)

    monkeypatch.setattr(pipeline, "_iter_work_items", _capturing_iter_work_items)

    with respx.mock:
        _mock_all_ok()
        run_backfill(dry_run=True)

    assert captured["start"] == expected_start
    assert captured["end"] == expected_end
```

Add `from appsflyer_pipeline.config import Settings` to this file's imports if not already present
(most of this file's other tests reach `Settings` only via `get_settings()`).

Run: `uv run pytest tests/test_pipeline.py -k uses_max_retention_days_not_the_cross_report_minimum -v`
Expected: FAIL — `captured["start"]` is 60 days back (today's Stage-3, unmodified `run_backfill` still
computes its default off `_active_retention_days()`, which is `60` now that installs is registered),
not the expected 90-days-back date.

- [ ] **Step 8: Decouple `run_backfill`/`run_daily`'s default-window and warn-threshold math from
  `_active_retention_days()`**

In `src/appsflyer_pipeline/pipeline.py`, change `run_backfill` (Stage-3 shape) from:

```python
    retention_days = _active_retention_days()
    end = end or (_today() - datetime.timedelta(days=1))
    default_start = end - datetime.timedelta(days=retention_days - 1)
    start = start or default_start

    if start > end:
        raise PipelineError(f"start {start} is after end {end}")
    _warn_if_before_retention_floor(start, "backfill start", retention_days=retention_days)
```

to:

```python
    # BAF-11 stage 4: do NOT use _active_retention_days() (the cross-REPORTS
    # minimum) here. Once installs (retention_days=60, hard_clamp_retention=
    # True) is registered, that minimum drops from 90 to 60 -- but installs
    # already clamps its OWN effective start inside _iter_work_items
    # regardless of what start this function resolves to (Architecture
    # decision 3). Keying the run-level default/warn threshold off the global
    # minimum would silently narrow in-app-events' no-args default window
    # from 90 days to 60, contradicting this stage's regression bar
    # (in-app-events' warn-only retention behavior stays byte-for-byte
    # unchanged) -- caught by test_run_backfill_default_window_is_90_days.
    # MAX_RETENTION_DAYS is what this caller-facing default/warn threshold is
    # actually about: the widest retention among hard_clamp_retention=False
    # specs (today, in-app-events only).
    end = end or (_today() - datetime.timedelta(days=1))
    default_start = end - datetime.timedelta(days=MAX_RETENTION_DAYS - 1)
    start = start or default_start

    if start > end:
        raise PipelineError(f"start {start} is after end {end}")
    _warn_if_before_retention_floor(start, "backfill start", retention_days=MAX_RETENTION_DAYS)
```

Change `run_daily` (Stage-3 shape) from:

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

to:

```python
    # BAF-11 stage 4: same reasoning as run_backfill above -- MAX_RETENTION_DAYS,
    # not _active_retention_days()'s cross-REPORTS minimum. installs clamps
    # itself inside _iter_work_items; these two warn call sites are about
    # in-app-events' own (hard_clamp_retention=False) floor.
    if date is not None:
        _warn_if_before_retention_floor(date, "daily --date", retention_days=MAX_RETENTION_DAYS)
        return _run_window(date, date, dry_run=dry_run)

    settings = get_settings()
    if settings.appsflyer_event_time_from is not None:
        start = settings.appsflyer_event_time_from
        end = settings.appsflyer_event_time_to or (_today() - datetime.timedelta(days=1))
        if start > end:
            raise PipelineError(f"APPSFLYER_EVENT_TIME_FROM {start} is after the window end {end}")
        _warn_if_before_retention_floor(
            start, "APPSFLYER_EVENT_TIME_FROM", retention_days=MAX_RETENTION_DAYS
        )
        return _run_window(start, end, dry_run=dry_run)
```

`_active_retention_days()` itself is untouched (still `min(...)` over `REPORTS`, still returns `60`
once installs is registered — pinned by Step 1's
`test_active_retention_days_narrows_to_installs_60_once_registered`); this change only stops
`run_backfill`/`run_daily` from consuming it. It is intentionally left with no other production call
site after this change — a small, still-correct diagnostic helper, not dead code to delete, since
removing a Stage-3 ground-truth function is out of this plan's scope and its own test still pins real
behavior worth keeping.

- [ ] **Step 9: Run the pipeline tests again, confirm green — including
  `test_run_backfill_default_window_is_90_days` unmodified**

Run: `uv run pytest tests/test_pipeline.py -v`
Expected: PASS — every test, **including the pre-existing
`test_run_backfill_default_window_is_90_days` with zero changes to its own source** (Step 8's fix
makes in-app-events' real default resolve back to 90-days-back, which is what that test's `min(starts)`
across all four specs' results was always implicitly relying on before installs existed to contribute
a second, more-recent minimum candidate). This is the regression proof for Architecture decision 3's
addendum: the existing test passing unmodified, not a patched expected value.

- [ ] **Step 10: Regression test — window clamps to 60 days, not 90, for installs**

This is the master spec's Этап 6 test-list item ("окно клэмпится по 60 дням, а не по 90"), stated as
its own explicit test even though Step 1 already covers the mechanism generally:

```python
def test_installs_retention_floor_is_60_not_90(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch)
    settings = get_settings()
    fixed_today = datetime.date(2026, 8, 31)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    start = fixed_today - datetime.timedelta(days=80)  # between 60 and 90 days back
    end = fixed_today - datetime.timedelta(days=1)

    items = list(_iter_work_items(settings, start, end))

    installs_starts = {s for spec, a, s, e in items if a == "app1" and spec.name == "installs"}
    in_app_events_starts = {
        s for spec, a, s, e in items if a == "app1" and spec.name == "in_app_events"
    }
    assert min(installs_starts) == fixed_today - datetime.timedelta(days=60)
    assert min(in_app_events_starts) == start  # 80 days back, well within in-app-events' 90
```

Run: `uv run pytest tests/test_pipeline.py -k retention_floor_is_60 -v`
Expected: PASS (this should already hold given Step 3's implementation — this step is confirming
the master spec's exact scenario, not introducing new behavior).

- [ ] **Step 11: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
Expected: all green.

```bash
git add src/appsflyer_pipeline/pipeline.py tests/test_pipeline.py
git commit -m "BAF-11 stage 4: hard-clamp installs retention, decouple run_backfill/run_daily's default window from it, fix REPORTS-growth test ripple"
```

---

### Task 6: `appsflyer_client.py` regression tests (installs request shape)

**Files:**
- Modify: `tests/test_appsflyer_client.py` (test-only — no production code change; Stage 3 already
  made `fetch_events`/`_fetch_csv` fully generic via `spec.endpoint`/`spec.sends_event_name`/
  `spec.sends_media_source`/`spec.additional_fields`, confirmed by re-reading `appsflyer_client.py`'s
  Stage-3 diff above).

**Interfaces:** None new — this task only proves the existing generic code handles installs specs
correctly.

- [ ] **Step 1: Read the current `test_fetch_events_never_sends_additional_fields` before touching it**

Run: `grep -n -A 30 "def test_fetch_events_never_sends_additional_fields" tests/test_appsflyer_client.py`

Confirm its exact assertion body. Its name and Stage-3's own code comment ("both registered specs
have `additional_fields=()`, so this is dead for now") both imply it currently asserts NO spec in
`REPORTS` sends `additional_fields` — which becomes **false** once installs (47 `additional_fields`)
joins the registry. This step is a read-before-edit gate, per this repo's grounding rule — do not
guess the assertion text; act on what the `grep` output actually shows.

- [ ] **Step 2: Narrow the existing test to specs with no additional_fields, add the installs complement**

Change the test's loop (`for spec in REPORTS.values(): ...`) to iterate only
`(spec for spec in REPORTS.values() if not spec.additional_fields)` — preserving its exact original
assertion and intent ("in-app-events never sends `additional_fields`") without silently becoming
vacuously true or false depending on dict ordering. Rename it, if its body's assertion is
specifically about the *absence* of the param, to
`test_fetch_events_never_sends_additional_fields_for_in_app_events` for clarity that it's now
scoped, not universal.

Add the installs complement:

```python
@respx.mock
def test_fetch_events_sends_all_47_additional_fields_for_installs() -> None:
    """Master spec Этап 6 test list: additional_fields уходит списком из 47 имён."""
    from appsflyer_pipeline.reports import REPORTS

    spec = REPORTS["installs_non_organic"]
    url = f"https://hq1.appsflyer.com/api/raw-data/export/app/id123/{spec.endpoint}/v5"
    route = respx.get(url).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))
    with httpx.Client() as client:
        fetch_events(
            client,
            app_id="id123",
            spec=spec,
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source=None,
            event_names=None,
        )
    sent = route.calls.last.request.url.params["additional_fields"].split(",")
    assert len(sent) == 47
    assert sent == list(spec.additional_fields)


@respx.mock
def test_fetch_events_never_sends_event_name_for_installs() -> None:
    """sends_event_name=False (ReportSpec) must suppress event_name even when
    the caller passes event_names -- installs has no purchase-event-name
    concept to filter on.
    """
    from appsflyer_pipeline.reports import REPORTS

    spec = REPORTS["installs_non_organic"]
    url = f"https://hq1.appsflyer.com/api/raw-data/export/app/id123/{spec.endpoint}/v5"
    route = respx.get(url).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))
    with httpx.Client() as client:
        fetch_events(
            client,
            app_id="id123",
            spec=spec,
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source=None,
            event_names=["af_purchase"],  # explicitly passed, must still be suppressed
        )
    assert "event_name" not in route.calls.last.request.url.params


@respx.mock
def test_fetch_events_sends_timezone_for_installs_regression() -> None:
    """Regression guard (BAF-11 decision #6): a UTC/Riga split between the
    in-app-events and installs tables would be invisible unless this is
    tested per-report, not just once for in-app-events. Must NOT regress to
    UTC-only for installs.
    """
    from appsflyer_pipeline.reports import REPORTS

    spec = REPORTS["installs_non_organic"]
    url = f"https://hq1.appsflyer.com/api/raw-data/export/app/id123/{spec.endpoint}/v5"
    route = respx.get(url).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))
    with httpx.Client() as client:
        fetch_events(
            client,
            app_id="id123",
            spec=spec,
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source=None,
            event_names=None,
            timezone="Europe/Riga",
        )
    assert route.calls.last.request.url.params["timezone"] == "Europe/Riga"


@respx.mock
def test_fetch_events_hits_installs_retarget_endpoint() -> None:
    """Master spec Этап 6 test list: "оба URL корректны" (both URLs are
    correct) -- the three tests above only ever exercise
    REPORTS["installs_non_organic"] (endpoint installs_report) directly.
    installs_retargeting's endpoint (installs-retarget) was otherwise only
    ever hit indirectly, through Task 5's pipeline-level respx routing
    (_mock_all_ok()/_url_for_spec) -- this pins the URL/params shape at the
    client layer itself, closing out the master spec's item the same way the
    existing in-app-events non_organic-vs-retargeting tests already do for
    that report family.
    """
    from appsflyer_pipeline.reports import REPORTS

    spec = REPORTS["installs_retargeting"]
    url = f"https://hq1.appsflyer.com/api/raw-data/export/app/id123/{spec.endpoint}/v5"
    route = respx.get(url).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))
    with httpx.Client() as client:
        fetch_events(
            client,
            app_id="id123",
            spec=spec,
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source=None,
            event_names=None,
        )
    assert route.called
    sent = route.calls.last.request.url.params["additional_fields"].split(",")
    assert sent == list(spec.additional_fields)
    assert "event_name" not in route.calls.last.request.url.params
```

- [ ] **Step 3: Run the new/renamed tests**

Run: `uv run pytest tests/test_appsflyer_client.py -k "additional_fields or event_name_for_installs or timezone_for_installs or installs_retarget_endpoint" -v`
Expected: PASS — Stage 3's generic threading (`spec.additional_fields`/`spec.sends_event_name`
already wired into `_fetch_csv`'s param construction) makes these pass with **zero production-code
changes** in this task; if any fails, that's evidence Stage 3's threading has a gap this stage must
now fix (stop and diagnose before patching the test).

- [ ] **Step 4: Run the full client test file**

Run: `uv run pytest tests/test_appsflyer_client.py -v`
Expected: PASS — every test.

- [ ] **Step 5: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
Expected: all green.

```bash
git add tests/test_appsflyer_client.py
git commit -m "BAF-11 stage 4: regression tests for installs' request shape (additional_fields, event_name, timezone)"
```

---

### Task 7: `docs/design-spec.md` — Config + Non-Goals

**Files:**
- Modify: `docs/design-spec.md`

**Interfaces:** Documentation-only, no tests (nothing here is executable).

- [ ] **Step 1: Add `DB_TABLE_INSTALLS` to the Config bullet**

In `docs/design-spec.md`'s Interfaces section, the bullet currently reading (lines 101-103):

```
- **Config (env / `.env`):** see `.env.example` — `DB_HOST/PORT/USER/PASSWORD/NAME/TABLE`,
  `APPSFLYER_API_TOKEN`, `APPSFLYER_APP_IDS`, `APPSFLYER_MEDIA_SOURCE`, `APPSFLYER_EVENT_NAMES`,
  `APPSFLYER_DAILY_LOOKBACK_DAYS` (default 1), `APPSFLYER_CHUNK_DAYS` (default 31), `APPSFLYER_TIMEZONE` (issue #53; unset = UTC,
```

becomes:

```
- **Config (env / `.env`):** see `.env.example` — `DB_HOST/PORT/USER/PASSWORD/NAME/TABLE`,
  `DB_TABLE_INSTALLS` (BAF-11 stage 4 — installs/installs_retarget's own table, same validation as
  `DB_TABLE`), `APPSFLYER_API_TOKEN`, `APPSFLYER_APP_IDS`, `APPSFLYER_MEDIA_SOURCE`,
  `APPSFLYER_EVENT_NAMES`, `APPSFLYER_DAILY_LOOKBACK_DAYS` (default 1), `APPSFLYER_CHUNK_DAYS`
  (default 31), `APPSFLYER_TIMEZONE` (issue #53; unset = UTC,
```

- [ ] **Step 2: Add the `Table schema` bullet's installs mirror**

The bullet currently reading (line 116):

```
- **Table schema:** `sql/create_table.sql` (Stage 2), per Mark's DDL in BAF-2 comment 62293.
```

becomes:

```
- **Table schema:** `sql/create_table.sql` (Stage 2, in-app-events), per Mark's DDL in BAF-2 comment
  62293; `sql/create_table_installs.sql` (BAF-11 stage 4, installs/installs_retarget's own
  128-column table) — see `docs/superpowers/specs/2026-08-13-baf-11-column-sizing.md` for the
  column-length measurement it's sized from.
```

- [ ] **Step 3: Add a Non-Goals bullet scoping this stage's own limits**

Add, right after the existing Non-Goals list's last bullet (line 22, "Building analytics/BI on top
of the loaded data (out of scope for this ticket)."):

```
- **Making the installs/installs_retarget tables live in production (BAF-11 stage 4 scope note).**
  This stage registers the `ReportSpec`s and creates the table shape (usable manually and via
  `--dry-run`); it does not enable installs in the scheduled daily/backfill timer. Cutover — running
  installs against real data on a schedule — is BAF-11 Этап 9, itself gated on the
  `appsflyer_events_fb` PK/index migration (Этап 7, unrelated to installs' own new
  `idx_app_attr_install` index, which this stage's DDL already includes). The in-app-events
  exact-duplicate-collapse dedupe-policy question (Этап 8b) is likewise untouched by this stage.
```

- [ ] **Step 4: Gates + commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy`
Expected: all clean (no source logic touched — this just confirms nothing else broke).

```bash
git add docs/design-spec.md
git commit -m "BAF-11 stage 4: document DB_TABLE_INSTALLS and this stage's own Non-Goals scope"
```

---

### Task 8: Full regression gate, diff audit, PR

**Files:** none (verification only).

- [ ] **Step 1: Full gate**

Run: `uv run pre-commit run --all-files`
Expected: clean.

Run: `uv run pytest --cov-fail-under=98`
Expected: PASS at CI's gated threshold (locally, `tests/test_loader_integration.py`'s DB-dependent
tests may skip without a reachable DB — matches this repo's established precedent; CI's `mysql:8`
service container exercises them for real).

- [ ] **Step 2: Diff audit against the regression bar**

Run: `git diff main --stat` to see every changed file. Specifically confirm:
- `sql/create_table.sql` is **untouched** (in-app-events' schema didn't change).
- `src/appsflyer_pipeline/loader.py`'s `_IN_APP_EVENTS_CREATE_TABLE_TEMPLATE` content (the renamed
  Stage-2 template) is byte-identical to the pre-rename `_CREATE_TABLE_TEMPLATE` — only its constant
  name changed:
  `git diff main -- src/appsflyer_pipeline/loader.py | grep -A 40 '_IN_APP_EVENTS_CREATE_TABLE_TEMPLATE'`
  and read it — every line inside the template string should show as unchanged content, only the
  surrounding rename/new-template-addition as real diff.
- `git diff main -- tests/ | grep -E '^[+-]' | grep -vE '^(\+\+\+|---)' | grep -viE 'installs|spec=|REPORTS\[|report_name|dedupe_key|hard_clamp|db_table_installs|from appsflyer_pipeline|^\+$|^-$'`
  — every remaining line should be one of this plan's explicitly-specified structural fixes (the
  `create_table`/`load_events` call-site signature updates, the `one_series`/`len(items)` filter
  narrowing, the `_mock_all_ok`/`_url_for_spec` additions) — **no** line should be an EXISTING
  in-app-events test's expected value (a URL, a log substring, a row count, a dedupe outcome)
  changing. This grep is a coarse filter, not a proof — read what it returns.

- [ ] **Step 3: Confirm commit history matches the plan's tasks**

Run: `git log --oneline main..HEAD`
Expected: 8 commits (Tasks 1-7; Task 8 has no commit of its own), each matching one of this plan's
`git commit -m "BAF-11 stage 4: ..."` messages.

- [ ] **Step 4: Push and open the PR**

```bash
git push -u origin baf-11-stage-4-installs-report
gh pr create --title "BAF-11 stage 4: installs table + report (Этапы 5-6)" --body "$(cat <<'EOF'
## Summary
- `DB_TABLE_INSTALLS` config field + a 128-column DDL (`sql/create_table_installs.sql`, mirrored in
  `loader.py`) sized per the live column-sizing measurement, comfortably under the 65,535-byte row
  limit (~28.4 KB) via semantic-category typing for the always-empty-in-sample columns.
- Two new `ReportSpec`s (`installs_non_organic`, `installs_retargeting`) — full pass-through
  (`column_map=None`, normalized instead of hand-mapped), 47 `additional_fields`, a hard 60-day
  retention clamp (not a warning — see the plan's Architecture decision 3), and their own
  `(appsflyer_id, event_time)` dedupe key (NOT in-app-events' key — see decision 7 for the full
  rationale and an explicitly open risk flagged for Mark/data-analytics sign-off).
- `create-table`/`check-connection` now cover both tables. In-app-events' table, schema, dedupe
  behavior, and warn-only retention are unchanged — proven by the full existing test suite passing
  with no assertion-value changes.
- Explicitly does NOT enable installs in the scheduled timer (cutover is Этап 9) or touch the
  production `appsflyer_events_fb` PK/index migration (Этап 7).

## Test plan
- [ ] `uv run pre-commit run --all-files` clean
- [ ] `uv run pytest --cov-fail-under=98` passes in CI (mysql:8 service container, DB_TABLE_INSTALLS set)
- [ ] Manual: `uv run appsflyer-pipeline create-table` against a real DB reports both
      `appsflyer_events_fb` and the configured `DB_TABLE_INSTALLS` as ready
- [ ] Manual: `uv run appsflyer-pipeline backfill --dry-run --start-date <recent>` previews installs
      rows without writing
EOF
)"
```

Return the PR URL to the user.

---

## Final check before opening the PR

- [ ] `uv run pre-commit run --all-files` clean.
- [ ] `uv run pytest --cov-fail-under=98` passes (CI's real gate).
- [ ] `git diff main --stat` shows: `src/appsflyer_pipeline/{config.py, reports.py, transform.py,
  loader.py, pipeline.py, cli.py}`, `sql/create_table_installs.sql` (new), `.github/workflows/ci.yml`,
  `.env.example`, `deploy/{appsflyer.env.example, user-level/appsflyer.env.example}`,
  `docs/design-spec.md`, and the corresponding `tests/` files — no `sql/create_table.sql` (in-app-events
  untouched).
- [ ] No EXISTING in-app-events test's expected SQL string, HTTP param value, log substring, or
  row/column value changed — confirmed by Task 8 Step 2's diff audit.
- [ ] `git log --oneline main..HEAD` shows exactly the 8 commits from Tasks 1-7.
- [ ] The open dedupe-key risk (Architecture decision 7) and the "installs not yet live in
  production" scope note (Task 7) are both actually present in `docs/design-spec.md`/this plan, not
  just stated in the PR description — a future reader of the repo, not just this PR, needs to see them.
