# Issues #9 + #8 (+ #10 guard) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fail loudly on empty CSV config lists (#9), ship a config-only trailing-window mechanism for the daily load (#8, default 1 day = current behavior), and make window wipes visible in logs (#10) — delivered as two PRs.

**Architecture:** Three small, independent changes to existing modules: pydantic `min_length` constraints in `config.py`; a new bounded `appsflyer_daily_lookback_days` setting consumed by `pipeline.run_daily` (Mark's `days_back` window shape, ending yesterday); DELETE-rowcount logging in `loader.load_events`. No new files, no schema changes, no new dependencies.

**Tech Stack:** Python 3.12, uv, pydantic-settings v2, SQLAlchemy 2.0 + PyMySQL, typer, pytest (+respx), ruff, mypy --strict.

**Spec:** `docs/superpowers/specs/2026-07-08-issues-8-9-design.md`

## Global Constraints

- Run everything through uv: `uv run pytest`, `uv run pre-commit run --all-files` (ruff + ruff-format + mypy strict — same hooks CI gates on).
- CI also gates `pytest --cov-fail-under=98` with **branch** coverage — every new branch needs a covering test.
- Repo is **public**: no secrets, hostnames, or account names in code, docs, tests, or commit messages.
- Two branches/PRs: `issue-9-config-min-length` (Task 1–2; branch already exists with the spec commit) and `issue-8-daily-lookback` (Tasks 3–8, cut from `main`).
- **Never merge a PR** — merges happen only on the user's explicit authorization, outside this plan.
- Default daily behavior must not change: lookback default is **1**. Explicit `--date D` always pulls exactly `[D, D]`.
- New pydantic fields use the `Annotated[..., Field(...)] = default` style (the repo does not enable the pydantic mypy plugin; `field: int = Field(...)` assignments would fail mypy strict).
- Commit messages: imperative summary line referencing the issue, e.g. `Reject empty CSV config lists at startup (#9)`; end with the `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` trailer.
- `tests/test_loader_integration.py` tests must stay safe against the production DB: sentinel app_ids (`__pytest_...__`), cleanup via a delete-only `load_events` call in `finally`.

---

### Task 1: #9 — reject empty CSV config lists

**Files:**
- Modify: `src/appsflyer_pipeline/config.py:39,43`
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: existing `CsvList = Annotated[list[str], NoDecode]` and `_parse_csv_fields` before-validator (splits `""` → `[]`).
- Produces: `Settings()` raises `pydantic.ValidationError` when either CSV field resolves to an empty list. No signature changes.

- [ ] **Step 1: Confirm you are on branch `issue-9-config-min-length`**

```bash
git checkout issue-9-config-min-length && git log --oneline -1
```

Expected: HEAD is the "Add design spec…" commit (`ef37c47`).

- [ ] **Step 2: Write the failing tests**

Append to `tests/test_config.py`:

```python
@pytest.mark.parametrize("field", ["APPSFLYER_APP_IDS", "APPSFLYER_EVENT_NAMES"])
@pytest.mark.parametrize("raw", ["", "   ", " , ,"])
def test_empty_csv_list_rejected(monkeypatch: pytest.MonkeyPatch, field: str, raw: str) -> None:
    """A truncated/fat-fingered EnvironmentFile line (issue #9) must abort startup,
    not degrade to a silent no-op run (empty app list) or an active window wipe
    (empty event list -> transform's is_in([]) drops every row before the load).
    """
    with pytest.raises(ValidationError):
        _settings(monkeypatch, **{field: raw})
```

- [ ] **Step 3: Run them to verify they fail**

Run: `uv run pytest tests/test_config.py -v -k empty_csv`
Expected: 6 FAILED, each with `Failed: DID NOT RAISE <class 'pydantic_core._pydantic_core.ValidationError'>`

- [ ] **Step 4: Implement the constraints**

In `src/appsflyer_pipeline/config.py`, change the import line

```python
from pydantic import field_validator
```

to

```python
from pydantic import Field, field_validator
```

and change the two field declarations

```python
    appsflyer_api_token: str
    appsflyer_app_ids: CsvList

    # Run parameters — defaulted to the BAF-2 acceptance criteria, overridable via env.
    appsflyer_media_source: str = "Facebook Ads"
    appsflyer_event_names: CsvList = ["af_purchase", "af_purchase_YC"]  # noqa: RUF012
```

to

```python
    appsflyer_api_token: str
    # min_length=1 (issue #9): an empty value (e.g. a truncated line in the server's
    # hand-edited EnvironmentFile) must fail startup loudly — an empty app list is a
    # silent no-op run that exits 0, and an empty event list actively wipes windows
    # (transform re-filters with is_in([]) and the loader then delete-then-inserts nothing).
    appsflyer_app_ids: Annotated[CsvList, Field(min_length=1)]

    # Run parameters — defaulted to the BAF-2 acceptance criteria, overridable via env.
    appsflyer_media_source: str = "Facebook Ads"
    appsflyer_event_names: Annotated[CsvList, Field(min_length=1)] = [  # noqa: RUF012
        "af_purchase",
        "af_purchase_YC",
    ]
```

- [ ] **Step 5: Run the new tests to verify they pass**

Run: `uv run pytest tests/test_config.py -v`
Expected: all PASS (the 6 new + 4 existing — valid configs unaffected).

- [ ] **Step 6: Run the full local gates**

Run: `uv run pre-commit run --all-files && uv run pytest`
Expected: all hooks pass; full suite passes (integration tests may SKIP without a reachable DB — that's fine).

- [ ] **Step 7: Commit**

```bash
git add src/appsflyer_pipeline/config.py tests/test_config.py
git commit -m "$(cat <<'EOF'
Reject empty CSV config lists at startup (#9)

min_length=1 on appsflyer_app_ids and appsflyer_event_names: an empty
APPSFLYER_APP_IDS made the daily job a silent no-op that exits 0, and an
empty APPSFLYER_EVENT_NAMES actively wiped loaded windows via the
is_in([]) re-filter. Both now fail with a ValidationError before any
API/DB work.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: PR 1 — push and open

**Files:** none (git/gh only)

**Interfaces:**
- Consumes: branch `issue-9-config-min-length` with Task 1's commit + the spec commit.
- Produces: an open PR whose merge auto-closes #9. **Do not merge.**

- [ ] **Step 1: Push the branch**

```bash
git push -u origin issue-9-config-min-length
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --title "Reject empty APPSFLYER_APP_IDS / APPSFLYER_EVENT_NAMES at startup" --body "$(cat <<'EOF'
Fixes #9.

`min_length=1` on both CSV list fields. An empty `APPSFLYER_APP_IDS` (e.g. a truncated
line in the mode-600 systemd `EnvironmentFile`) previously produced a successful no-op
run (`Loaded 0 rows across 0/0 windows.`, exit 0); an empty `APPSFLYER_EVENT_NAMES`
was worse — transform's `is_in([])` re-filter drops every fetched row, after which the
idempotent delete-then-insert *erases* the already-loaded window. Both now abort at
`Settings()` construction with a pydantic `ValidationError`, surfaced by the CLI as
`FAILED: …` + exit 1 (nonzero exit ⇒ visible as a failed unit in systemd).

Also carries the brainstormed design spec for this and the companion #8/#10 PR
(`docs/superpowers/specs/2026-07-08-issues-8-9-design.md`).

Tests: empty and whitespace-only values for both fields (6 parametrized cases).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Wait for CI and report**

Run: `gh pr checks --watch`
Expected: all checks green. Report the PR URL + CI status to the user. **Stop — merging needs explicit user authorization.**

---

### Task 3: #8 — `appsflyer_daily_lookback_days` setting

**Files:**
- Modify: `src/appsflyer_pipeline/config.py` (Settings class)
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: `Settings`/`Field` from Task 1 (both PRs touch `config.py`; this branch is cut from `main`, so write the change against whatever is on `main` — the field is additive and does not overlap Task 1's lines).
- Produces: `settings.appsflyer_daily_lookback_days: int` (env `APPSFLYER_DAILY_LOOKBACK_DAYS`), default `1`, validated `1 ≤ N ≤ 90`. Task 4 reads it in `run_daily`.

- [ ] **Step 1: Cut the branch from main**

```bash
git checkout main && git checkout -b issue-8-daily-lookback
```

- [ ] **Step 2: Write the failing tests**

Append to `tests/test_config.py`:

```python
def test_daily_lookback_defaults_to_single_day(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _settings(monkeypatch).appsflyer_daily_lookback_days == 1


def test_daily_lookback_accepts_valid_depth(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(monkeypatch, APPSFLYER_DAILY_LOOKBACK_DAYS="3")
    assert settings.appsflyer_daily_lookback_days == 3


@pytest.mark.parametrize("raw", ["0", "-3", "91", "not-a-number"])
def test_daily_lookback_out_of_bounds_rejected(
    monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    with pytest.raises(ValidationError):
        _settings(monkeypatch, APPSFLYER_DAILY_LOOKBACK_DAYS=raw)
```

If Task 1's PR is not merged yet, this branch's `tests/test_config.py` won't have the
`test_empty_csv_list_rejected` block — that's expected; do not copy it here.

- [ ] **Step 3: Run them to verify they fail**

Run: `uv run pytest tests/test_config.py -v -k lookback`
Expected: `test_daily_lookback_defaults_to_single_day` FAILS with `AttributeError: 'Settings' object has no attribute 'appsflyer_daily_lookback_days'`; the bounds cases FAIL with `DID NOT RAISE` (the unknown env var is ignored by `extra="ignore"`).

- [ ] **Step 4: Implement the setting**

In `src/appsflyer_pipeline/config.py`, ensure the pydantic import includes `Field` (it will
if Task 1 merged first; add it otherwise), and add below `appsflyer_event_names`:

```python
    # Daily trailing-window depth (issue #8): the scheduled `daily` run pulls
    # [yesterday - (N-1), yesterday]. Default 1 preserves the original single-day
    # pull; deeper windows re-capture AppsFlyer late/offline-cached events (the
    # 05:00 +03 timer fires exactly at AppsFlyer's 02:00 UTC late-event boundary)
    # and cost no extra API quota at N <= 31 (one report download per
    # app/attribution regardless of range length). Upper bound = the Pull API's
    # ~90-day retention (appsflyer_client.MAX_RETENTION_DAYS; literal here to
    # keep config free of package imports).
    appsflyer_daily_lookback_days: Annotated[int, Field(ge=1, le=90)] = 1
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_config.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/appsflyer_pipeline/config.py tests/test_config.py
git commit -m "$(cat <<'EOF'
Add bounded APPSFLYER_DAILY_LOOKBACK_DAYS setting (#8)

Trailing-window depth for the daily run, 1..90 (Pull API retention).
Default 1 = the original single-day behavior; consumption in run_daily
lands in the next commit.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: #8 — `run_daily` trailing window + CLI help

**Files:**
- Modify: `src/appsflyer_pipeline/pipeline.py:271-276` (`run_daily`)
- Modify: `src/appsflyer_pipeline/cli.py:140-149` (`daily` command help text only)
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `settings.appsflyer_daily_lookback_days` (Task 3); existing `_run_window`, `_today`, `get_settings` in `pipeline.py`.
- Produces: `run_daily(*, date=None, dry_run=False)` — same signature. Default window `[yesterday−(N−1), yesterday]`; explicit `date` ⇒ `[date, date]` always.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pipeline.py` (uses the file's existing `_set_env`, `load_spy`, `_mock_all_ok`, `APP_IDS`, `ATTRIBUTION_TYPES` helpers):

```python
def test_run_daily_lookback_widens_default_window(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    _set_env(monkeypatch, APPSFLYER_DAILY_LOOKBACK_DAYS="3")
    fixed_today = datetime.date(2026, 7, 7)
    monkeypatch.setattr(pipeline, "_today", lambda: fixed_today)
    expected_end = fixed_today - datetime.timedelta(days=1)
    expected_start = expected_end - datetime.timedelta(days=2)

    with respx.mock:
        _mock_all_ok()
        summary = run_daily(dry_run=True)

    assert all(
        r.start_date == expected_start and r.end_date == expected_end for r in summary.results
    )
    # 3 days <= 31 -> still exactly one chunk (one report download) per combo: no extra quota.
    assert len(summary.results) == len(APP_IDS) * len(ATTRIBUTION_TYPES)


def test_run_daily_explicit_date_ignores_lookback(
    monkeypatch: pytest.MonkeyPatch, load_spy: list[dict[str, Any]]
) -> None:
    """--date is a targeted repair tool: exactly [date, date], lookback or not."""
    _set_env(monkeypatch, APPSFLYER_DAILY_LOOKBACK_DAYS="3")
    target = datetime.date(2026, 5, 20)

    with respx.mock:
        _mock_all_ok()
        summary = run_daily(date=target, dry_run=True)

    assert all(r.start_date == target and r.end_date == target for r in summary.results)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_pipeline.py -v -k lookback_widens`
Expected: FAIL on the first `assert all(...)` (window is still single-day).
(`test_run_daily_explicit_date_ignores_lookback` already passes today — it pins the
behavior against regression; that's fine.)

- [ ] **Step 3: Implement the window logic**

Replace `run_daily` in `src/appsflyer_pipeline/pipeline.py` (currently lines 271-276) with:

```python
def run_daily(*, date: datetime.date | None = None, dry_run: bool = False) -> RunSummary:
    """Daily incremental load, sharing run_backfill's fetch/transform/load path.

    The default window is [yesterday - (N-1), yesterday] where N is
    settings.appsflyer_daily_lookback_days — the same days_back shape as the
    backfill, re-pulling recent days on every run so late/offline-cached
    AppsFlyer events get captured (issue #8). N=1 (the default) is the
    original single-day pull. Idempotent delete-then-insert makes the daily
    rewrite of recent days safe by construction.

    An explicit `date` is a targeted repair tool and always pulls exactly
    [date, date], regardless of the lookback setting.
    """
    if date is not None:
        return _run_window(date, date, dry_run=dry_run)
    end = _today() - datetime.timedelta(days=1)
    start = end - datetime.timedelta(days=get_settings().appsflyer_daily_lookback_days - 1)
    return _run_window(start, end, dry_run=dry_run)
```

- [ ] **Step 4: Update the CLI help text**

In `src/appsflyer_pipeline/cli.py`, replace the `daily` command's docstring and `--date` option:

```python
@app.command()
def daily(
    date: str | None = typer.Option(
        None,
        "--date",
        help=(
            "ISO date (YYYY-MM-DD): pull exactly this one day (targeted repair), "
            "ignoring APPSFLYER_DAILY_LOOKBACK_DAYS. Default: the trailing "
            "lookback window ending yesterday."
        ),
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Fetch and transform but don't write to the database."
    ),
) -> None:
    """Daily incremental load: pulls the trailing lookback window (default: yesterday
    only) from both sources.
    """
```

(The command body is unchanged.)

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_pipeline.py tests/test_cli.py -v`
Expected: all PASS, including the pre-existing `test_run_daily_defaults_to_yesterday`
(default N=1 is byte-for-byte the old behavior).

- [ ] **Step 6: Commit**

```bash
git add src/appsflyer_pipeline/pipeline.py src/appsflyer_pipeline/cli.py tests/test_pipeline.py
git commit -m "$(cat <<'EOF'
Give run_daily a configurable trailing window (#8)

Default window becomes [yesterday-(N-1), yesterday] with
N = APPSFLYER_DAILY_LOOKBACK_DAYS (default 1 — behavior unchanged until
an operator opts in). Explicit --date stays a strict single-day pull for
targeted repairs.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: #10 — wipe visibility in `load_events`

**Files:**
- Modify: `src/appsflyer_pipeline/loader.py` (module header + `load_events`)
- Test: `tests/test_loader_integration.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: unchanged `load_events` signature/return. New log lines on logger `appsflyer_pipeline.loader`: INFO `... deleted=N inserted=M` on every load; WARNING containing `wiped` when `deleted > 0` and `rows == []`.

- [ ] **Step 1: Write the failing integration test**

Append to `tests/test_loader_integration.py` (add `import logging` next to the existing
`import datetime` at the top):

```python
def test_load_events_logs_rowcounts_and_warns_on_wipe(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Issue #10: a successful-but-empty fetch silently erased a loaded window.
    Every load must log deleted/inserted counts; a non-empty->empty transition
    must WARN so it is loud in journalctl. Sentinel app_id + delete-only cleanup
    keep this safe against the production DB (same pattern as the test above).
    """
    try:
        settings = get_settings()
        engine = create_engine(settings)
        create_table(engine, settings.db_table)
    except Exception as exc:  # noqa: BLE001 - environment without a reachable/configured DB
        pytest.skip(f"no usable database in this environment: {exc}")

    test_app_id = "__pytest_wipe_test_app__"
    test_attribution = "non_organic"
    window = datetime.date(2020, 1, 2)
    row = {
        "event_time": datetime.datetime(2020, 1, 2, 12, 0, 0),
        "install_time": None,
        "attributed_touch_time": None,
        "event_name": "af_purchase",
        "event_revenue": Decimal("1.23"),
        "media_source": "Facebook Ads",
        "channel": None,
        "campaign": None,
        "campaign_id": None,
        "adset": None,
        "adset_id": None,
        "ad": None,
        "ad_id": None,
        "appsflyer_id": "test-af-id",
        "customer_user_id": None,
        "attribution_type": test_attribution,
        "app_id": test_app_id,
    }

    try:
        with caplog.at_level(logging.INFO, logger="appsflyer_pipeline.loader"):
            load_events(
                engine,
                settings.db_table,
                [row],
                app_id=test_app_id,
                attribution_type=test_attribution,
                start_date=window,
                end_date=window,
            )
        # First load into an empty window: counts logged, nothing to warn about.
        assert any(
            "deleted=0" in r.message and "inserted=1" in r.message for r in caplog.records
        )
        assert not any(r.levelno == logging.WARNING for r in caplog.records)

        caplog.clear()
        with caplog.at_level(logging.INFO, logger="appsflyer_pipeline.loader"):
            load_events(
                engine,
                settings.db_table,
                [],
                app_id=test_app_id,
                attribution_type=test_attribution,
                start_date=window,
                end_date=window,
            )
        # Re-loading the now-populated window with zero rows is a wipe: WARN.
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "wiped" in warnings[0].message
        assert "deleted=1" in warnings[0].message
        assert any(
            "deleted=1" in r.message and "inserted=0" in r.message
            for r in caplog.records
            if r.levelno == logging.INFO
        )
    finally:
        # Delete-only call for the same window cleans up regardless of outcome.
        load_events(
            engine,
            settings.db_table,
            [],
            app_id=test_app_id,
            attribution_type=test_attribution,
            start_date=window,
            end_date=window,
        )
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_loader_integration.py::test_load_events_logs_rowcounts_and_warns_on_wipe -v`
Expected: FAIL on the first `assert any("deleted=0" ...)` (no such log line exists yet).
If it SKIPs instead ("no usable database"), there is no reachable DB in this
environment — note that in the task report; CI's `mysql:8` service container will
exercise it, and the coverage gate runs there.

- [ ] **Step 3: Implement the logging**

In `src/appsflyer_pipeline/loader.py`, add to the imports (after `import re`):

```python
import logging
```

and after the imports block (module level, mirroring `pipeline.py`):

```python
logger = logging.getLogger(__name__)
```

Then in `load_events`, change the transaction block

```python
    try:
        with engine.begin() as conn:
            conn.execute(
                delete_stmt,
                {
                    "app_id": app_id,
                    "attribution_type": attribution_type,
                    "window_start": window_start,
                    "window_end": window_end,
                },
            )
            if rows:
                conn.execute(insert_stmt, rows)
    except SQLAlchemyError as exc:
```

to

```python
    try:
        with engine.begin() as conn:
            deleted = conn.execute(
                delete_stmt,
                {
                    "app_id": app_id,
                    "attribution_type": attribution_type,
                    "window_start": window_start,
                    "window_end": window_end,
                },
            ).rowcount
            if rows:
                conn.execute(insert_stmt, rows)
    except SQLAlchemyError as exc:
```

and change the final `return len(rows)` to:

```python
    logger.info(
        "loaded app_id=%s attribution_type=%s window=[%s, %s]: deleted=%d inserted=%d",
        app_id,
        attribution_type,
        start_date,
        end_date,
        deleted,
        len(rows),
    )
    if deleted > 0 and not rows:
        # Issue #10: delete-then-insert makes a successful-but-empty fetch erase an
        # already-loaded window with zero trace. Legitimate only if AppsFlyer really
        # revised the window to zero events — so it must be loud in journalctl.
        logger.warning(
            "wiped previously loaded window: app_id=%s attribution_type=%s "
            "window=[%s, %s] deleted=%d inserted=0",
            app_id,
            attribution_type,
            start_date,
            end_date,
            deleted,
        )
    return len(rows)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_loader_integration.py tests/test_loader.py -v`
Expected: all PASS (or the integration file SKIPs uniformly without a DB — then rely on
CI for the green evidence and say so in the report).

- [ ] **Step 5: Commit**

```bash
git add src/appsflyer_pipeline/loader.py tests/test_loader_integration.py
git commit -m "$(cat <<'EOF'
Log DELETE rowcount and warn on window wipes in load_events (#10)

Every load now logs deleted=N inserted=M; a non-empty window replaced
with zero rows logs a WARNING, so a successful-but-empty fetch erasing
loaded data is visible in journalctl instead of silent. Required to land
with #8's window widening, which raises the wipe blast radius.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: docs + env template

**Files:**
- Modify: `deploy/appsflyer.env.example` (Run parameters section)
- Modify: `docs/design-spec.md:87` (Daily bullet), `:98-99` (Config bullet), Risks table (add a row after line 122's quota row)
- Modify: `docs/RUNBOOK.md` §5 format-reminder paragraph (after line 106)

**Interfaces:** none (docs only).

- [ ] **Step 1: Extend `deploy/appsflyer.env.example`**

Append to the `# --- Run parameters ---` section:

```
# Daily trailing-window depth (issue #8): the scheduled `daily` run pulls
# [yesterday-(N-1), yesterday]. Default (unset) = 1, the original single-day
# pull. Recommended in production: 3 -- the timer fires at 05:00 +03, which is
# exactly AppsFlyer's 02:00 UTC late-event boundary, and offline devices'
# SDK-cached events can arrive days late; a 3-day window re-captures them at
# ZERO extra API quota (any depth <=31 days is still one report download per
# app/attribution per run). Loads are idempotent, so the daily rewrite is safe.
#APPSFLYER_DAILY_LOOKBACK_DAYS=3
```

- [ ] **Step 2: Update `docs/design-spec.md`**

Replace the Daily bullet (line 87):

```markdown
- **Daily:** window = [yesterday − (N−1), yesterday], N = `APPSFLYER_DAILY_LOOKBACK_DAYS`
  (default 1, i.e. the original [yesterday, yesterday] — issue #8); same client/transform/load
  path as one backfill chunk. An explicit `--date` pulls exactly that one day (targeted repair),
  ignoring the lookback.
```

In the Config bullet (lines 98-99), extend the env-var list:

```markdown
- **Config (env / `.env`):** see `.env.example` — `DB_HOST/PORT/USER/PASSWORD/NAME/TABLE`,
  `APPSFLYER_API_TOKEN`, `APPSFLYER_APP_IDS`, `APPSFLYER_MEDIA_SOURCE`, `APPSFLYER_EVENT_NAMES`,
  `APPSFLYER_DAILY_LOOKBACK_DAYS` (default 1). The two CSV list fields reject empty values at
  startup (issue #9) — a truncated EnvironmentFile line fails loudly instead of producing a
  silent no-op run (empty app list) or an active window wipe (empty event list).
```

Add a row to the Risks table, directly after the daily-quota row (line 122):

```markdown
| **Late/offline-cached events arrive after the daily pull** (the 05:00 +03 timer = exactly AppsFlyer's documented 02:00 UTC late-event boundary; SDK-cached events from offline devices can arrive days late — issue #8) | Slow, silent under-count of purchases/revenue: a single-day window never revisits past days, and every run still reports success | `APPSFLYER_DAILY_LOOKBACK_DAYS` re-pulls a trailing window daily — zero extra quota at depths ≤31 (still one report download per combo per run) and idempotent by construction. **Default is 1 (original behavior); production enablement (recommended: 3) is an explicit operator decision, flagged here rather than silently changed.** #10's wipe-visibility logging covers the widened delete window. |
```

- [ ] **Step 3: Update `docs/RUNBOOK.md` §5**

Append to the "Format reminder" paragraph (after line 106):

```markdown
Optional: `APPSFLYER_DAILY_LOOKBACK_DAYS=3` widens the daily run to a trailing 3-day
window ending yesterday, re-capturing AppsFlyer late/offline-cached events at no extra
report-download quota (default when unset: 1 = yesterday only; see
`deploy/appsflyer.env.example` for the full rationale).
```

- [ ] **Step 4: Run the gates (docs are still linted for whitespace by pre-commit)**

Run: `uv run pre-commit run --all-files`
Expected: all hooks pass.

- [ ] **Step 5: Commit**

```bash
git add deploy/appsflyer.env.example docs/design-spec.md docs/RUNBOOK.md
git commit -m "$(cat <<'EOF'
Document the daily lookback window and late-event risk (#8)

env.example template entry (recommended production value: 3), design-spec
daily-window/config updates plus a Risks-table row that flags production
enablement as an explicit operator decision, and a RUNBOOK §5 note.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: PR 2 — gates, push, open

**Files:** none (git/gh only)

**Interfaces:**
- Consumes: branch `issue-8-daily-lookback` with Tasks 3–6 committed.
- Produces: an open PR that closes #10 on merge and **references** #8 without closing it. **Do not merge.**

- [ ] **Step 1: Full local gates**

Run: `uv run pre-commit run --all-files && uv run pytest`
Expected: all pass (integration may SKIP locally without a DB).

- [ ] **Step 2: Push and open the PR**

```bash
git push -u origin issue-8-daily-lookback
gh pr create --title "Configurable daily lookback window + wipe-visibility logging" --body "$(cat <<'EOF'
Fixes #10. Addresses #8 (mechanism only — see below), per the design spec
`docs/superpowers/specs/2026-07-08-issues-8-9-design.md` (in PR for #9).

**#8 — daily trailing window.** `run_daily`'s default window becomes
`[yesterday − (N−1), yesterday]` with `N = APPSFLYER_DAILY_LOOKBACK_DAYS` (bounded 1–90) —
Mark's `days_back` window shape from the backfill, run daily. **The default is 1, so merged
behavior is byte-for-byte unchanged**; enabling a deeper window (recommended: 3, zero extra
report-download quota at depths ≤31) is a production-config decision made in the server's
EnvironmentFile, documented in `deploy/appsflyer.env.example` and RUNBOOK §5. #8 therefore
stays open until that enablement decision — this PR deliberately does not auto-close it.
Explicit `--date` remains a strict single-day pull for targeted repairs.

**#10 — wipe visibility.** `load_events` now logs `deleted=N inserted=M` on every load and
WARNs when a previously non-empty window is replaced with zero rows — required to land with
the window widening, which raises the wipe blast radius.

Tests: lookback bounds/default in `test_config.py`; widened-window + `--date`-ignores-lookback
in `test_pipeline.py`; rowcount-logging/wipe-warning integration test in
`test_loader_integration.py` (runs against CI's mysql:8 service container).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Wait for CI and report**

Run: `gh pr checks --watch`
Expected: all green (including the coverage gate — the loader branches are exercised by the
integration test against CI's mysql:8). Report the PR URL + CI status. **Stop — no merging.**

---

### Task 8: Live verification (user's standing preference — real runs over mocks)

**Files:** none (runs the CLI from the `issue-8-daily-lookback` working tree against the real `.env`).

**Quota budget (2026-07-08):** the daily quota is ~6-7 report downloads per (app_id,
attribution_type) per day, and today has already spent ~3-4 on the hottest combo (morning
manual daily + the #7 purge backfill + one dry-run). Per the design-spec's own operational
takeaway ("prefer going straight to a real call over a dry-run-then-real pair"), do **not**
run a default-window dry-run (it proves nothing the tests don't). The two steps below cost
2 downloads per combo. If any window fails with the known quota HTTP 400, report it as the
expected per-window-isolated quota behavior (not a bug) and defer that step to the next
quota reset rather than retrying.

- [ ] **Step 1: Lookback mechanism, dry-run (no DB writes)**

```bash
APPSFLYER_DAILY_LOOKBACK_DAYS=3 uv run appsflyer-pipeline daily --dry-run
```

Expected: each result line shows a 3-day window (`2026-07-05..2026-07-07` when run on
2026-07-08), exactly one window per (app_id, attribution_type) — 4 total — and a plausible
"Would load N rows" total. Real env vars take precedence over `.env`, so the override
applies even with a populated `.env`.

- [ ] **Step 2: Real run at default settings — #10's log line live**

```bash
uv run appsflyer-pipeline daily
```

Expected: default (N=1) single-day window = yesterday; the new
`loaded app_id=... deleted=D inserted=M` INFO lines appear, with `deleted` ≈ `inserted`
(yesterday was already loaded this morning, so this is an idempotent rewrite — stable row
counts, matching the morning's ~133 rows across 4 windows), and **no** wipe WARNINGs.

- [ ] **Step 3: Report results to the user**

Summarize: windows, fetched/loaded counts, the observed `deleted=/inserted=` lines, any
quota 400s. Then stop — merge authorization and the production
`APPSFLYER_DAILY_LOOKBACK_DAYS=3` enablement decision (which is what ultimately closes #8)
both belong to the user.
