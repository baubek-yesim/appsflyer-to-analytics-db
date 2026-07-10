# is_primary_attribution column + dedup view Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist AppsFlyer's `Is Primary Attribution` flag as a `is_primary_attribution BOOLEAN NOT NULL` column and ship a `<table>_deduped` view that collapses cross-attribution duplicate purchases (keep the primary row of a pair, keep all singletons) — so Mark can get a de-duplicated total without dropping the non-FB-primary singles that #47 restored.

**Architecture:** The flag is a standard column already present in **both** report exports (live-verified 2026-07-10: non_organic has a true/false mix; retargeting is `true` — a dual-attributed purchase is `false` in the non_organic pull and `true` in its retargeting twin). `transform` reads it into every row; `loader` writes it and exposes a `ROW_NUMBER()`-based dedup view; a two-phase migration adds the column to the already-provisioned production table; a prompt re-backfill repopulates the existing 3,467 rows while `06-05` is still fetchable.

**Tech Stack:** Python 3.12 (uv), polars, SQLAlchemy 2.0 + PyMySQL, typer CLI, pytest + respx, ruff + mypy (strict), MariaDB/MySQL.

## Global Constraints

- Python 3.12, managed with **uv** (`uv run …`); never pip/poetry.
- **Fail-loud** on unexpected data (match `_parse_timestamp`/`_parse_revenue` and issues #26/#29): raise `TransformError`, never silently default.
- Every SQL identifier interpolated into raw SQL passes `loader._validate_identifier` first — values are bound, identifiers are validated.
- Table name comes from `DB_TABLE` (`settings.db_table`); nothing hardcodes `appsflyer_events_fb` except the reference `sql/*.sql` files.
- `sql/create_table.sql` must stay column-for-column in sync with `loader._CREATE_TABLE_TEMPLATE`.
- Public repo — no secrets, no real host/user identifiers in committed files. `scratch/` and `reference/` are gitignored.
- Merge gate: `uv run pre-commit run --all-files` (ruff + ruff-format + mypy) and `uv run pytest` with CI's `--cov-fail-under=98` must pass.
- Do not request `additional_fields=is_primary_attribution` — it returns HTTP 400 (#7). The flag is read from the standard export column only.
- All production actions (ALTER, deploy, create-view, re-backfill) are performed by the controller directly, each on its own explicit user go-ahead — never delegated to a subagent (the #7/#14/#26 mixed-automation pattern).

## File structure

- `src/appsflyer_pipeline/transform.py` — add `_parse_bool`, map + store the flag (Task 1).
- `src/appsflyer_pipeline/loader.py` — add the column to the table template + `_INSERT_COLUMNS`; add `_CREATE_VIEW_TEMPLATE` + `create_view()` (Tasks 2, 3).
- `src/appsflyer_pipeline/cli.py` — add the `create-view` command (Task 3).
- `sql/create_table.sql` — add the column (Task 2).
- `sql/create_view.sql` — new reference DDL for the dedup view (Task 3).
- `sql/migrations/2026-07-10-add-is-primary-attribution.sql` — new two-phase migration (Task 2).
- `tests/test_transform.py`, `tests/test_loader.py`, `tests/test_loader_integration.py`, `tests/test_cli.py`, `tests/test_pipeline.py` — updated/new tests (all tasks).
- `docs/design-spec.md`, `docs/RUNBOOK.md` — attribution model, migration, create-view, re-backfill timing (Tasks 2, 3, 4).

---

### Task 1: Store `is_primary_attribution` in transform

**Files:**
- Modify: `src/appsflyer_pipeline/transform.py` (`_COLUMN_MAP` ~26-42; add `_parse_bool` after `_parse_revenue` ~67; row-build loop ~152-164; docstring ~123-129)
- Test: `tests/test_transform.py`

**Interfaces:**
- Produces: `transform_events(...)` now returns row dicts that include `"is_primary_attribution": bool`. `_parse_bool(value: str | None) -> bool` (raises `TransformError` on anything but `true`/`false`, case/space-insensitive).
- Consumes: nothing new.

- [ ] **Step 1: Write the failing tests**

In `tests/test_transform.py`, update the map test to assert the new field (add after line 86, the `app_id` assert):

```python
    assert row["is_primary_attribution"] is True
```

Replace `test_transform_keeps_non_primary_rows` (currently lines 230-249) so it also pins the stored flag — this doubles as the falsy-trap regression (a `false` row must NOT be treated as a missing required field):

```python
def test_transform_stores_primary_flag_and_keeps_non_primary_rows() -> None:
    """Issue #55: the flag is now persisted (still never filtered — #47).
    A row with Is Primary Attribution=false must load AND store False; it must
    not be dropped by the _REQUIRED_NOT_NULL truthiness check.
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
    assert [r["is_primary_attribution"] for r in rows] == [True, False]
```

Invert `test_transform_never_requires_the_flag_column` (lines 252-266) — after #55 the flag IS required in both reports (live-verified present in both):

```python
@pytest.mark.parametrize("attribution_type", ["non_organic", "retargeting"])
def test_transform_now_requires_the_flag_column(attribution_type: AttributionType) -> None:
    """Issue #55 reverses #47's 'flag not required': the column is now persisted,
    so a response missing it is schema drift. Live-verified 2026-07-10 that both
    the non_organic and retargeting exports include 'Is Primary Attribution'.
    """
    df = _df([_raw_row()]).drop("Is Primary Attribution")
    with pytest.raises(TransformError, match="missing expected column"):
        transform_events(
            df,
            attribution_type=attribution_type,
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase"],
        )
```

Add `_parse_bool` unit coverage via the public API (parametrized):

```python
@pytest.mark.parametrize(
    ("raw", "expected"),
    [("true", True), ("false", False), ("TRUE", True), ("  False  ", False)],
)
def test_transform_parses_primary_flag(raw: str, expected: bool) -> None:
    df = _df([_raw_row(**{"Is Primary Attribution": raw})])
    rows = transform_events(
        df,
        attribution_type="non_organic",
        app_id="id1458505230",
        media_source_filter="Facebook Ads",
        event_names_filter=["af_purchase"],
    )
    assert rows[0]["is_primary_attribution"] is expected


@pytest.mark.parametrize("raw", ["", "yes", "1", None])
def test_transform_raises_on_bad_primary_flag(raw: str | None) -> None:
    df = _df([_raw_row(**{"Is Primary Attribution": raw})])
    with pytest.raises(TransformError, match="is_primary_attribution"):
        transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase"],
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_transform.py -k "primary or requires_the_flag or maps_columns" -v`
Expected: FAIL — `KeyError: 'is_primary_attribution'` / the requires-flag test still passes the old way / `_parse_bool` behavior absent.

- [ ] **Step 3: Implement — add the mapping, the parser, and the store**

In `_COLUMN_MAP` (transform.py), add after the `"Customer User ID": "customer_user_id",` line:

```python
    "Is Primary Attribution": "is_primary_attribution",
```

Add `_parse_bool` immediately after `_parse_revenue` (after line 67):

```python
def _parse_bool(value: str | None) -> bool:
    if value is not None:
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    raise TransformError(f"Unexpected is_primary_attribution value: {value!r}")
```

In the row-build loop, after the `row["event_revenue"] = _parse_revenue(row["event_revenue"])` line (currently line 156), add:

```python
        row["is_primary_attribution"] = _parse_bool(row["is_primary_attribution"])
```

Update the `transform_events` docstring paragraph (lines 123-129) to note the flag is now persisted:

```python
    ALL rows are loaded regardless of `Is Primary Attribution` (issue #47,
    data-analytics decision on #46, reversing #7's filter). Issue #55 now also
    *persists* the flag as `is_primary_attribution` (still never filters on it):
    it is a standard column in both the non_organic and retargeting exports
    (live-verified 2026-07-10), and the `<table>_deduped` view uses it to collapse
    a dual-attributed purchase (false in the UA pull, true in its retargeting
    twin) to one row while keeping every single-attribution row.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_transform.py -v`
Expected: PASS (all, including the #26 headers-only test — `HEADERS_ONLY_EXPORT` already contains the flag column).

- [ ] **Step 5: Commit**

```bash
git add src/appsflyer_pipeline/transform.py tests/test_transform.py
git commit -m "Persist is_primary_attribution in transform (#55)"
```

---

### Task 2: Persist the column — schema template, INSERT, migration

**Files:**
- Modify: `src/appsflyer_pipeline/loader.py` (`_CREATE_TABLE_TEMPLATE` 101-124; `_INSERT_COLUMNS` 139-157)
- Modify: `sql/create_table.sql`
- Create: `sql/migrations/2026-07-10-add-is-primary-attribution.sql`
- Modify: `tests/test_loader_integration.py` (two row dicts: 83-101, 165-183)
- Modify: `docs/RUNBOOK.md` (migration section)

**Interfaces:**
- Consumes: `transform_events` rows now carry `is_primary_attribution` (Task 1).
- Produces: `create_table` builds a table whose schema includes `is_primary_attribution`; `load_events` writes it (its INSERT binds `:is_primary_attribution`, so every row dict must contain the key).

- [ ] **Step 1: Write the failing test**

In `tests/test_loader_integration.py`, add `"is_primary_attribution": True,` to **both** row dicts — after the `"attribution_type": test_attribution,` line in each (lines ~99 and ~181). Then add a schema assertion to `test_create_table_is_idempotent` (after line 66):

```python
    with engine.connect() as conn:
        cols = {
            r[0]
            for r in conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = :t"
                ),
                {"t": settings.db_table},
            )
        }
    assert "is_primary_attribution" in cols
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_loader_integration.py -v`
Expected: FAIL against a reachable DB (`is_primary_attribution` not in `cols`, and/or INSERT errors on the unknown column). If no DB is reachable it SKIPS — in that case rely on Step 4's CI-equivalent run; note the skip.

- [ ] **Step 3: Implement — template, INSERT columns, SQL files, migration**

In `loader._CREATE_TABLE_TEMPLATE`, add after the `attribution_type` line (line 119):

```sql
    `is_primary_attribution` TINYINT(1)    NOT NULL,
```

In `_INSERT_COLUMNS`, add `"is_primary_attribution",` after `"attribution_type",` (line 155).

Apply the identical column addition to `sql/create_table.sql` (after its `attribution_type` line, keeping the two in sync).

Create `sql/migrations/2026-07-10-add-is-primary-attribution.sql`:

```sql
-- BAF-2 issue #55: add the is_primary_attribution flag to the already-provisioned
-- production table (CREATE TABLE IF NOT EXISTS does not retrofit an existing table).
-- Run PHASE 1, then re-backfill the retained window to populate real flags, then
-- (only after verifying zero NULLs) run PHASE 2. See docs/RUNBOOK.md.
-- Replace `appsflyer_events_fb` with the DB_TABLE value if it differs.

-- PHASE 1 — add nullable so pre-deploy inserts keep working and existing rows
-- read as "not yet populated" (a DEFAULT 0 would mislabel all history as false):
ALTER TABLE `appsflyer_events_fb`
    ADD COLUMN `is_primary_attribution` TINYINT(1) NULL AFTER `attribution_type`;

-- PHASE 2 — run ONLY after the re-backfill repopulates every row and
-- `SELECT COUNT(*) ... WHERE is_primary_attribution IS NULL` returns 0:
-- ALTER TABLE `appsflyer_events_fb`
--     MODIFY `is_primary_attribution` TINYINT(1) NOT NULL;
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_loader_integration.py tests/test_loader.py -v`
Expected: PASS against a reachable DB (fresh CI `mysql:8` builds the table from the template with the column; the seeded rows insert). If skipping locally (prod not yet migrated), note it and rely on CI.

- [ ] **Step 5: Document the migration in the RUNBOOK**

In `docs/RUNBOOK.md`, add a migration subsection modeled on the existing `2026-07-08-add-id-pk-and-index.sql` entry: PHASE 1 → re-backfill → verify zero NULLs → PHASE 2, and the note that PHASE 1 must be applied to production **before** the new code is deployed there (the INSERT references the new column).

- [ ] **Step 6: Commit**

```bash
git add src/appsflyer_pipeline/loader.py sql/create_table.sql \
  sql/migrations/2026-07-10-add-is-primary-attribution.sql \
  tests/test_loader_integration.py docs/RUNBOOK.md
git commit -m "Persist is_primary_attribution column + migration (#55)"
```

---

### Task 3: Dedup view + `create-view` CLI

**Files:**
- Modify: `src/appsflyer_pipeline/loader.py` (add `_CREATE_VIEW_TEMPLATE` + `create_view()` after `create_table` ~135)
- Modify: `src/appsflyer_pipeline/cli.py` (import `create_view`; add `create-view` command after `create-table` ~77)
- Create: `sql/create_view.sql`
- Modify: `tests/test_loader.py` (add `create_view` error-wrap test)
- Modify: `tests/test_loader_integration.py` (add dedup-view behavior test)
- Modify: `tests/test_cli.py` (add `create-view` command tests)
- Modify: `docs/RUNBOOK.md` (create-view step)

**Interfaces:**
- Consumes: `_INSERT_COLUMNS` (the view selects exactly these 18 columns); `_validate_identifier`, `PipelineError`.
- Produces: `create_view(engine: Engine, table_name: str) -> None` (creates/replaces `<table_name>_deduped`, raises `PipelineError` on DB error); CLI `appsflyer-pipeline create-view`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_loader.py`, add (mirrors `test_create_table_wraps_sqlalchemy_error`; import `create_view` in the existing loader import block):

```python
def test_create_view_wraps_sqlalchemy_error() -> None:
    engine = _unreachable_engine()
    with pytest.raises(PipelineError, match="Could not create view") as excinfo:
        create_view(engine, "some_table")
    assert isinstance(excinfo.value.__cause__, SQLAlchemyError)
```

In `tests/test_loader_integration.py`, add a behavior test (creates and drops its own scratch table + view — the one integration test that writes DDL; cleaned up in `finally`):

```python
def test_create_view_dedups_pairs_keeps_singles() -> None:
    """Issue #55: <table>_deduped keeps the primary row of a cross-attribution
    pair and passes singletons through untouched. Uses a dedicated scratch table
    it creates and drops, so it never touches real data.
    """
    from appsflyer_pipeline.loader import create_view

    try:
        settings = get_settings()
        engine = create_engine(settings)
        table = "__pytest_view_test__"
        create_table(engine, table)
    except Exception as exc:
        pytest.skip(f"no usable database in this environment: {exc}")

    def _row(af_id: str, attr: str, primary: bool) -> dict[str, object]:
        return {
            "event_time": datetime.datetime(2026, 6, 15, 0, 54, 43),
            "install_time": None,
            "attributed_touch_time": None,
            "event_name": "af_purchase",
            "event_revenue": Decimal("45.00"),
            "media_source": "Facebook Ads",
            "channel": None, "campaign": None, "campaign_id": None,
            "adset": None, "adset_id": None, "ad": None, "ad_id": None,
            "appsflyer_id": af_id,
            "customer_user_id": None,
            "attribution_type": attr,
            "is_primary_attribution": primary,
            "app_id": "id1458505230",
        }

    try:
        # A dual-attribution pair (same 3-field key): false in UA, true in retarget.
        load_events(engine, table, [_row("dup-af", "non_organic", False)],
                    app_id="id1458505230", attribution_type="non_organic",
                    start_date=datetime.date(2026, 6, 15), end_date=datetime.date(2026, 6, 15))
        load_events(engine, table, [_row("dup-af", "retargeting", True)],
                    app_id="id1458505230", attribution_type="retargeting",
                    start_date=datetime.date(2026, 6, 15), end_date=datetime.date(2026, 6, 15))
        # A singleton with is_primary_attribution=false must be KEPT.
        single = _row("single-af", "non_organic", False)
        single["event_time"] = datetime.datetime(2026, 6, 16, 1, 0, 0)
        load_events(engine, table, [single],
                    app_id="id1458505230", attribution_type="non_organic",
                    start_date=datetime.date(2026, 6, 16), end_date=datetime.date(2026, 6, 16))

        create_view(engine, table)
        with engine.connect() as conn:
            rows = conn.execute(
                text(f"SELECT appsflyer_id, attribution_type FROM `{table}_deduped` ORDER BY event_time")
            ).fetchall()
        # pair collapses to its primary (retargeting) row; singleton survives.
        assert rows == [("dup-af", "retargeting"), ("single-af", "non_organic")]
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP VIEW IF EXISTS `__pytest_view_test___deduped`"))
            conn.execute(text(f"DROP TABLE IF EXISTS `__pytest_view_test__`"))
```

In `tests/test_cli.py`, add command tests mirroring the `create-table` ones (success + failure):

```python
def test_create_view_success_reports_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_cli_env(monkeypatch)
    monkeypatch.setattr(cli, "create_view", lambda engine, table_name: None)

    result = runner.invoke(app, ["create-view"])

    get_settings.cache_clear()
    assert result.exit_code == 0
    assert "some_table_deduped" in result.output  # DB_TABLE in CLI_ENV is "some_table"
    assert "is ready." in result.output


def test_create_view_reports_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_cli_env(monkeypatch)

    def _raise(engine: object, table_name: str) -> None:
        raise PipelineError("boom")

    monkeypatch.setattr(cli, "create_view", _raise)

    result = runner.invoke(app, ["create-view"])

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAILED: boom" in result.output
```

(Uses the module-level `runner = CliRunner()` and imported `app`/`cli`/`get_settings`/`PipelineError` already present in `test_cli.py`. Typer's `CliRunner` mixes stderr into `result.output`, so assert on `.output`, not `.stdout`/`.stderr`. `create_view` must be imported into `cli.py` (Task 3 Step 3) for the `monkeypatch.setattr(cli, "create_view", …)` to resolve.)

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_loader.py tests/test_cli.py -k create_view -v`
Expected: FAIL — `create_view` / the `create-view` command don't exist yet (`ImportError` on the loader test, and "No such command" / non-zero exit on the CLI tests).

- [ ] **Step 3: Implement — view template, function, CLI, reference SQL**

In `loader.py`, after `create_table` (after line 135), add (builds the column list from `_INSERT_COLUMNS`, so it stays DRY and in sync — note the doubled braces so `.format()` sees `{view}`/`{table}`):

```python
_VIEW_COLUMNS_SQL = ", ".join(f"`{c}`" for c in _INSERT_COLUMNS)

# Issue #55: a de-duplicated read over the base table. Collapses a dual-attributed
# purchase (same event_time/event_name/appsflyer_id in both the UA and retargeting
# reports) to its primary row, while passing single-attribution rows through
# untouched. Window functions require MariaDB >=10.2 / MySQL 8 (both satisfied).
_CREATE_VIEW_TEMPLATE = f"""
CREATE OR REPLACE VIEW `{{view}}` AS
SELECT {_VIEW_COLUMNS_SQL}
FROM (
    SELECT {_VIEW_COLUMNS_SQL},
           ROW_NUMBER() OVER (
               PARTITION BY `event_time`, `event_name`, `appsflyer_id`
               ORDER BY `is_primary_attribution` DESC, `attribution_type` ASC
           ) AS _dedup_rn
    FROM `{{table}}`
) ranked
WHERE _dedup_rn = 1
"""


def create_view(engine: Engine, table_name: str) -> None:
    """Create/replace the `<table_name>_deduped` view (idempotent)."""
    table_name = _validate_identifier(table_name)
    view_name = _validate_identifier(f"{table_name}_deduped")
    ddl = _CREATE_VIEW_TEMPLATE.format(view=view_name, table=table_name)
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
    except SQLAlchemyError as exc:
        raise PipelineError(f"Could not create view `{view_name}`: {exc}") from exc
```

In `cli.py`, add `create_view` to the loader import (line 19) and add the command after `create_table_command` (after line 76):

```python
@app.command(name="create-view")
def create_view_command() -> None:
    """Create/replace the de-duplicated `<table>_deduped` view (idempotent)."""
    settings = _get_settings_or_exit()
    engine = create_engine(settings)
    try:
        create_view(engine, settings.db_table)
    except PipelineError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"View `{settings.db_table}_deduped` is ready.")
```

Create `sql/create_view.sql` (reference/manual copy, literal table name like `sql/create_table.sql`):

```sql
-- BAF-2 issue #55: de-duplicated read over appsflyer_events_fb. Keeps the
-- primary row of a cross-attribution purchase pair, passes singletons through.
-- `appsflyer-pipeline create-view` creates this programmatically (idempotent)
-- using the DB_TABLE-configured name; keep the two in sync.
CREATE OR REPLACE VIEW `appsflyer_events_fb_deduped` AS
SELECT `event_time`, `install_time`, `attributed_touch_time`, `event_name`,
       `event_revenue`, `media_source`, `channel`, `campaign`, `campaign_id`,
       `adset`, `adset_id`, `ad`, `ad_id`, `appsflyer_id`, `customer_user_id`,
       `attribution_type`, `is_primary_attribution`, `app_id`
FROM (
    SELECT `event_time`, `install_time`, `attributed_touch_time`, `event_name`,
           `event_revenue`, `media_source`, `channel`, `campaign`, `campaign_id`,
           `adset`, `adset_id`, `ad`, `ad_id`, `appsflyer_id`, `customer_user_id`,
           `attribution_type`, `is_primary_attribution`, `app_id`,
           ROW_NUMBER() OVER (
               PARTITION BY `event_time`, `event_name`, `appsflyer_id`
               ORDER BY `is_primary_attribution` DESC, `attribution_type` ASC
           ) AS _dedup_rn
    FROM `appsflyer_events_fb`
) ranked
WHERE _dedup_rn = 1;
```

- [ ] **Step 4: Run to verify they pass**

Run: `uv run pytest tests/test_loader.py tests/test_cli.py tests/test_loader_integration.py -v`
Expected: PASS (the integration dedup test runs where a DB is reachable, else skips).

- [ ] **Step 5: Document create-view in the RUNBOOK**

In `docs/RUNBOOK.md`, add `uv run appsflyer-pipeline create-view` as the step run after `create-table` (fresh install) and after the migration + re-backfill (existing install), noting the view is only meaningful once flags are populated.

- [ ] **Step 6: Commit**

```bash
git add src/appsflyer_pipeline/loader.py src/appsflyer_pipeline/cli.py \
  sql/create_view.sql tests/test_loader.py tests/test_loader_integration.py \
  tests/test_cli.py docs/RUNBOOK.md
git commit -m "Add <table>_deduped view + create-view command (#55)"
```

---

### Task 4: Docs, fixture sweep, and full-suite verification

**Files:**
- Modify: `docs/design-spec.md` (dual-attribution risk row ~134)
- Modify: `docs/RUNBOOK.md` (ops sequence + re-backfill timing)
- Verify: `tests/test_pipeline.py`, `tests/test_cli.py` `SAMPLE_CSV` fixtures

- [ ] **Step 1: Sweep the remaining CSV fixtures**

Confirm every `SAMPLE_CSV`/inline-CSV fixture that reaches `transform_events` has an `Is Primary Attribution` header **and** a `true`/`false` value in each data row (otherwise `_parse_bool` now raises).

Run: `uv run pytest tests/test_pipeline.py tests/test_cli.py -v`
If any fail with `Unexpected is_primary_attribution value`, add/fix the column's value in that fixture's data rows (header already present per grep). Re-run until green.

- [ ] **Step 2: Update design-spec attribution model**

In `docs/design-spec.md`, extend the dual-attribution risk row (line ~134) to note: as of #55 the `Is Primary Attribution` flag is **persisted** as `is_primary_attribution`, and the `<table>_deduped` view gives the de-duplicated read (keep the primary row of each pair, keep singletons) — the ~1.2% cross-attribution revenue overlap is resolved at query time without dropping non-FB-primary singles.

- [ ] **Step 3: Document the ops sequence + re-backfill timing**

In `docs/RUNBOOK.md`, add the ordered ops runbook: (1) migration PHASE 1, (2) deploy code, (3) `create-view`, (4) **re-backfill `06-05 → 07-09` the same day** — with the ⚠️ note that the oldest DB day sits at the ~35-day availability floor (#45): if it slides past `06-05`, a re-fetch returns empty and delete-then-insert would wipe it, (5) verify zero NULL flags + the 43 pairs each collapse to one view row, (6) migration PHASE 2 (`MODIFY … NOT NULL`).

- [ ] **Step 4: Full merge-gate verification**

Run: `uv run pre-commit run --all-files`
Expected: PASS (ruff, ruff-format, mypy strict).

Run: `uv run pytest --cov=appsflyer_pipeline --cov-branch --cov-report=term-missing --cov-fail-under=98`
Expected: PASS, coverage ≥98%.

- [ ] **Step 5: Commit**

```bash
git add docs/design-spec.md docs/RUNBOOK.md tests/test_pipeline.py tests/test_cli.py
git commit -m "Docs + fixtures for is_primary_attribution + dedup view (#55)"
```

---

## Delivery & post-merge ops (controller-only, each gated on explicit go-ahead)

1. **File issue #55** under `baubek-yesim` (verify active account first — it drifts to `unreal-kz`); set the board item In Progress.
2. **Review + merge** on explicit user authorization; the merge/PR body carries `Fixes #55` (single issue).
3. **Production sequence** (all controller-direct, each its own go-ahead), in this order so nothing breaks mid-deploy:
   - migration **PHASE 1** on prod (add nullable column);
   - `git pull` + `uv sync --frozen --no-dev` on the server;
   - `create-view`;
   - **re-backfill `06-05 → 07-09` the same day** (idempotent; repopulates the 3,467 rows) — before `06-05` slides past the floor;
   - **verify:** `<table>` = 3,467, `<table>_deduped` = 3,424, zero NULL flags, Mark's example ID `1773683081087-6632471` → one (retargeting) row in the view;
   - migration **PHASE 2** (`MODIFY … NOT NULL`).
4. **Board** #55 → Done on close. Tell Mark the dedup view is live and how to query it.

## Self-review notes

- **Spec coverage:** column (Task 2) ✓, transform populate (Task 1) ✓, `_parse_bool` fail-loud (Task 1) ✓, `_REQUIRED_NOT_NULL` falsy-trap avoided + regressioned (Task 1) ✓, two-phase migration (Task 2) ✓, dedup view + create-view (Task 3) ✓, re-backfill timing/#45 (Task 4 + Delivery) ✓, docs (Tasks 2/3/4) ✓, post-backfill verification (Delivery) ✓.
- **Behavior reversal called out:** `test_transform_never_requires_the_flag_column` → `test_transform_now_requires_the_flag_column` (the flag is required again, but stored not filtered).
- **Type consistency:** `create_view(engine, table_name)` signature identical across loader definition, CLI call, and both tests; view name `f"{table}_deduped"` used identically in `create_view`, the CLI success message, and the integration test's `DROP`.
