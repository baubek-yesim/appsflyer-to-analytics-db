# Design: add an `is_primary_attribution` column + a canonical dedup view

**Date:** 2026-07-10
**Issue:** #55 (to be filed under `baubek-yesim`) — Mark Malovichko reported that dedup "isn't
working" for the key `event_time + event_name + appsflyer_id`, example `appsflyer_id =
'1773683081087-6632471'`.
**Status:** approved in brainstorming, ready for implementation planning

## Summary of the investigation (why this is not a dedup bug)

Verified read-only against production 2026-07-10:

- Mark's example ID has **2 rows** that are identical on his 3-field key *and* on `event_revenue`
  (45.00) and `app_id` (`id1458505230`), differing in **exactly one field: `attribution_type`**
  (`non_organic` vs `retargeting`, each with its own campaign / install / touch story). The same
  Facebook purchase was attributed in both AppsFlyer report types.
- Table-wide (3,467 rows): **0** genuine within-`attribution_type` duplicates (a real dedup bug
  would surface here); **43** groups collide on the 3-field key, **all** exactly
  `{non_organic, retargeting}`, **all** on `id1458505230`, **all equal-revenue**, spanning
  `2026-06-05 → 2026-07-09`. Naive `SUM(event_revenue)` over-counts by **575.34 of 46,909.23
  ≈ 1.23%**.

`transform._dedupe_rows` keys on `(event_time, event_name, appsflyer_id)` but only ever sees **one
`attribution_type` per call** (transform runs per `(app_id, attribution_type, window)`), so it is
covariant with Mark's 4-column key (BAF-2 c62585) and **structurally cannot** collapse a
non_organic↔retargeting pair. Keeping both rows is the **deliberate `#47` outcome** (the
data-analytics decision on `#46` that reversed `#7`'s `Is Primary Attribution` filter):
attribution is a dimension and cross-attribution sums count a dual-attributed purchase in both
dimensions by design (documented in `transform.transform_events`'s docstring, lines 123-129).

So Mark is re-encountering the `#46`/`#47` trade-off. This design implements `#46`'s recommended
**Option C**: keep every row, but surface AppsFlyer's `Is Primary Attribution` flag as a column so
the 43 pairs can be collapsed at query time **without** dropping the non-FB-primary single
purchases that `#46`/`#47` deliberately restored.

## Goals

1. Add a persisted `is_primary_attribution BOOLEAN NOT NULL` column populated from the raw
   AppsFlyer export.
2. Preserve `#47`'s "load every row" model unchanged — no row is dropped at load time; attribution
   stays a dimension.
3. Ship a **canonical dedup view** implementing Mark's chosen semantics — *dedup only the pairs,
   keep the singles*: per `(event_time, event_name, appsflyer_id)`, if a purchase appears in both
   reports keep the `is_primary_attribution = true` row and drop its twin; purchases appearing once
   stay regardless of flag.

## Non-goals

- **Not** reverting `#47` / reinstating the load-time `Is Primary Attribution` filter (`#7`). Every
  row still loads; the view is a read-time projection, not a delete.
- **Not** changing `_dedupe_rows`' key or its exact-duplicate-collapse / conflict-raise semantics.
- **Not** restoring the `#14` `id`/PRIMARY KEY/index that the 2026-07-10 table recreation dropped —
  separate, Mark's schema call. The dedup view is intentionally designed to need **no** surrogate
  key (deterministic tiebreak on existing columns).
- **Not** the pre-`06-05` history import from Mark's Google Sheet extract — separate item, blocked
  on Mark's A/B/C + import decision.

## Context / evidence the design relies on

- `Is Primary Attribution` is a **standard** column (position 51 of 81) in the raw export we
  already fetch — confirmed against Mark's reference file
  `reference/com_yesimmobile_in_app_events_2026_07_09_2026_07_09_Europe_Riga.csv` (15 `true` /
  3 `false`). Values are lowercase strings `true` / `false`. Requesting it via
  `additional_fields=is_primary_attribution` returns **HTTP 400** (`#7`, live-verified — do not
  re-suggest), so the design reads the **standard column** already present.
- The production table currently holds exactly the API's retained window: `06-05 → 07-09`. Today's
  effective availability floor **is** `06-05` (`#45`: ~35 days, slides one day forward daily). This
  drives the re-backfill timing constraint below.
- The table was recreated 2026-07-10 with `DATETIME` time columns and **without** the `#14`
  `id`/PK/index; `SELECT id …` errors. The view must not assume an `id` column exists.

## Design

### Part 1 — Schema (`sql/create_table.sql`, `loader._CREATE_TABLE_TEMPLATE`)

Add, immediately after `attribution_type`:

```sql
`is_primary_attribution` TINYINT(1) NOT NULL,   -- AppsFlyer "Is Primary Attribution" (true/false)
```

`TINYINT(1)` is MariaDB/MySQL `BOOLEAN`. `NOT NULL` is the final state (see the migration's two
phases). Keep `sql/create_table.sql` and the loader template column-for-column in sync (existing
invariant).

### Part 2 — Migration (`sql/migrations/2026-07-10-add-is-primary-attribution.sql`)

Two phases so nothing breaks mid-deploy and existing rows are never silently mislabeled:

1. **Add nullable** — `ALTER TABLE <table> ADD COLUMN is_primary_attribution TINYINT(1) NULL AFTER
   attribution_type;`
   Old (pre-deploy) inserts omit the column → `NULL`; the 3,467 existing rows become `NULL`, which
   reads unambiguously as *not yet populated* (a `DEFAULT 0` would mislabel them all as `false`).
2. **Enforce NOT NULL** — run **only after** the re-backfill repopulates every row and the verify
   step confirms zero `NULL`s: `ALTER TABLE <table> MODIFY is_primary_attribution TINYINT(1) NOT
   NULL;`

Both statements live in the one migration file, clearly separated, phase 2 marked "run after
re-backfill + verify".

### Part 3 — Transform (`src/appsflyer_pipeline/transform.py`)

- Add `"Is Primary Attribution": "is_primary_attribution"` to `_COLUMN_MAP`.
- New `_parse_bool(value: str | None) -> bool`: `"true" → True`, `"false" → False`, anything else
  (including blank / unexpected casing) → `TransformError`, matching the fail-loud convention of
  `_parse_timestamp`/`_parse_revenue` and `#26`/`#29`. Applied in the row-build loop.
- **Correctness trap to avoid:** do **not** add `is_primary_attribution` to `_REQUIRED_NOT_NULL`.
  That guard is `if not row[required]`, which is truthiness-based and would treat a legitimate
  `False` as "missing" and raise on every non-primary row. `_parse_bool` is the validator; a
  dedicated regression test pins that an `is_primary=false` row survives.
- `_dedupe_rows` needs **no** logic change: the new field simply joins the existing exact-equality
  comparison. Interaction to note: two rows in *one* report sharing the 3-field key but disagreeing
  on `is_primary_attribution` would raise `TransformError` (correct fail-loud) — not expected to
  occur, since one report attributes an event once.

### Part 4 — Canonical dedup view (`sql/create_view.sql`, `loader._CREATE_VIEW_TEMPLATE`, new `create-view` CLI command)

Mirror the `create-table` pattern (idempotent DDL, table name from `DB_TABLE`, so the view name is
`<DB_TABLE>_deduped`):

```sql
CREATE OR REPLACE VIEW `<table>_deduped` AS
SELECT <persisted columns, explicitly listed>   -- the loader's INSERT set (17 today + is_primary_attribution = 18); excludes the surrogate `id` (if present) and the `_dedup_rn` helper
FROM (
    SELECT t.*,
           ROW_NUMBER() OVER (
               PARTITION BY event_time, event_name, appsflyer_id
               ORDER BY is_primary_attribution DESC, attribution_type ASC
           ) AS _dedup_rn
    FROM `<table>` t
) ranked
WHERE _dedup_rn = 1;
```

- **Partition** on Mark's exact 3-field key. `appsflyer_id` functionally determines `app_id` (an
  AppsFlyer ID is per-install-per-app), so `app_id` is intentionally omitted from the partition to
  match his key exactly; confirmed in data (all 43 pairs share one `app_id`).
- **Order** `is_primary_attribution DESC` puts `true` (1) before `false` (0), so a pair keeps its
  primary row; `attribution_type ASC` is a deterministic tiebreak for the (not-expected)
  both-same-flag case — no surrogate key required.
- **Singletons** have `_dedup_rn = 1` and pass through untouched regardless of flag — this is the
  "keep singles" half of Mark's semantics.
- Window functions require MariaDB ≥10.2 / MySQL 8; both the analytics MariaDB and the CI
  `mysql:8` container satisfy this.
- A new `appsflyer-pipeline create-view` command (idempotent, `CREATE OR REPLACE VIEW`) deploys it
  the same way as `create-table`.

### Part 5 — Ops sequence (migrate → deploy → re-backfill → verify → lock), each gated on explicit go-ahead

1. Run migration **phase 1** (add nullable column) on production.
2. Deploy code to the server (`git pull` + `uv sync --frozen --no-dev`).
3. `create-view`.
4. **Re-backfill `06-05 → 07-09` promptly (same day).** Idempotent delete-then-insert repopulates
   all 3,467 rows with real flags. ⚠️ **Timing:** the oldest DB day (`06-05`) is at today's
   availability floor. If this slips a day, `06-05` slides beyond the floor, a re-fetch returns
   empty, and delete-then-insert would **wipe** those rows (the `#45` hazard). Do it while `06-05`
   is still fetchable.
5. **Verify:** zero `NULL` `is_primary_attribution`; the 43 pairs each collapse to exactly one row
   in `<table>_deduped`; report any both-`true`/both-`false` pair (the view still returns one row
   deterministically, but it's worth knowing).
6. Run migration **phase 2** (`MODIFY … NOT NULL`).

Production actions (ALTER, deploy, create-view, re-backfill) are performed by the controller
directly — never delegated to a subagent — each on its own explicit user go-ahead, per the
`#7`/`#14`/`#26` mixed-automation pattern.

### Ops/docs

- `docs/design-spec.md`: update the attribution-model / dedup risk section to document the flag
  column, the double-count it resolves, and the `<table>_deduped` view as the canonical
  de-duplicated read.
- `docs/RUNBOOK.md`: a migration section for `2026-07-10-add-is-primary-attribution.sql` (both
  phases), a `create-view` step, and the re-backfill-timing caution.
- `.env.example` / README: no new config (nothing to add).

## Tests

- `tests/test_transform.py`:
  - `_parse_bool`: `true`/`false`/mixed-case/blank/garbage (parametrized) — first two map to
    `True`/`False`, the rest raise `TransformError`.
  - Row-build: `is_primary_attribution` present and correctly typed in the output dict.
  - **Regression for the falsy trap:** a fully-valid row with `Is Primary Attribution = false`
    transforms successfully (is *not* treated as a missing required field).
  - Dedup still collapses exact dups and still conflict-raises with the new field included.
- Fixture/CSV-header updates across `tests/test_transform.py`, `tests/test_cli.py`,
  `tests/test_pipeline.py` to add the `Is Primary Attribution` column.
- `tests/test_loader_integration.py` (skip-gracefully live-DB pattern): the `create-table` template
  includes the column; a new `create-view` test confirms `<table>_deduped` is created and returns
  one row per 3-field group (seed a synthetic non_organic+retargeting pair + a singleton).
- Coverage: all new branches exercised; CI `--cov-fail-under=98` unaffected.

## Delivery

- Branch `issue-55-is-primary-attribution-column` (already created); this spec and its plan ride
  with it.
- File issue #55 under `baubek-yesim`; set the board item Todo → In Progress when the branch starts,
  Done on close (board setup itself still pending the `project` token scope).
- Merge only on the user's explicit authorization. The merge/PR body carries `Fixes #55`
  (single issue — the one-keyword-per-issue rule is moot).
- Deploy + migrate + re-backfill after merge, gated on their own explicit go-ahead (production
  actions stay with the controller).
- Live verification: after the re-backfill, `SELECT COUNT(*)` on `<table>` (unchanged 3,467) vs
  `<table>_deduped` (3,467 − 43 = **3,424**), zero `NULL` flags, and Mark's example ID resolves to
  one row in the view.
