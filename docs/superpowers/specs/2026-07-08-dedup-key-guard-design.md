# Design: duplicate-row guard on (event_time, event_name, appsflyer_id, attribution_type)

**Date:** 2026-07-08
**Issue:** [#23](https://github.com/baubek-yesim/appsflyer-to-analytics-db/issues/23) — stems from
Mark Malovichko's dedup-key comment on BAF-2 (comment 62585), surfaced while reviewing his
original reference scripts (`reference/mark-scripts/`, gitignored, local-only).
**Status:** approved in brainstorming, issue filed, ready for implementation planning

## Goals

1. Close the gap between Mark's stated dedup key and what the pipeline actually implements: no
   mechanism today catches a true duplicate row returned within a **single** AppsFlyer report
   response — as opposed to the cross-report duplication issue #7 already fixed, or the
   window-replace idempotency `load_events` already has.
2. Quantify whether this gap has already produced duplicate rows in production
   `appsflyer_events_fb`, and purge them if so — same audit → fix → purge → reconfirm pattern as
   issue #7.

## Non-goals

- A DB-level unique constraint/index on this key. Considered and explicitly rejected for this
  issue — stays application-level only (matches #7's scope). A schema-level backstop is issue
  #14's territory (missing PRIMARY KEY/indexes), not duplicated here.
- Cross-chunk boundary duplicates (the same `event_time` straddling two adjacent, non-overlapping
  `chunk_date_range` windows). Chunks are disjoint date ranges and AppsFlyer filters by
  `from`/`to` on `event_time`, so this is a much lower-probability edge case than intra-response
  duplication; not addressed here.
- Issues #8, #11–#17 (tracked separately).

## Context

Mark (BAF-2 comment 62585), responding to the #7 dual-attribution fix, said dedup should be keyed
on `event_time, event_name, appsflyer_id, attribution_type` — explicitly not `event_revenue` (a
measure, not an identity).

Today's two dedup-adjacent mechanisms don't implement that key:

- `load_events` (`loader.py:152-224`): delete-by-window-then-insert, keyed on `app_id` +
  `attribution_type` + `event_time` range. A **partition** key for idempotent re-runs, not a
  row-uniqueness check.
- `transform_events`'s `Is Primary Attribution` filter (`transform.py:105-117`, issue #7): drops
  cross-report duplicates (a retargeting-attributed event's secondary copy in the UA report). A
  different failure mode — same-report duplication is untouched.

`docs/design-spec.md:116` already notes *"AppsFlyer data has no reliable natural unique key
(duplicate/near-duplicate events are possible)"* as the rationale for delete-then-insert, but no
code turns that acknowledgment into an actual check.

## Design

### Part 1 — Dedup in `transform_events` (`src/appsflyer_pipeline/transform.py`)

Placement: after the existing `Is Primary Attribution` filter, before the final row-dict
conversion loop. Operates on the already-typed `rows: list[dict]` (post-parse — comparing parsed
`Decimal`/`datetime` values avoids false conflicts from raw-string formatting differences, e.g.
`"10.00"` vs `"10.0"`).

Effective key: `(event_time, event_name, appsflyer_id)`. `attribution_type` and `app_id` are
constant within a single `transform_events` call (they're function parameters), so they're
already covariant with Mark's full 4-column key — no need to carry them in an explicit tuple.

Algorithm — single pass building a `dict[key, row]`:

- New key → keep.
- Key seen, new row `==` stored row (full dict equality, covers every column at once) → exact
  duplicate: drop it, increment a counter.
- Key seen, rows differ → raise `TransformError` naming the key and both conflicting rows (same
  style/context as the existing schema-drift and primary-attribution errors — includes
  `attribution_type=`/`app_id=`).

If any exact duplicates were collapsed, `logger.warning` once with the count, `app_id`,
`attribution_type` (visibility principle, same as issue #10's wipe-visibility logging). Adds
`import logging` + a module logger to `transform.py` (not currently present), matching the
existing pattern in `loader.py`/`pipeline.py`.

### Part 2 — Production audit (read-only script, not part of `src/`)

Same shape as issue #7's `probe_issue7.py` precedent: connects via the existing
`create_engine`/`get_settings()`, read-only, run manually — not pytest-covered, not subject to
CI's coverage gate.

```sql
SELECT event_time, event_name, appsflyer_id, attribution_type,
       COUNT(*) AS n_rows
FROM appsflyer_events_fb
GROUP BY event_time, event_name, appsflyer_id, attribution_type
HAVING n_rows > 1;
```

Reports dup-group count, extra-row count, and revenue impact, and splits groups into:

- **exact** (all non-key columns agree) — will silently collapse on next reload under Part 1's fix.
- **conflicting** (some column disagrees, e.g. different revenue) — will raise on next reload;
  needs manual attention before that window is re-run.

Sequencing (mirrors #7's fix → deploy → purge → reconfirm):

1. Run the audit now, before the code change — baseline.
2. Ship Part 1.
3. Re-run `backfill`, scoped only to the `(app_id, attribution_type)` combos/windows the audit
   flagged — not a blanket re-backfill (avoids repeating #7's daily-quota exhaustion from a full
   re-backfill).
4. Re-run the audit post-purge to confirm 0 remaining duplicate groups; note any
   conflicting-duplicate windows that needed manual resolution.

## Tests (`tests/test_transform.py`, existing `_raw_row()`/`_df()` pattern)

- `test_transform_collapses_exact_duplicate_rows`: two raw rows identical on the key and every
  other field → 1 row returned; a `WARNING` is logged (asserted via
  `caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.transform")`) with the collapsed
  count, `app_id`, `attribution_type`.
- `test_transform_raises_on_conflicting_duplicate_rows`: two raw rows sharing the key but with a
  different `Event Revenue` → `pytest.raises(TransformError, match=...)`, message includes the key.
- No pipeline.py/loader.py-level test: `transform_events` is a pure function and `_process_window`
  adds no logic around this call, so a pipeline-level test would just re-exercise the same
  branches through an extra layer.
- CI gate is `--cov-fail-under=98` branch coverage — the three code paths (new key / exact match /
  conflict) are fully covered by these two new tests plus the many existing single-row tests that
  already exercise "no duplicate encountered."

## GitHub issue text (filed as #23)

**Title:** No dedup guard on `(event_time, event_name, appsflyer_id, attribution_type)` — a
same-report duplicate row would load twice, uncaught

**Labels:** `enhancement` (→ `bug` if the audit finds this already happened in prod)

> ## Problem
>
> Two dedup mechanisms exist today, and neither catches a true duplicate row within a single
> AppsFlyer report response:
> - `load_events`'s delete-by-window-then-insert (`loader.py:152-224`) is a partition key
>   (`app_id`, `attribution_type`, `event_time` range), not a row-uniqueness check — it makes
>   *re-running* a window safe, but does nothing about duplicates *within* one fetch.
> - The `Is Primary Attribution` filter (`transform.py:105-117`, issue #7) removes cross-report
>   duplicates (a retargeting-attributed event appearing a second time in the UA report) — a
>   different failure mode from AppsFlyer returning the same row twice within one report call.
>
> Mark (BAF-2 comment 62585) flagged this directly: dedup should be keyed on `event_time,
> event_name, appsflyer_id, attribution_type` — not `event_revenue` (a measure, not an identity).
> `docs/design-spec.md:116` already acknowledges "AppsFlyer data has no reliable natural unique
> key (duplicate/near-duplicate events are possible)" as the rationale for delete-then-insert, but
> that acknowledgment never became an actual duplicate-row check.
>
> ## Impact
>
> If AppsFlyer's export ever returns the same event twice within one report response, `load_events`
> bulk-inserts both copies — silently double-counting that purchase's revenue, indistinguishable
> from a real second purchase to any downstream consumer. Unknown today whether this has already
> happened in production; quantifying that is part of this issue (see Verification).
>
> ## Suggested fix
>
> In `transform_events` (`transform.py`), after the existing primary-attribution filter and before
> the final row-dict conversion, dedup the batch on `(event_time, event_name, appsflyer_id)` —
> `attribution_type`/`app_id` are already constant within one call, so they're covariant with
> Mark's full key:
> - Exact duplicate (all fields match) → collapse to one row, log a `WARNING` with the count
>   (visibility, same principle as issue #10).
> - Same key, differing fields (e.g. different revenue) → raise `TransformError` naming the key
>   and both rows.
>
> ## Verification
>
> Read-only audit against `appsflyer_events_fb` (same shape as #7's investigation):
> ```sql
> SELECT event_time, event_name, appsflyer_id, attribution_type, COUNT(*) AS n
> FROM appsflyer_events_fb
> GROUP BY event_time, event_name, appsflyer_id, attribution_type
> HAVING n > 1;
> ```
> Run before the fix (baseline count/revenue impact, and whether any groups conflict on non-key
> fields), then again after deploying the fix and re-running `backfill` scoped only to the
> affected `(app_id, attribution_type)` windows (avoids repeating #7's daily-quota exhaustion from
> a blanket re-backfill), to confirm 0 remaining duplicate groups.

## Delivery

- One branch/PR (per explicit choice: fix + audit together, matching #7's scope):
  `issue-23-dedup-key-guard`.
- This spec rides with that branch.
- Merge only on the user's explicit authorization, per established workflow.
