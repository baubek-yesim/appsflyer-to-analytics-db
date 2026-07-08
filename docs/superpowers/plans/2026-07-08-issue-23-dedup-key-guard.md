# Issue #23 — Dedup Key Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the gap between Mark Malovichko's stated dedup key (`event_time`, `event_name`, `appsflyer_id`, `attribution_type` — BAF-2 comment 62585) and the pipeline's actual behavior: no mechanism today catches a true duplicate row returned within a single AppsFlyer report response. Add that guard, and quantify whether it has already produced duplicate rows in production.

**Architecture:** One small addition to `transform_events` (`src/appsflyer_pipeline/transform.py`): after the existing `Is Primary Attribution` filter, collapse exact duplicate rows sharing `(event_time, event_name, appsflyer_id)` — `attribution_type`/`app_id` are already constant per call, so this is Mark's full key — and raise if two rows share the key but disagree on any other field. Paired with a read-only, gitignored audit script (same shape as issue #7's `probe_issue7.py` precedent) to baseline production before the fix ships.

**Tech Stack:** Python 3.12, uv, polars, SQLAlchemy 2.0 + PyMySQL, pytest, ruff, mypy --strict.

**Spec:** `docs/superpowers/specs/2026-07-08-dedup-key-guard-design.md`

## Global Constraints

- Run everything through uv: `uv run pytest`, `uv run pre-commit run --all-files` (ruff + ruff-format + mypy strict — same hooks CI gates on).
- CI also gates `pytest --cov-fail-under=98` with **branch** coverage — every new branch needs a covering test.
- Repo is **public**: no secrets, hostnames, or account names in code, docs, tests, or commit messages.
- One branch/PR: `issue-23-dedup-key-guard`, cut from `main` (fix + audit together, per explicit scope decision — matches issue #7's shape).
- **Never merge a PR** — merges happen only on the user's explicit authorization, outside this plan.
- The audit script (`scratch/probe_issue23.py`) is **local-only, gitignored** — same precedent as issue #7's `probe_issue7.py`, which was never committed. Only the `.gitignore` entry itself is committed.
- Commit messages: imperative summary line referencing the issue, e.g. `Add dedup-key guard for same-report duplicate rows (#23)`; end with the `Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>` trailer.

---

### Task 1: Cut the branch

**Files:** none (git only)

**Interfaces:** none.

- [x] **Step 1: Cut the branch from main**

Done via an isolated worktree at `.worktrees/issue-23-dedup-key-guard` (branch
`issue-23-dedup-key-guard`, created from local `main` HEAD `8e6cbdf`). All
subsequent tasks execute inside that worktree, not the primary checkout.

- [ ] **Step 2: Commit this plan onto the branch**

```bash
git add docs/superpowers/plans/2026-07-08-issue-23-dedup-key-guard.md
git commit -m "$(cat <<'EOF'
Add implementation plan for issue #23 dedup-key guard

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Dedup-key guard in `transform_events`

**Files:**
- Modify: `src/appsflyer_pipeline/transform.py` (imports, module logger, new `_dedupe_rows` helper, `transform_events`'s final `return`)
- Test: `tests/test_transform.py`

**Interfaces:**
- Consumes: `TransformError` (already defined in `transform.py`); `AttributionType` (already imported).
- Produces: `transform_events(...)` — same signature and return type (`list[dict[str, Any]]`) as today. New private helper `_dedupe_rows(rows: list[dict[str, Any]], *, attribution_type: AttributionType, app_id: str) -> list[dict[str, Any]]`, called only from `transform_events`.

- [ ] **Step 1: Write the failing tests**

Add `import logging` to the top of `tests/test_transform.py` (after `import datetime`):

```python
from __future__ import annotations

import datetime
import logging
from decimal import Decimal

import polars as pl
import pytest

from appsflyer_pipeline.transform import TransformError, transform_events
```

Append to the end of `tests/test_transform.py`:

```python
def test_transform_collapses_exact_duplicate_rows(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """AppsFlyer returning the identical row twice within one report response
    (not issue #7's cross-report case) is collapsed to one row, with a WARNING
    for visibility — same principle as issue #10's wipe-visibility logging.
    """
    df = _df([_raw_row(), _raw_row()])
    with caplog.at_level(logging.WARNING, logger="appsflyer_pipeline.transform"):
        rows = transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase", "af_purchase_YC"],
        )
    assert len(rows) == 1
    assert any(
        "collapsed 1 exact-duplicate" in r.message and "id1458505230" in r.message
        for r in caplog.records
    )


def test_transform_raises_on_conflicting_duplicate_rows() -> None:
    """Same key (event_time, event_name, appsflyer_id) but different
    event_revenue is not a safe-to-collapse duplicate — the dedup key's
    uniqueness assumption doesn't hold for this data, so it must fail loudly
    rather than silently pick one value (Mark's key explicitly excludes
    event_revenue, BAF-2 comment 62585).
    """
    df = _df(
        [
            _raw_row(**{"Event Revenue": "9.99"}),
            _raw_row(**{"Event Revenue": "19.99"}),
        ]
    )
    with pytest.raises(TransformError, match="Conflicting duplicate"):
        transform_events(
            df,
            attribution_type="non_organic",
            app_id="id1458505230",
            media_source_filter="Facebook Ads",
            event_names_filter=["af_purchase", "af_purchase_YC"],
        )
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_transform.py -v -k "duplicate"`
Expected: `test_transform_collapses_exact_duplicate_rows` FAILS on `assert len(rows) == 1` (currently returns 2 rows, no dedup exists); `test_transform_raises_on_conflicting_duplicate_rows` FAILS with `Failed: DID NOT RAISE <class 'appsflyer_pipeline.transform.TransformError'>`.

- [ ] **Step 3: Implement the guard**

In `src/appsflyer_pipeline/transform.py`, change the import block:

```python
from __future__ import annotations

import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import polars as pl

from appsflyer_pipeline.appsflyer_client import AttributionType

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
```

to:

```python
from __future__ import annotations

import datetime
import logging
from decimal import Decimal, InvalidOperation
from typing import Any

import polars as pl

from appsflyer_pipeline.appsflyer_client import AttributionType

logger = logging.getLogger(__name__)

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
```

Then insert a new `_dedupe_rows` helper between `_parse_revenue` and `transform_events` — change:

```python
def _parse_revenue(value: str | None) -> Decimal | None:
    if value is None or value.strip() == "":
        return None
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise TransformError(f"Unexpected event_revenue value: {value!r}") from exc


def transform_events(
```

to:

```python
def _parse_revenue(value: str | None) -> Decimal | None:
    if value is None or value.strip() == "":
        return None
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise TransformError(f"Unexpected event_revenue value: {value!r}") from exc


def _dedupe_rows(
    rows: list[dict[str, Any]], *, attribution_type: AttributionType, app_id: str
) -> list[dict[str, Any]]:
    """Collapse exact duplicate rows sharing (event_time, event_name, appsflyer_id).

    `attribution_type`/`app_id` are constant across one transform_events call, so
    this 3-column key is covariant with Mark's full 4-column dedup key (BAF-2
    comment 62585) — neither column can differ within a single call. Rows
    sharing the key but disagreeing on any other field raise: that means the
    key's uniqueness assumption doesn't hold for this data and needs a human,
    not a silent pick.
    """
    seen: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    duplicate_count = 0
    for row in rows:
        key = (row["event_time"], row["event_name"], row["appsflyer_id"])
        existing = seen.get(key)
        if existing is None:
            seen[key] = row
        elif existing == row:
            duplicate_count += 1
        else:
            raise TransformError(
                f"Conflicting duplicate rows for key {key!r} "
                f"(attribution_type={attribution_type}, app_id={app_id}): {existing} vs {row}"
            )
    if duplicate_count:
        logger.warning(
            "collapsed %d exact-duplicate row(s): attribution_type=%s app_id=%s",
            duplicate_count,
            attribution_type,
            app_id,
        )
    return list(seen.values())


def transform_events(
```

Finally, change the end of `transform_events`:

```python
        rows.append(row)

    return rows
```

to:

```python
        rows.append(row)

    return _dedupe_rows(rows, attribution_type=attribution_type, app_id=app_id)
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `uv run pytest tests/test_transform.py -v`
Expected: all PASS (the 2 new + all existing — single-row and non-duplicate multi-row cases are unaffected, since every key in those tests is seen only once).

- [ ] **Step 5: Run the full local gates**

Run: `uv run pre-commit run --all-files && uv run pytest`
Expected: all hooks pass; full suite passes (integration tests may SKIP without a reachable DB — that's fine).

- [ ] **Step 6: Commit**

```bash
git add src/appsflyer_pipeline/transform.py tests/test_transform.py
git commit -m "$(cat <<'EOF'
Add dedup-key guard for same-report duplicate rows (#23)

transform_events now collapses exact duplicate rows sharing
(event_time, event_name, appsflyer_id) - attribution_type/app_id are
already constant per call, so this is Mark's full 4-column dedup key
(BAF-2 comment 62585). A collapsed duplicate logs a WARNING; rows
sharing the key but disagreeing on another field (e.g. event_revenue)
raise TransformError instead of silently picking one. This is distinct
from issue #7's cross-report Is Primary Attribution filter - it guards
against AppsFlyer returning the same row twice within one report call.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Production audit — pre-fix baseline

**Files:**
- Modify: `.gitignore` (add `/scratch/`)
- Create: `scratch/probe_issue23.py` (gitignored, local-only — never committed, same precedent as issue #7's `probe_issue7.py`)

**Interfaces:**
- Consumes: `appsflyer_pipeline.config.get_settings`, `appsflyer_pipeline.loader.create_engine` (existing, read-only use).
- Produces: printed audit report only. No other task depends on this script's code.

- [ ] **Step 1: Add `/scratch/` to `.gitignore`**

In `.gitignore`, after the existing `/reference/` block:

```
# Mark Malovichko's original reference scripts (BAF-2 comment 62293) — local-only,
# kept for review/reference, never committed (this repo is public).
/reference/

# One-off read-only production audit/probe scripts (e.g. issue #7's/#23's
# duplicate-row investigations) — local-only, never committed (this repo is
# public and these connect to production credentials via .env).
/scratch/
```

- [ ] **Step 2: Create the audit script**

Create `scratch/probe_issue23.py`:

```python
"""Issue #23 probe — READ-ONLY. Quantifies same-report duplicate rows
(distinct from issue #7's cross-report dual-attribution duplicates).

Dedup key: (event_time, event_name, appsflyer_id, attribution_type) — Mark's
key from BAF-2 comment 62585. "Exact" groups below are approximated by also
grouping on event_revenue (the field most likely to disagree, per the design
doc); the code-level guard in transform.py instead compares every column.
"""

from sqlalchemy import text

from appsflyer_pipeline.config import get_settings
from appsflyer_pipeline.loader import create_engine

settings = get_settings()
engine = create_engine(settings)
T = settings.db_table
assert T == "appsflyer_events_fb", f"unexpected table {T!r}"

with engine.connect() as conn:
    print("== exact-duplicate groups: same key + same event_revenue ==")
    n_groups, dup_rows, overcount_rev = conn.execute(
        text(
            f"""
            SELECT COUNT(*), COALESCE(SUM(n_rows - 1), 0),
                   COALESCE(SUM((n_rows - 1) * event_revenue), 0)
            FROM (
                SELECT event_time, event_name, appsflyer_id, attribution_type,
                       event_revenue, COUNT(*) AS n_rows
                FROM `{T}`
                GROUP BY event_time, event_name, appsflyer_id, attribution_type, event_revenue
                HAVING COUNT(*) > 1
            ) exact_dups
            """
        )
    ).one()
    print(f"groups: {n_groups}   extra rows: {dup_rows}   overcounted revenue: {overcount_rev}")

    print("\n== conflicting groups: same key, different event_revenue ==")
    n_conflict_groups, conflict_rows = conn.execute(
        text(
            f"""
            SELECT COUNT(*), COALESCE(SUM(n_rows), 0) FROM (
                SELECT event_time, event_name, appsflyer_id, attribution_type,
                       COUNT(*) AS n_rows, COUNT(DISTINCT event_revenue) AS n_distinct_revenue
                FROM `{T}`
                GROUP BY event_time, event_name, appsflyer_id, attribution_type
                HAVING COUNT(*) > 1 AND COUNT(DISTINCT event_revenue) > 1
            ) conflicts
            """
        )
    ).one()
    print(f"groups: {n_conflict_groups}   rows: {conflict_rows}")

    if n_groups or n_conflict_groups:
        print("\n== sample rows (up to 10 duplicate-key groups) ==")
        rows = conn.execute(
            text(
                f"""
                SELECT e.event_time, e.event_name, e.appsflyer_id, e.attribution_type,
                       e.event_revenue, e.app_id
                FROM `{T}` e
                JOIN (
                    SELECT event_time, event_name, appsflyer_id, attribution_type
                    FROM `{T}`
                    GROUP BY event_time, event_name, appsflyer_id, attribution_type
                    HAVING COUNT(*) > 1
                    LIMIT 10
                ) d ON e.event_time = d.event_time
                   AND e.event_name = d.event_name
                   AND e.appsflyer_id = d.appsflyer_id
                   AND e.attribution_type = d.attribution_type
                ORDER BY e.event_time, e.appsflyer_id
                """
            )
        ).all()
        for r in rows:
            print(f"  t={r[0]} event={r[1]} af_id={r[2][:12]}... [{r[3]}] rev={r[4]} app={r[5]}")
    else:
        print("\nNo duplicate-key groups found.")
```

- [ ] **Step 3: Run it against the real database**

Run: `uv run python scratch/probe_issue23.py`
Expected: prints the two group counts (exact-duplicate and conflicting) plus revenue impact; either both zero (no gap in practice) or a nonzero count with sample rows printed. Record the exact numbers in your report to the user — they determine what (if anything) Task 4's follow-up purge needs to cover; do not guess at them ahead of running the script.

- [ ] **Step 4: Commit the gitignore entry only**

```bash
git add .gitignore
git commit -m "$(cat <<'EOF'
Gitignore local one-off audit/probe scripts (#23)

scratch/ holds read-only production diagnostics like probe_issue23.py -
same precedent as issue #7's investigation. Never committed: this repo
is public and these connect to production via .env credentials.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: PR — gates, push, open

**Files:** none (git/gh only)

**Interfaces:**
- Consumes: branch `issue-23-dedup-key-guard` with Tasks 2–3 committed.
- Produces: an open PR that closes #23 on merge. **Do not merge.**

- [ ] **Step 1: Full local gates**

Run: `uv run pre-commit run --all-files && uv run pytest`
Expected: all pass (integration tests may SKIP locally without a reachable DB).

- [ ] **Step 2: Push and open the PR**

```bash
git push -u origin issue-23-dedup-key-guard
gh pr create --title "Add dedup-key guard for same-report duplicate rows" --body "$(cat <<'EOF'
Fixes #23, per the design spec `docs/superpowers/specs/2026-07-08-dedup-key-guard-design.md`.

`transform_events` now collapses exact duplicate rows sharing
`(event_time, event_name, appsflyer_id)` — `attribution_type`/`app_id` are already
constant per call, so this is Mark's full 4-column dedup key (BAF-2 comment 62585).
A collapsed duplicate logs a WARNING (visibility, same principle as #10); rows
sharing the key but disagreeing on another field (e.g. `event_revenue`) raise
`TransformError` instead of silently picking one.

This is distinct from #7's `Is Primary Attribution` filter, which handles
cross-report duplication (a retargeting-attributed event's secondary copy in the
UA report). This guard instead covers AppsFlyer returning the same row twice
within a single report call — a gap neither #7's fix nor the window-replace
idempotency in `load_events` covers.

A read-only production audit (`scratch/probe_issue23.py`, gitignored, same
precedent as #7's `probe_issue7.py`) was run against `appsflyer_events_fb` before
this fix to baseline whether the gap has already produced duplicates in prod —
see the PR discussion / issue comments for the actual numbers found.

Tests: exact-duplicate collapsing (with WARNING assertion) and conflicting-field
raise, in `test_transform.py`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Wait for CI and report**

Run: `gh pr checks --watch`
Expected: all checks green. Report the PR URL, CI status, and the audit numbers from Task 3 to the user. **Stop — merging needs explicit user authorization.**

---

## Follow-up after merge (not part of this plan)

Per the design spec's Verification sequencing: once this PR is merged and deployed, and only if Task 3's baseline found duplicate-key groups, re-run `backfill --start-date ... --end-date ...` scoped **only** to the specific `(app_id, attribution_type)` windows the audit flagged (not a blanket re-backfill — issue #7's purge already burned into the ~6-7/day download quota once). Then re-run `scratch/probe_issue23.py` to confirm 0 remaining duplicate-key groups, and note in issue #23 whether any conflicting-duplicate windows needed manual resolution before they could be reloaded. This step is deliberately left out of the numbered tasks above because its exact scope (which windows, which dates) is only known after Task 3's real results — it cannot be pre-scripted without guessing.
