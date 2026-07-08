# Design: issues #9, #8 (+ #10's guard) — config validation, daily lookback mechanism, wipe visibility

**Date:** 2026-07-08
**Issues:** [#9](https://github.com/unreal-kz/appsflyer-to-analytics-db/issues/9),
[#8](https://github.com/unreal-kz/appsflyer-to-analytics-db/issues/8),
[#10](https://github.com/unreal-kz/appsflyer-to-analytics-db/issues/10)
**Status:** approved (defaults settled in brainstorming: lookback default **1**, per Mark's
daily-script behavior — the trailing-window mechanism is config-only opt-in)

## Goals

1. **#9 — fail loudly on empty config lists.** An empty `APPSFLYER_APP_IDS` or
   `APPSFLYER_EVENT_NAMES` (e.g. a truncated line in the hand-edited mode-600 systemd
   `EnvironmentFile`) must abort startup with a pydantic `ValidationError` instead of a
   successful no-op run (empty app list) or an active data wipe (empty event list makes
   `transform`'s `is_in([])` re-filter drop every row, after which the loader deletes the
   window and inserts nothing).
2. **#8 — make the daily window depth configurable.** `run_daily` currently pulls exactly
   `[yesterday, yesterday]` (inherited from Mark's `mark_1day.py`), fired at 05:00 +03 =
   02:00 UTC — AppsFlyer's documented late-event boundary. Late/offline-cached events for a
   day that has already been pulled are never recovered. The fix ships a trailing-window
   mechanism in Mark's `days_back` shape (`mark_allday.py::get_date_chunks`): window =
   `[yesterday − (N−1), yesterday]`.
3. **#10 — make window wipes visible.** `load_events` deletes the window before inserting;
   a successful-but-empty fetch for a previously loaded day silently erases it. Log the
   DELETE rowcount on every load and WARN on non-empty→empty transitions. Required by #8's
   own caveat to land with (or before) the window change.

## Non-goals

- Changing the **default** daily behavior. Decided in brainstorming: default lookback stays
  **1 day** (Mark's daily as-is). Production enablement of a deeper window is a deploy-time
  config decision (`APPSFLYER_DAILY_LOOKBACK_DAYS=3` in the server EnvironmentFile),
  documented but not made for the operator. #8 therefore closes (or stays open) only after
  that explicit enablement decision — it must not silently close as "fixed by default".
- Blocking empty-window loads (#10 asks for visibility, not a guard rail that refuses).
- New CLI flags. The lookback is env-config only; `--date` remains the targeted-repair tool.
- Issues #11–#17.

## Design

### Part 1 — #9: `min_length=1` on both CSV fields (`src/appsflyer_pipeline/config.py`)

```python
appsflyer_app_ids: Annotated[CsvList, Field(min_length=1)]
appsflyer_event_names: Annotated[CsvList, Field(min_length=1)] = ["af_purchase", "af_purchase_YC"]
```

The existing `mode="before"` `_parse_csv_fields` validator turns `""`/`" , ,"` into `[]`;
`min_length=1` then rejects it at `Settings()` construction. The CLI already surfaces this
as `FAILED: …` + exit 1 (pydantic v2 `ValidationError` subclasses `ValueError`, which
`backfill`/`daily` catch — see #12; behavior is correct here even though that issue's
comment cleanup is out of scope). Valid configs see no change.

### Part 2 — #8: `APPSFLYER_DAILY_LOOKBACK_DAYS` (config + `pipeline.run_daily`)

- New setting: `appsflyer_daily_lookback_days: int = 1`, bounds `ge=1, le=90`
  (90 = `MAX_RETENTION_DAYS`, the Pull API retention floor).
- `run_daily` (no explicit date): `end = yesterday`, `start = end − (N−1)` — Mark's
  `days_back` semantics. With the default N=1 this is exactly the current single-day pull.
- **Explicit `--date D` stays a strict single-day `[D, D]` pull**, regardless of N — it is
  a targeted repair/re-pull tool; the lookback applies only to the scheduled/default case.
  Documented in the option's help text.
- No quota impact at any N ≤ 31: one report download per (app_id, attribution_type) per run
  regardless of range length; N > 31 chunks via the existing `chunk_date_range` (more
  downloads — bounded by `le=90`, and the RUNBOOK documents the quota math).
- Idempotency: re-writing `[yesterday−(N−1), yesterday]` daily is safe by construction
  (delete-then-insert per exact window).

### Part 3 — #10: wipe visibility (`src/appsflyer_pipeline/loader.py`)

In `load_events`, capture the DELETE's `rowcount` and:

- always log `deleted=N inserted=M` for the window at INFO;
- WARN when `deleted > 0 and not rows`: a previously non-empty window became empty —
  legitimate only if AppsFlyer genuinely revised the day to zero, so it must be loud in
  `journalctl`.

Logging only. Return value and behavior unchanged. (MySQL/PyMySQL DELETE reports accurate
`rowcount`; no `SET ROWCOUNT` caveats apply.)

## Tests (extend existing files/patterns)

- `tests/test_config.py`: empty and whitespace-only `APPSFLYER_APP_IDS` /
  `APPSFLYER_EVENT_NAMES` → `ValidationError`; lookback default is 1; `0` and `91`
  rejected; `3` accepted.
- `tests/test_pipeline.py`: default `run_daily` window unchanged (single day, via the
  `_today` seam); with lookback 3 the window spans `[yesterday−2, yesterday]` as one chunk;
  explicit `date=` stays single-day even with lookback set.
- `tests/test_loader.py`: `caplog` asserts the `deleted=/inserted=` INFO line and the
  non-empty→empty WARNING; no WARNING when rows are inserted or nothing was deleted.
- CI gate is `--cov-fail-under=98` with branch coverage — new branches need covering tests.

## Docs & deploy artifacts

- `deploy/appsflyer.env.example`: add `APPSFLYER_DAILY_LOOKBACK_DAYS` (commented, with the
  recommendation to set `3` in production and why).
- `docs/RUNBOOK.md`: env-var table entry + a short late-events note (02:00 UTC boundary,
  quota math for N ≤ 31, enablement is an operator decision).
- `docs/design-spec.md`: update the daily-window description; amend the late-event risk row
  to "mechanism available, default preserves Mark's single-day behavior, production
  enablement pending" — flagged, not silently resolved (same style as the 90-day retention
  conflict).

## Delivery

- **Two branches/PRs** (matching the PR-per-issue pattern):
  1. `issue-9-config-min-length` — Part 1 + tests. `Fixes #9`.
  2. `issue-8-daily-lookback` — Parts 2 + 3 + tests + docs. `Fixes #10`; **references** #8
     with the honest-status comment (mechanism merged; default unchanged; enablement is a
     server-config decision) rather than auto-closing it.
- This spec file rides with the first PR branch.
- Merges only on the user's explicit authorization, per established workflow.

## Live verification (user's standing preference: real runs over mocks)

1. Local `daily --dry-run` with the real `.env` — confirms default behavior is byte-for-byte
   the current single-day window.
2. Local `daily --dry-run` with `APPSFLYER_DAILY_LOOKBACK_DAYS=3` — confirms a 3-day window
   fetches as one report per combo and previews sensible row counts.
3. A real (non-dry-run) `daily` re-run to see the #10 guard's `deleted=/inserted=` line
   against production data.
- **Quota budget:** 2026-07-08's purge backfill already spent ~2 downloads per combo
  (empirical limit ~6-7/day per combo). Steps 1–3 cost 3 more per combo — feasible today,
  but with little margin for extra live iterations; defer repeats to the next quota reset if
  anything needs a second pass.

## Rollout note

After merge + server `git pull` && `uv sync`, nothing changes in production behavior until
the operator adds `APPSFLYER_DAILY_LOOKBACK_DAYS=3` (or another depth) to the server
EnvironmentFile. That enablement — and then closing #8 — is a separate, explicit decision.
