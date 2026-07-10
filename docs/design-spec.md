# Design Spec: AppsFlyer → Analytics DB Pipeline

- **Jira:** [BAF-2](https://yesimapp.atlassian.net/browse/BAF-2)
- **Risk level:** Level 3 (System Change) — involves production persistence (new/changed table in the
  analytics MariaDB) and a scheduled server-side job. Requires this spec + rollback plan before implementation.
- **Status:** Draft, pending resolution of the backfill-window conflict (see Risks below).

## Goals

- Load AppsFlyer **In-App Events** purchases attributed to Facebook Ads from two Pull API v5 sources —
  **Non-Organic** and **Retargeting** — into a single MariaDB table.
- Support both a one-time **historical backfill** and an ongoing **daily incremental** load.
- Make every load idempotent, so backfill chunks and daily runs can be safely re-run.
- Run unattended on a CLI-only Linux server via a systemd timer.

## Non-Goals

- Building a general-purpose AppsFlyer connector for media sources other than Facebook Ads, or event
  types other than `af_purchase` / `af_purchase_YC`.
- Real-time/streaming ingestion — this is a scheduled batch pull.
- Replacing or migrating the legacy `statistics.yesim_appsflyer_raw_events` table (co-existence is fine).
- Building analytics/BI on top of the loaded data (out of scope for this ticket).

## Requirements

**Functional** (from the ticket's acceptance criteria):
- Connect to both AppsFlyer Pull API endpoints (Non-Organic, Retargeting).
- Filter to `media_source = 'Facebook Ads'` and `event_name in ('af_purchase', 'af_purchase_YC')`.
- Union both sources into one table, tagged with `attribution_type` (`non_organic` | `retargeting`)
  and `app_id`.
- All fields listed in the ticket present (Attributed Touch Time, Install Time, Event Time, Event Name,
  Event Revenue, Media Source, Channel, Campaign, Campaign ID, Adset, Adset ID, Ad, Ad ID, AppsFlyer ID,
  Customer User ID) — column list finalized per Mark's DDL (BAF-2 comment 62293).
- Daily refresh, unattended.

**Non-functional:**
- Idempotent (safe to re-run any window without duplicating rows).
- Resilient to transient AppsFlyer API failures (retry with backoff) and DB connection blips
  (`pool_pre_ping`, timeouts).
- Secrets never committed; loaded from environment at runtime.
- Observable: structured logs to stdout, captured by journald on the server.

## System Boundaries

- **Upstream:** AppsFlyer Pull API v5 (`hq1.appsflyer.com`), authenticated by a per-app API token
  (owned by Mark Malovichko).
- **Downstream:** analytics MariaDB (same server/instance the other YESIM loaders write to),
  reached directly from this network.
- **Out of boundary:** AppsFlyer Data Locker / raw-data exports, Fabric/Power BI (any future consumer
  of the table reads it independently).

## Components

| Component | Responsibility |
|---|---|
| `config.py` | Typed settings (pydantic-settings) from env/`.env`: DB creds, API token, app IDs, filters. |
| `appsflyer_client.py` | Auth, both v5 endpoints, ≤31-day chunking, retry/backoff (tenacity), CSV parse. |
| `transform.py` | Map raw AppsFlyer fields → table columns; add `attribution_type` + `app_id`; apply filters. |
| `loader.py` | SQLAlchemy engine factory (pooling/timeouts); preflight checks; idempotent delete+insert; `PipelineError`. |
| `pipeline.py` | Orchestration: `run_backfill(start, end)`, `run_daily()` — chunk loop, logging, dry-run support. |
| `cli.py` | Typer commands: `check-connection`, `create-table`, `backfill`, `daily`. |

## Data Flow

```
                 ┌─────────────────────────┐
                 │   AppsFlyer Pull API     │
                 │  (Non-Organic + Retarget)│
                 └────────────┬────────────┘
                              │ HTTP GET, ≤31-day windows, CSV
                              ▼
                  appsflyer_client.py (retry/backoff)
                              │ raw rows (per source, per chunk)
                              ▼
                     transform.py (filter + map)
                              │ typed rows + attribution_type + app_id
                              ▼
              loader.py: DELETE window → INSERT (one transaction)
                              │
                              ▼
          MariaDB: analytics_statistics.appsflyer_events_fb
```

- **Backfill:** window = [today − 90d, yesterday] (subject to the conflict below), split into ≤31-day
  chunks; each chunk pulled from both sources, transformed, and loaded independently — a chunk failure
  doesn't roll back earlier chunks (idempotent replay is cheap and safe).
- **Daily:** window = [yesterday − (N−1), yesterday], N = `APPSFLYER_DAILY_LOOKBACK_DAYS`
  (default 1, i.e. the original [yesterday, yesterday] — issue #8); same client/transform/load
  path as one backfill chunk. An explicit `--date` pulls exactly that one day (targeted repair),
  ignoring the lookback.

## Interfaces

- **CLI:** `appsflyer-pipeline check-connection|create-table|backfill|daily [--dry-run]`.
  `backfill` also accepts `--start-date`/`--end-date` and `daily` accepts `--date` (ISO `YYYY-MM-DD`)
  to override the default window — a deliberate extension beyond the original spec, useful for
  gap-filling a missed day and for probing what AppsFlyer actually returns before its 90-day
  retention floor (see the backfill-window risk below). An explicit `--start-date` earlier than the
  floor is *not* silently clamped — the request proceeds and a warning is logged, since the resulting
  behavior is itself evidence toward resolving that open question.
- **Config (env / `.env`):** see `.env.example` — `DB_HOST/PORT/USER/PASSWORD/NAME/TABLE`,
  `APPSFLYER_API_TOKEN`, `APPSFLYER_APP_IDS`, `APPSFLYER_MEDIA_SOURCE`, `APPSFLYER_EVENT_NAMES`,
  `APPSFLYER_DAILY_LOOKBACK_DAYS` (default 1), `APPSFLYER_TIMEZONE` (issue #53; unset = UTC,
  production sets `Europe/Riga` so report times and day boundaries match the analytics team's
  references). The two CSV list fields reject empty values at
  startup (issue #9) — a truncated EnvironmentFile line fails loudly instead of producing a
  silent no-op run (empty app list) or an active window wipe (empty event list).
- **Table schema:** `sql/create_table.sql` (Stage 2), per Mark's DDL in BAF-2 comment 62293.

## Alternatives Considered

- **Dependency manager:** `uv` chosen over Poetry/pip-tools — fastest, single lockfile, manages the
  Python interpreter itself; matches current (2026) best practice. *(User decision.)*
- **Deployment:** native `uv sync` + **systemd timer** chosen over Docker — no container runtime needed
  on the target server, and systemd gives journald logging + auto-restart without extra infra.
  Docker remains an option if the server later standardizes on containers. *(User decision.)*
- **Load strategy:** `DELETE window THEN INSERT` chosen over `INSERT ... ON DUPLICATE KEY UPDATE` —
  AppsFlyer data has no reliable natural unique key (duplicate/near-duplicate events are possible), so
  a partition-owning delete+insert is simpler and provably idempotent.
- **Target table:** `analytics_statistics.appsflyer_events_fb` — a new table over reusing
  `yesim_appsflyer_raw_events` (stale since 2023, missing `app_id`/Retargeting). Already provisioned
  in production; confirmed via `SHOW CREATE TABLE` (Stage 1) to match Mark's DDL exactly.

## Risks & Failure Modes

| Risk | Impact | Mitigation |
|---|---|---|
| **Ticket asks for backfill from 2025-01-01, but the Pull API retains only 90 days** (Mark's comment) | AC as written is unsatisfiable via this API | **Blocking — needs stakeholder decision** (accept rolling ~90-day backfill, or source pre-90-day history from AppsFlyer Data Locker/raw export/legacy table). Pipeline built to backfill the full available window; gap is flagged, not silently dropped. |
| AppsFlyer API rate limits / transient 5xx | Chunk pull fails mid-backfill | `tenacity` retry with exponential backoff + jitter; chunk-level isolation means a retry doesn't redo the whole backfill. |
| **AppsFlyer's daily report-download quota** (confirmed live, Stage 7: `HTTP 400 "You've reached your maximum number of in-app event reports that can be downloaded today for this app"`) | One (app_id, attribution_type) combo fails for the rest of that calendar day | This is a plain 4xx, not 429/5xx, so it correctly fails fast rather than retrying (retrying would just fail again immediately). Chunk-level isolation means only the affected combo is skipped — confirmed live twice (Stage 7): 11/12 backfill windows still loaded, and separately 2/4 `daily` windows. The quota appears to trip per (app_id, attribution_type) after roughly 6-7 report downloads in a single day — heavy same-day testing (dry-run previews *and* real runs *and* manual preflight checks, all against the same app/attribution pairs) exhausts it fast. Fix is time, not retries: re-run just the affected window(s) (`backfill --start-date/--end-date`) once the quota resets the next day. Operational takeaway: during any first-time or heavy manual testing, prefer going straight to a real (non-dry-run) call over a dry-run-then-real pair, and avoid re-running the same window/app repeatedly within one day. |
| **Late/offline-cached events arrive after the daily pull** (the 05:00 +03 timer = exactly AppsFlyer's documented 02:00 UTC late-event boundary; SDK-cached events from offline devices can arrive days late — issue #8) | Slow, silent under-count of purchases/revenue: a single-day window never revisits past days, and every run still reports success | `APPSFLYER_DAILY_LOOKBACK_DAYS` re-pulls a trailing window daily — zero extra quota at depths ≤31 (still one report download per combo per run) and idempotent by construction. **Default is 1 (original behavior); production enablement (recommended: 3) is an explicit operator decision, flagged here rather than silently changed.** #10's wipe-visibility logging covers the widened delete window. |
| Partial load (process killed mid-run) | Inconsistent window state | Delete+insert wrapped in a single DB transaction per window/source — either fully applied or fully rolled back. |
| Duplicate or re-attributed events on re-pull | Overcounted revenue | Delete-by-window-then-insert makes re-runs idempotent by construction. |
| **Dual attribution: a retargeting-attributed event also appears in the UA report** (~14 twin purchases/month measured — issues #7/#46/#47) | A purchase can appear under both `attribution_type` values | **Accepted by design since 2026-07-09 (issue #47, data-analytics decision on #46, reversing #7's filter):** all UA rows load, including `Is Primary Attribution = false`; dedup follows Mark's key (`event_time, event_name, appsflyer_id, attribution_type`), under which the twin rows are two legitimate dimension entries. Cross-attribution sums count such purchases in both dimensions — matching the reference extract's semantics. Per-attribution metrics are unaffected. |
| Schema drift on AppsFlyer's side (new/renamed fields) | Silent data loss or load failure | `transform.py` explicitly maps known fields only; unexpected/missing fields raise rather than silently drop (fail loud). |
| Credential leakage | Security incident | `.env` gitignored; server uses systemd `EnvironmentFile` (mode 600); no secrets in logs or git history. |
| DB connectivity blip | Job failure | `pool_pre_ping=True`, `pool_recycle`, connect/read/write timeouts; preflight check before the main run. |

## Testing Strategy

- **Unit:** transform field-mapping/filters; chunk-window math (31-day split, 90-day floor); idempotent
  delete+insert SQL builder; config validation (missing/invalid env fails fast).
- **HTTP:** `respx` mocks both AppsFlyer endpoints using fixtures derived from the ticket's sample data
  (Google Sheet, 18–24 May).
- **Integration (CI):** load path exercised against a `mysql:8` service container.
- **Manual verification:** row counts + revenue sums cross-checked against the sample sheet and a
  manual AppsFlyer UI export.

## Acceptance Criteria

Mirrors the ticket, restated as testable conditions:

- [x] `check-connection` succeeds against the analytics MariaDB. (Verified live, Stage 1.)
- [x] `create-table` creates `analytics_statistics.appsflyer_events_fb` (idempotent). (Verified live,
      Stage 2 — table already existed, schema matched.)
- [ ] `backfill` loads the full available history (≤90 days back from yesterday) from both API sources.
      (First real production run, Stage 7: **11/12 windows loaded (1,285 rows)**; one window —
      `id1458505230` retargeting, 2026-06-09..2026-07-06 — hit AppsFlyer's **daily per-app download
      quota** for in-app event reports (HTTP 400, not a retryable 429/5xx — correctly failed fast per
      design rather than retried) after repeated dry-run + real calls against the same app earlier the
      same day. Chunk-level isolation worked as designed: the other 11 windows were unaffected. Pending
      follow-up: re-run just that window once the quota resets (next day), e.g. `backfill --start-date
      2026-06-09 --end-date 2026-07-06`. Also see `docs/RUNBOOK.md` §9 for the separate ~90-day
      retention caveat — this loads the API's retained window, not the ticket's 2025-01-01 ask, which
      remains open.)
- [ ] `daily` run (via systemd timer) loads yesterday's data from both sources, unattended. (The `daily`
      command itself is verified live — Stage 5, 136 rows — including idempotent re-runs. The timer is
      now deployed and armed on the target server (as a no-root `systemd --user` stopgap —
      see `docs/RUNBOOK.md` §14 — pending a sudo grant to migrate to the canonical root-based setup in
      §§1-13), and `check-connection`/`daily --dry-run` were verified live through that exact systemd
      path. Its first *unattended, scheduled* fire is `2026-07-08` ~05:00 — not yet observed as of this
      writing.)
- [x] Rows from both sources coexist in one table, correctly tagged with `attribution_type` and `app_id`.
- [x] All required fields are populated; NOT NULL columns never null.
- [ ] Row counts and revenue sums match a manual AppsFlyer UI export within tolerance. (Not yet done —
      needs a manual cross-check against the ticket's sample sheet / AppsFlyer UI export.)
- [x] Re-running `backfill` or `daily` for an already-loaded window does not duplicate rows. (Verified
      live for `daily`, Stage 5 — same run, twice, stable row count. `backfill` shares the identical
      `load_events` delete-then-insert call per window — also covered by
      `test_loader_integration.py::test_load_events_is_idempotent_and_isolated` against the real DB.
      Deliberately did *not* re-run a full live `backfill` a second time just to re-prove this: it would
      burn more of AppsFlyer's already-tight daily per-app download quota (see above) for no new
      evidence beyond what's already proven at the unit/integration level plus `daily`'s live proof.)
- [x] `pytest` / `ruff` / `mypy` green in CI. (68 tests, 99% branch coverage, gated at
      `--cov-fail-under=98`; CI runs `pre-commit run --all-files` — Stage 6.)

## Rollback

- Loads are scoped to `(app_id, attribution_type, event date-range)` partitions — rolling back a bad
  load means re-running `DELETE` for the affected window (no data outside that window is touched).
- The table is additive/new; disabling the systemd timer fully stops future writes with no other
  system depending on this table yet.
