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
- **Daily:** window = [yesterday, yesterday]; same client/transform/load path as one backfill chunk.

## Interfaces

- **CLI:** `appsflyer-pipeline check-connection|create-table|backfill|daily [--dry-run]`.
  `backfill` also accepts `--start-date`/`--end-date` and `daily` accepts `--date` (ISO `YYYY-MM-DD`)
  to override the default window — a deliberate extension beyond the original spec, useful for
  gap-filling a missed day and for probing what AppsFlyer actually returns before its 90-day
  retention floor (see the backfill-window risk above). An explicit `--start-date` earlier than the
  floor is *not* silently clamped — the request proceeds and a warning is logged, since the resulting
  behavior is itself evidence toward resolving that open question.
- **Config (env / `.env`):** see `.env.example` — `DB_HOST/PORT/USER/PASSWORD/NAME/TABLE`,
  `APPSFLYER_API_TOKEN`, `APPSFLYER_APP_IDS`, `APPSFLYER_MEDIA_SOURCE`, `APPSFLYER_EVENT_NAMES`.
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
| Partial load (process killed mid-run) | Inconsistent window state | Delete+insert wrapped in a single DB transaction per window/source — either fully applied or fully rolled back. |
| Duplicate or re-attributed events on re-pull | Overcounted revenue | Delete-by-window-then-insert makes re-runs idempotent by construction. |
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

- [ ] `check-connection` succeeds against the analytics MariaDB.
- [ ] `create-table` creates `analytics_statistics.appsflyer_events_fb` (idempotent).
- [ ] `backfill` loads the full available history (≤90 days back from yesterday) from both API sources.
- [ ] `daily` run (via systemd timer) loads yesterday's data from both sources, unattended.
- [ ] Rows from both sources coexist in one table, correctly tagged with `attribution_type` and `app_id`.
- [ ] All required fields are populated; NOT NULL columns never null.
- [ ] Row counts and revenue sums match a manual AppsFlyer UI export within tolerance.
- [ ] Re-running `backfill` or `daily` for an already-loaded window does not duplicate rows.
- [ ] `pytest` / `ruff` / `mypy` green in CI.

## Rollback

- Loads are scoped to `(app_id, attribution_type, event date-range)` partitions — rolling back a bad
  load means re-running `DELETE` for the affected window (no data outside that window is touched).
- The table is additive/new; disabling the systemd timer fully stops future writes with no other
  system depending on this table yet.
