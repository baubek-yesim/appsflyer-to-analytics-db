# appsflyer-to-analytics-db

Loads AppsFlyer Pull API raw data into the analytics MariaDB: every in-app event (Non-Organic +
Retargeting, all media sources) into one table, every install / retargeting conversion (all 128
raw fields) into a second. Built for [BAF-2](https://yesimapp.atlassian.net/browse/BAF-2)
(Facebook Ads purchases only) and widened by [BAF-11](https://yesimapp.atlassian.net/browse/BAF-11)
to the full raw export. Full design in [`docs/design-spec.md`](docs/design-spec.md) — read it
before making architectural changes.

## Stack

- Python 3.12, managed with **uv** (`pyproject.toml` + `uv.lock`) — not Poetry/pip-tools.
- `src/appsflyer_pipeline/` package layout with a `typer` CLI (`appsflyer-pipeline` console script).
- **SQLAlchemy 2.0 + PyMySQL** against MariaDB/MySQL — matches the rest of the YESIM stack.
- **httpx + tenacity** for the AppsFlyer API client (retry/backoff on 429/5xx/network errors only —
  4xx fails fast). **polars** for CSV/DataFrame work.
- **pydantic-settings** for typed config from env vars / `.env`.
- Deploy target: native `uv sync` + a **systemd timer** on a CLI-only server — no Docker.

## Commands

```bash
uv sync                      # install/update deps into .venv
uv run ruff check .          # lint
uv run ruff format .         # format
uv run mypy                  # type check (strict)
uv run pre-commit run --all-files   # same checks CI gates on — run before pushing
uv run pytest                # unit + respx-mocked HTTP + live-DB integration tests (auto-skip if unreachable)
uv run appsflyer-pipeline check-connection   # verify DB connectivity
uv run appsflyer-pipeline create-table       # idempotent DDL
uv run appsflyer-pipeline backfill|daily     # --dry-run, --start-date/--end-date/--date overrides
```

CI (`.github/workflows/ci.yml`) runs `uv run pre-commit run --all-files` (ruff + ruff-format + mypy,
the same hooks as local `.pre-commit-config.yaml`) then `pytest --cov-fail-under=98` against a
`mysql:8` service container — a hook that breaks pre-commit breaks CI with it, so the config can't
silently rot.

## Conventions

- **Secrets:** `.env` (gitignored) locally via `python-dotenv`/pydantic-settings; on the server, a
  systemd `EnvironmentFile` (mode 600). Never commit credentials — this repo is **public** on GitHub.
- **Table name** is configured via `DB_TABLE`, not hardcoded — currently `appsflyer_events_fb`,
  already provisioned in production with the schema in `sql/create_table.sql`.
- **Idempotent loads:** delete-by-window-then-insert (not `ON DUPLICATE KEY UPDATE`) — AppsFlyer
  has no reliable natural unique key. See `docs/design-spec.md` for why.
- **SQL identifiers** (table names) are validated against an allowlist regex before being
  interpolated into raw SQL (`loader._validate_identifier`) — they can't be parameterized like values.
- Tests that touch a real database (`tests/test_loader_integration.py`) skip gracefully (not fail)
  when no DB is reachable — CI provides one via a service container; locally a real `.env` also
  satisfies it. They're read-only or `CREATE TABLE IF NOT EXISTS`, safe to run against production.

## AppsFlyer limits (read before touching production)

- **Availability window, not 90 days:** the Pull API refuses dates older than 90 days (HTTP 400),
  but only serves **31 days of in-app events** and **60 days of installs** (support.appsflyer.com,
  "Data availability windows"). In between it returns a valid, header-only **empty** report. The
  pipeline hard-clamps every report to its window (`reports.IN_APP_EVENTS_AVAILABILITY_DAYS` /
  `INSTALLS_AVAILABILITY_DAYS`) and `load_events` refuses to replace a populated window with an
  empty fetch — our table is the only copy of anything older. BAF-2's "backfill from 2025-01-01"
  is therefore unsatisfiable via this API, closed as such.
- **Download quota** is per report type × app × UTC day (resets 03:00 Europe/Riga), subscription-
  dependent (~6-7/day measured for in-app events), and counts **calls, not rows** — a 31-day pull
  and a `--dry-run` each cost exactly one. Never dry-run-then-run the same combo on the same day;
  see `docs/RUNBOOK.md` §9.

## Git workflow

Feature branch + PR per stage, merged into `main` after review. Branch naming: `stage-N-<slug>`
matching the stage numbering below.

## Build stages (tracked via TaskCreate/TaskUpdate each session)

0. Scaffold (uv, pyproject, CI, pre-commit) — done
1. Config + DB connectivity (`check-connection`) — done, verified live
2. Target table DDL + `create-table` — done, verified live (table already existed, schema matched)
3. AppsFlyer API client (hybrid from Mark's reference scripts) — done, verified live against the
   real API (surfaced two real bugs: httpx needs `follow_redirects=True`, unlike `requests`)
4. Transform + idempotent loader — done, verified live end-to-end (fetch -> transform -> load ->
   idempotent re-load) against production
5. Orchestration + CLI (`backfill`/`daily`, `--dry-run`, `--start-date`/`--end-date`/`--date`) —
   done, verified live: a real `daily` run loaded 136 rows, re-running was idempotent (still 136),
   dry-run previews never write
6. Tests + CI green — done: 68 tests, 99% branch coverage (`branch = true`, gated at
   `--cov-fail-under=98` in CI only); CI's lint/format/type steps consolidated into one
   `pre-commit run --all-files` step so the pre-commit config is continuously verified instead of
   sitting unexercised
7. Server deploy (systemd unit+timer, RUNBOOK, first backfill) — **live** on the target analytics
   server, but via a **no-root `systemd --user` stopgap** (`deploy/user-level/`, `docs/RUNBOOK.md`
   §14) — the deploy account has no sudo on that box yet, a request is in with the backend team. Real
   host/access details are kept out of this public repo — see whoever owns BAF-2. Once granted,
   migrate to the canonical root-based setup
   (`deploy/appsflyer-daily.{service,timer}`, §§1-13). Daily timer is enabled and armed (first
   scheduled fire: 2026-07-08 ~05:00 +03); `check-connection`/`daily --dry-run` verified live through
   the real systemd path. First backfill run live against production: 11/12 windows loaded
   (1,285 rows); a couple of (app_id, attribution_type) combos have since hit AppsFlyer's daily
   report-download quota (~6-7 downloads/day per combo, confirmed empirically — a real, expected 4xx
   per the design's rate-limit risk mitigation, not a bug) from the cumulative testing today — pending
   scoped retries once the quota resets. See `docs/design-spec.md`'s Acceptance Criteria and Risks,
   and `docs/RUNBOOK.md` §14, for full detail.

## BAF-11 build stages

[BAF-11](https://yesimapp.atlassian.net/browse/BAF-11) supersedes BAF-2's scope: drop the
media_source/event_names filters (load the full raw export), add a second report family
(installs/installs-retarget), keep everything BAF-2 already built. Full requirements analysis and
the 10-stage plan (Этап 0-10) are in
[`docs/superpowers/plans/2026-08-13-baf-11-full-raw-export.md`](docs/superpowers/plans/2026-08-13-baf-11-full-raw-export.md).
Branch/PR numbering below is this ticket's own (`baf-11-stage-N-<slug>`, independent of BAF-2's
`stage-N` numbers above) and doesn't map 1:1 onto the master spec's Этапы — noted per stage.

1. Этап 1 — optional `media_source`/`event_names` filters (three-valued: unset = no filter, named
   = BAF-2 behavior, blank = fail loud) — done, [PR #57](https://github.com/baubek-yesim/appsflyer-to-analytics-db/pull/57)
   merged.
2. Этап 2 + part of Этап 8 — quota-aware chunking (`APPSFLYER_CHUNK_DAYS`) and dedupe/NOT-NULL
   hardening — done, [PR #59](https://github.com/baubek-yesim/appsflyer-to-analytics-db/pull/59)
   merged.
3. Этап 3 — `ReportSpec`/`REPORTS` registry refactor (pure, no behavior change — generalizes the
   pipeline from hardcoded in-app-events assumptions to a report-type-parameterized model, the
   prerequisite for stage 4) — done, executed via subagent-driven development (7 tasks +
   whole-branch review, verdict clean),
   [PR #60](https://github.com/baubek-yesim/appsflyer-to-analytics-db/pull/60) merged.
4. Этап 5 + Этап 6 combined — second table (`DB_TABLE_INSTALLS`) + the installs/installs-retarget
   report itself (130-column DDL, full pass-through column mapping, distinct
   `(appsflyer_id, event_time)` dedupe key, hard-clamped 60-day retention) — done, executed via
   subagent-driven development (8 tasks; whole-branch review first returned `needs_fixes` — 6
   important cross-task findings, notably installs entering the live scheduled timer with no gate
   and a client-side filter silently zeroing installs rows — fixed in one follow-up round, CI green
   against `mysql:8` including the new installs-DDL-creation test),
   [PR #61](https://github.com/baubek-yesim/appsflyer-to-analytics-db/pull/61) merged. Installs is
   registered but **off by default** — `APPSFLYER_ENABLED_REPORTS` defaults to the two in-app-events
   reports only, so the deployed timer keeps pulling exactly what it does today until Этап 9.

5. Cutover safety (pre-Этап 9, replaces the parts of Этап 4 that the measured volumes actually
   need) — in-app-events hard-clamped to AppsFlyer's documented 31-day availability window (issue
   #45's "~35-day silent empty" explained and closed), `load_events` refuses to wipe a populated
   window with an empty fetch unless `allow_wipe=True`, `TimeoutStartSec` 1800 → 7200 (issue
   #35), RUNBOOK §9 rewritten around the real availability/quota model and §15 added with the
   cutover procedure — done, `baf-11-stage-5-cutover-safety`.

Not started: Этап 7 (prod PK/index migration — a one-off `ALTER`, RUNBOOK §15 step 2), Этап 9
(the cutover itself — RUNBOOK §15), Этап 10 (acceptance — §15 step 4). Deliberately not done:
Этап 4's streaming loader/transform (a full-mode day is ~25k rows, a full backfill under 1M — see
the 2026-08-13 measurement) and a real alerting backend (issue #16, stub kept by decision
2026-09-07).
