# appsflyer-to-analytics-db

Loads AppsFlyer Pull API purchase events (Non-Organic + Retargeting, Facebook Ads) into the
analytics MariaDB. Implements [BAF-2](https://yesimapp.atlassian.net/browse/BAF-2). Full design
in [`docs/design-spec.md`](docs/design-spec.md) — read it before making architectural changes.

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

## Known open issue

The ticket's acceptance criteria ask for backfill from **2025-01-01**, but the AppsFlyer Pull API
only retains **90 days** of data (per Mark Malovichko's BAF-2 comment). This is unresolved —
flagged in `docs/design-spec.md` — needs a stakeholder decision before backfill can be called done.

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
7. Server deploy (systemd unit+timer, RUNBOOK, first backfill) — artifacts done:
   `deploy/appsflyer-daily.{service,timer}` + `deploy/appsflyer.env.example` + `docs/RUNBOOK.md`
   (systemd + `uv` interaction, EnvironmentFile format, security hardening, troubleshooting all
   documented — not yet applied to a real server in this environment). First backfill run live
   against production: 11/12 windows loaded (1,285 rows); one window hit AppsFlyer's daily per-app
   download quota (a real, expected 4xx per the design's rate-limit risk mitigation, not a bug) —
   pending a scoped retry once the quota resets. See `docs/design-spec.md`'s Acceptance Criteria and
   Risks for full detail.
