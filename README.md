# appsflyer-to-analytics-db

Loads AppsFlyer Pull API purchase events (Non-Organic + Retargeting, Facebook Ads) into the analytics
MariaDB. Implements [BAF-2](https://yesimapp.atlassian.net/browse/BAF-2). Design details in
[`docs/design-spec.md`](docs/design-spec.md).

## Requirements

- Python 3.12
- [uv](https://docs.astral.sh/uv/) — dependency/environment manager

## Setup

```bash
uv sync                     # creates .venv and installs runtime + dev dependencies
cp .env.example .env        # then fill in DB + AppsFlyer credentials — .env is gitignored
uv run pre-commit install   # optional: run lint/format/type checks on every commit
```

## Usage

```bash
uv run appsflyer-pipeline check-connection   # verify DB connectivity
uv run appsflyer-pipeline create-table       # create the target table (idempotent)
uv run appsflyer-pipeline backfill           # historical load: full available window (<=90 days)
uv run appsflyer-pipeline daily              # yesterday's incremental load
```

Add `--dry-run` to `backfill`/`daily` to preview row counts without writing to the database.

`backfill` accepts `--start-date`/`--end-date` (ISO `YYYY-MM-DD`) to override the default 90-day
window — e.g. to re-run a specific gap, or to probe what AppsFlyer actually returns for dates older
than its 90-day retention floor (see the "Known open issue" in `CLAUDE.md`). `daily` accepts `--date`
to replay a single missed day. Both loads are idempotent per `(app_id, attribution_type, window)`, so
re-running any of these is always safe.

## Development

```bash
uv run ruff check .          # lint
uv run ruff format .         # format
uv run mypy                  # type check
uv run pytest                # tests (unit + respx-mocked HTTP; mysql:8 service in CI)
uv run pre-commit run --all-files   # same checks CI gates on — run before pushing
```

## Deployment

Runs unattended on a CLI-only server via a systemd timer (`deploy/appsflyer-daily.service` +
`deploy/appsflyer-daily.timer`, no Docker): a dedicated system user builds the venv with `uv sync`,
secrets live in a mode-600 `EnvironmentFile` (`deploy/appsflyer.env.example` is the template), and the
timer fires `appsflyer-pipeline daily` once a day. Full install, scheduling, verification, and
first-backfill steps: [`docs/RUNBOOK.md`](docs/RUNBOOK.md).
