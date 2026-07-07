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
uv run appsflyer-pipeline check-connection   # verify DB connectivity          (Stage 1)
uv run appsflyer-pipeline create-table       # create the target table        (Stage 2)
uv run appsflyer-pipeline backfill           # one-time historical load        (Stage 5)
uv run appsflyer-pipeline daily              # yesterday's incremental load    (Stage 5)
```

Add `--dry-run` to `backfill`/`daily` to preview row counts without writing to the database.

## Development

```bash
uv run ruff check .          # lint
uv run ruff format .         # format
uv run mypy                  # type check
uv run pytest                # tests (unit + respx-mocked HTTP; mysql:8 service in CI)
```

## Deployment

Runs unattended on a CLI-only server via a systemd timer — see [`docs/RUNBOOK.md`](docs/RUNBOOK.md)
(added in Stage 7) for install, scheduling, and first-backfill steps.
