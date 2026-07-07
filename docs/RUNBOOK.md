# RUNBOOK: appsflyer-to-analytics-db

Operational guide for deploying and running the AppsFlyer → analytics MariaDB pipeline (BAF-2) on a
CLI-only Linux server via systemd. Design rationale: [`design-spec.md`](design-spec.md). Repo
conventions: [`../CLAUDE.md`](../CLAUDE.md).

Placeholders used throughout — adjust to your environment:

| Placeholder | Example | Meaning |
|---|---|---|
| service user | `appsflyer` | dedicated, non-login system account the job runs as |
| install dir | `/opt/appsflyer/appsflyer-to-analytics-db` | where the repo is cloned + venv built |
| secrets file | `/etc/appsflyer/appsflyer.env` | mode-600 systemd `EnvironmentFile` |

## 0. Overview

- Runs `appsflyer-pipeline daily` once a day via `appsflyer-daily.timer` → `appsflyer-daily.service`,
  loading yesterday's AppsFlyer Facebook Ads purchase events (Non-Organic + Retargeting) into
  `DB_NAME.DB_TABLE` (`analytics_statistics.appsflyer_events_fb` by default).
- Schedule: `05:00` server-local time (± up to 5 min jitter), catches up automatically if the server
  was down (`Persistent=true`).
- Secrets live only in `/etc/appsflyer/appsflyer.env` (mode 600) — never in the repo, never in git.
- Logs go to journald: `journalctl -u appsflyer-daily.service`.
- Every load is idempotent per `(app_id, attribution_type, date-window)` — re-running any command for
  the same window is always safe and never duplicates rows.

## 1. Prerequisites

- A user with `sudo` on the target host; systemd present (`systemctl --version`).
- **Outbound network egress on 443 to both** `hq1.appsflyer.com` **and** `rawdata.appsflyer.com` — the
  Pull API 302-redirects export delivery to the second host; a firewall that only allows `hq1` will
  fail every single pull. Also egress to the DB host on `DB_PORT` (3306 by default).
  ```bash
  getent hosts hq1.appsflyer.com
  getent hosts rawdata.appsflyer.com
  nc -vz <DB_HOST> 3306
  ```
- Python 3.12 available, or let `uv` fetch it.
- Credentials in hand: an AppsFlyer API token from Mark Malovichko, and an analytics-DB user with
  `SELECT`/`INSERT`/`DELETE`/`CREATE` on the target schema.

## 2. Create the dedicated service account

```bash
sudo useradd --system --home-dir /opt/appsflyer --create-home \
  --shell /usr/sbin/nologin appsflyer
```

## 3. Install `uv` (deploy-time only — not needed at runtime)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sudo env UV_INSTALL_DIR=/usr/local/bin sh
uv --version
```

`uv` is only used to build the venv during install/redeploy. The systemd unit execs the venv's
console script directly at runtime — see §7 for why.

## 4. Clone and build the venv

```bash
sudo mkdir -p /opt/appsflyer
sudo git clone https://github.com/baubek-yesim/appsflyer-to-analytics-db.git \
  /opt/appsflyer/appsflyer-to-analytics-db
sudo chown -R appsflyer:appsflyer /opt/appsflyer

# Build as the service user so ownership/permissions are correct throughout:
sudo -u appsflyer --login bash -c '
  cd /opt/appsflyer/appsflyer-to-analytics-db &&
  uv sync --frozen --no-dev'

# Sanity-check the console script exists and its shebang points into this venv:
head -1 /opt/appsflyer/appsflyer-to-analytics-db/.venv/bin/appsflyer-pipeline
```

`--frozen` fails loudly if `uv.lock` is out of date, instead of silently changing it on a server.
`--no-dev` skips pytest/mypy/ruff — not needed at runtime. If Python 3.12 isn't already installed,
`uv sync` downloads it (needs network + a writable `~/.cache/uv` at deploy time only); pin an existing
interpreter instead with `--python /usr/bin/python3.12` if preferred.

## 5. Create the mode-600 secrets file

```bash
sudo mkdir -p /etc/appsflyer
sudo install -o appsflyer -g appsflyer -m 600 \
  /opt/appsflyer/appsflyer-to-analytics-db/deploy/appsflyer.env.example \
  /etc/appsflyer/appsflyer.env
sudoedit /etc/appsflyer/appsflyer.env    # fill in real values -- see the format rules in the file
sudo chmod 600 /etc/appsflyer/appsflyer.env
sudo chown appsflyer:appsflyer /etc/appsflyer/appsflyer.env
ls -l /etc/appsflyer/appsflyer.env       # expect: -rw------- appsflyer appsflyer
```

**Format reminder** (full detail in `deploy/appsflyer.env.example`): this is a systemd
`EnvironmentFile`, not a shell script — no `export`, no `$VAR` expansion. `APPSFLYER_MEDIA_SOURCE=Facebook Ads`
is written with the space literal and unquoted. `APPSFLYER_APP_IDS`/`APPSFLYER_EVENT_NAMES` are plain
comma-separated values, **not** JSON arrays (the app's `CsvList` type disables JSON decoding, so a
`[...]` literal would be mis-split on commas).

## 6. Preflight — through systemd, not a shell `source`

Sourcing the env file in bash would choke on the unquoted space in `Facebook Ads` (bash would try to
run `Ads` as a command). Test through a transient systemd unit instead, using the exact same
`EnvironmentFile=` the real job uses:

```bash
sudo systemd-run --wait --pty --collect --unit=appsflyer-preflight \
  --property=User=appsflyer --property=Group=appsflyer \
  --property=WorkingDirectory=/opt/appsflyer/appsflyer-to-analytics-db \
  --property=EnvironmentFile=/etc/appsflyer/appsflyer.env \
  /opt/appsflyer/appsflyer-to-analytics-db/.venv/bin/appsflyer-pipeline check-connection

sudo systemd-run --wait --pty --collect --unit=appsflyer-preflight \
  --property=User=appsflyer --property=Group=appsflyer \
  --property=WorkingDirectory=/opt/appsflyer/appsflyer-to-analytics-db \
  --property=EnvironmentFile=/etc/appsflyer/appsflyer.env \
  /opt/appsflyer/appsflyer-to-analytics-db/.venv/bin/appsflyer-pipeline create-table
```

`check-connection` should print the MariaDB server version and the target table's status.
`create-table` is idempotent — the table already exists in production, so expect "is ready.".

## 7. Install the unit files and enable the timer

```bash
sudo install -m 644 \
  /opt/appsflyer/appsflyer-to-analytics-db/deploy/appsflyer-daily.service \
  /opt/appsflyer/appsflyer-to-analytics-db/deploy/appsflyer-daily.timer \
  /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now appsflyer-daily.timer   # enable the TIMER only, never the .service directly
```

Why the venv console script and not `uv run appsflyer-pipeline daily` as `ExecStart`: `uv run`
re-checks (and can re-sync) `uv.lock` on every invocation — under the unit's `ProtectHome`/
`ProtectSystem=strict` sandbox that needs `HOME`, a writable cache dir, and possibly network just to
start, and `uv` itself (installed via the standalone installer into `~/.local/bin`) usually isn't on
systemd's minimal `PATH` at all. The venv's own console script has an absolute shebang into this
host's venv `python`, so systemd execs it directly — no PATH lookup, no login shell, no `uv` needed at
runtime. If the install path is ever nested deep enough that the shebang line exceeds the kernel's
127-byte limit ("bad interpreter: No such file or directory"), switch `ExecStart` in the `.service`
file to:
```
ExecStart=/opt/appsflyer/appsflyer-to-analytics-db/.venv/bin/python -m appsflyer_pipeline.cli daily
```
which execs the interpreter directly and has no shebang-length dependency.

## 8. Verify

```bash
systemctl list-timers appsflyer-daily.timer      # shows NEXT / LEFT / LAST
systemctl status appsflyer-daily.timer

# One-off immediate smoke test of the real service unit (safe -- idempotent):
sudo systemctl start appsflyer-daily.service
journalctl -u appsflyer-daily.service -n 100 --no-pager
journalctl -t appsflyer-daily -f                 # follow live
```

## 9. First backfill (~90-day historical load)

> **Retention caveat — read before running.** The BAF-2 ticket's acceptance criteria ask for backfill
> from **2025-01-01**, but the AppsFlyer Pull API retains only **~90 days** of data (per Mark
> Malovichko's BAF-2 comment; `MAX_RETENTION_DAYS = 90` in `appsflyer_client.py`). This backfill
> therefore loads only `[yesterday − 89d, yesterday]` — **not** full history back to 2025-01-01. That
> gap is an **open, unresolved stakeholder decision** (accept a rolling ~90-day backfill, or source
> pre-90-day history from AppsFlyer Data Locker / a raw export / the legacy
> `yesim_appsflyer_raw_events` table) — see `design-spec.md`'s Risks table. Do not record this step as
> "full history loaded."

Preview first, then load, via `systemd-run` so it uses the same secrets/sandbox as the daily job and
lands in journald:

```bash
# Preview -- no writes:
sudo systemd-run --wait --pty --collect --unit=appsflyer-backfill \
  --property=Type=oneshot --property=TimeoutStartSec=7200 \
  --property=User=appsflyer --property=Group=appsflyer \
  --property=WorkingDirectory=/opt/appsflyer/appsflyer-to-analytics-db \
  --property=EnvironmentFile=/etc/appsflyer/appsflyer.env \
  /opt/appsflyer/appsflyer-to-analytics-db/.venv/bin/appsflyer-pipeline backfill --dry-run

# Real load, once the preview looks right:
sudo systemd-run --collect --unit=appsflyer-backfill \
  --property=Type=oneshot --property=TimeoutStartSec=7200 \
  --property=User=appsflyer --property=Group=appsflyer \
  --property=WorkingDirectory=/opt/appsflyer/appsflyer-to-analytics-db \
  --property=EnvironmentFile=/etc/appsflyer/appsflyer.env \
  /opt/appsflyer/appsflyer-to-analytics-db/.venv/bin/appsflyer-pipeline backfill
journalctl -u appsflyer-backfill -f
```

To gather evidence toward resolving the retention conflict, you can deliberately probe below the
90-day floor — the pipeline does **not** silently clamp an explicit `--start-date`; it logs a warning
and proceeds, so you can observe what AppsFlyer actually returns:

```bash
sudo systemd-run --wait --pty --collect --unit=appsflyer-probe \
  --property=User=appsflyer --property=Group=appsflyer \
  --property=WorkingDirectory=/opt/appsflyer/appsflyer-to-analytics-db \
  --property=EnvironmentFile=/etc/appsflyer/appsflyer.env \
  /opt/appsflyer/appsflyer-to-analytics-db/.venv/bin/appsflyer-pipeline \
  backfill --start-date 2025-01-01 --end-date <yesterday> --dry-run
```
Record the observed behavior (empty windows vs. errors) in the BAF-2 ticket to help close the open
question.

> **Daily download quota — confirmed live.** AppsFlyer caps how many in-app event reports can be
> downloaded *per app, per calendar day*. A backfill spans many chunks × apps × attribution types, so
> running `--dry-run` and then the real load back-to-back against the same app can exhaust that
> app's quota partway through the real run (observed: `HTTP 400 "You've reached your maximum number
> of in-app event reports that can be downloaded today for this app"`). This is a plain 4xx, so the
> client correctly does **not** retry it — retrying immediately would just fail again. Chunk-level
> isolation means the rest of the backfill still completes; only the exhausted window(s) fail. Do not
> immediately re-run the whole backfill to "fix" this — wait for the quota to reset (next day) and
> re-run just the failed window(s) with `--start-date`/`--end-date`. If you must minimize API calls
> during a first backfill, skip the `--dry-run` preview and go straight to the real load.

## 10. Monitoring (day-to-day)

```bash
systemctl list-timers appsflyer-daily.timer                 # is it scheduled? did it last fire?
journalctl -u appsflyer-daily.service --since yesterday      # last run's log
systemctl is-failed appsflyer-daily.service                  # quick health probe
journalctl -u appsflyer-daily.service -p err --since "-7d"   # errors in the last week
```

A failed run leaves the unit in `failed` state but does **not** block the next day's timer fire (daily
loads are idempotent and independent). Clear the cosmetic failed flag with:
```bash
sudo systemctl reset-failed appsflyer-daily.service
```

## 11. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `203/EXEC`, "No such file or directory" | `ExecStart` path wrong, or venv not built | Verify `.venv/bin/appsflyer-pipeline` exists; re-run `uv sync --frozen --no-dev` (§4). If the shebang is >127 bytes ("bad interpreter"), switch to the `python -m appsflyer_pipeline.cli` form (§7). |
| `200/CHDIR` | `WorkingDirectory` missing or unreadable by `appsflyer` | Check the install dir exists and is owned/readable by the service user. |
| "Failed to load environment files" / pydantic `ValidationError` | Env-file perms or format wrong | `ls -l /etc/appsflyer/appsflyer.env` (must be 600, owned by the service user); re-check §5/§6 — no JSON arrays, no `export`, literal unquoted spaces. |
| `PipelineError: Could not connect...` | DB unreachable | `nc -vz <DB_HOST> 3306`; check firewall/security group and `DB_USER` grants; confirm `RestrictAddressFamilies` in the unit still includes `AF_INET`/`AF_UNIX` (needed for DNS). |
| `AppsFlyerAPIError: HTTP 401/403` | Bad/expired API token | Get a fresh token from Mark Malovichko. |
| `AppsFlyerAPIError: HTTP 404` or empty result | Wrong `APPSFLYER_APP_IDS`, or a date before the 90-day floor | Confirm app IDs; expected for pre-retention dates (§9). |
| `AppsFlyerAPIError: HTTP 400 "...maximum number of in-app event reports that can be downloaded today..."` | AppsFlyer's per-app daily report-download quota exhausted (confirmed live — see the note in §9) | Don't retry today — it will fail again. Wait for the quota to reset (next day), then re-run just the failed window(s) with `backfill --start-date/--end-date`. |
| Job killed / times out | `TimeoutStartSec` too low for a large window | Already 1800s for daily / 7200s for backfill in the examples above; raise further if needed. |
| `SIGSYS` or crash right at startup | A hardening directive is too tight | Comment out `MemoryDenyWriteExecute` if enabled, then loosen `SystemCallFilter`; `daemon-reload` and retry. |

## 12. Rollback

Stop all future scheduled runs:
```bash
sudo systemctl disable --now appsflyer-daily.timer
systemctl list-timers | grep appsflyer || echo "timer gone"
```

Full removal:
```bash
sudo rm /etc/systemd/system/appsflyer-daily.{service,timer}
sudo systemctl daemon-reload
```

Undoing a bad *data* load: loads are scoped to `(app_id, attribution_type, event date-range)`
partitions — re-running `load_events`/`backfill --start-date/--end-date` for the affected window
deletes and reloads only that window; nothing outside it is touched (see `design-spec.md`'s Rollback
section).

## 13. Redeploy / upgrade

```bash
sudo -u appsflyer --login bash -c '
  cd /opt/appsflyer/appsflyer-to-analytics-db && git pull && uv sync --frozen --no-dev'
```
If the unit files themselves changed, re-run §7's `install` + `daemon-reload`. No explicit "restart"
is needed otherwise — it's a oneshot; the next timer fire automatically uses the updated venv.
