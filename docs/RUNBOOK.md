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
| deploy user (no-root stopgap, §14) | `<deploy-user>` | existing personal account used when root isn't available yet |

> **Current live status (2026-09-08, see `docs/2026-09-08-production-audit.md` for full detail):** the
> pipeline is deployed and running on the target analytics server via the **no-root stopgap** in §14
> below, not the root-based setup §§1-13 describe. `<deploy-user>` still has no sudo on that box (a
> request is in with the backend team); everything below this point assumes root is available. Read
> §14 first if you're picking this up before that's resolved. Deployed commit is `main` HEAD
> (verified live 2026-09-08); §15 Day D steps 1-2 are done (filters and `DB_TABLE_INSTALLS` already in
> the `EnvironmentFile`, code pulled and running clean), steps 3-7 are outstanding — see §15 step 0.
> (Real host/access details for this deployment are kept out of this public repo — ask whoever owns
> BAF-2 if you need them.)

## 0. Overview

- Runs `appsflyer-pipeline daily` once a day via `appsflyer-daily.timer` → `appsflyer-daily.service`,
  loading yesterday's AppsFlyer Facebook Ads purchase events (Non-Organic + Retargeting) into
  `DB_NAME.DB_TABLE` (`analytics_statistics.appsflyer_events_fb` by default).
- Since BAF-11 stage 4 the pipeline knows a **second** report family and a **second** table:
  `installs`/`installs-retarget` → `DB_NAME.DB_TABLE_INSTALLS`
  (`analytics_statistics.appsflyer_installs_fb` by default), a separate 130-column schema.
  `create-table`/`check-connection` cover **both** tables. The **scheduled timer does not pull
  installs** — `APPSFLYER_ENABLED_REPORTS` defaults to the two in-app-events reports only, and an
  operator opts a single run into installs deliberately (see §5). That gate stays in place until the
  cutover decision.
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
sudo -u appsflyer env HOME=/opt/appsflyer bash -c '
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

> **Upgrading to BAF-11 stage 4 — do this BEFORE `git pull`/`uv sync` (§13):** two new env vars.
>
> - **`DB_TABLE_INSTALLS` — required, no default.** Every command loads the full `Settings` at
>   startup, so an EnvironmentFile without this key fails *every* invocation (including
>   `check-connection`) with `FAILED: invalid configuration: db_table_installs: Field required`.
>   Add it to `/etc/appsflyer/appsflyer.env` (§14's stopgap: `~/appsflyer-secrets/appsflyer.env`)
>   **first**, then pull. Suggested value: `DB_TABLE_INSTALLS=appsflyer_installs_fb`. The table
>   itself does not have to exist yet — `create-table` provisions it, and the run-time preflight
>   only checks the tables of the reports a run is actually enabled to fetch.
> - **`APPSFLYER_ENABLED_REPORTS` — optional, safe default.** Unset means
>   `in_app_events_non_organic,in_app_events_retargeting`: exactly today's behaviour, installs never
>   pulled. **A safe deploy does not need this line at all.** Add it only to deliberately opt a run
>   into installs, and prefer scoping that to one run via a second `EnvironmentFile` on the
>   `systemd-run` command line (§14's note on override precedence) over editing the standing file —
>   editing the standing file opts the *scheduled timer* in too. An unrecognized key aborts the run
>   loudly rather than silently pulling nothing.

**Format reminder** (full detail in `deploy/appsflyer.env.example`): this is a systemd
`EnvironmentFile`, not a shell script — no `export`, no `$VAR` expansion. `APPSFLYER_MEDIA_SOURCE=Facebook Ads`
is written with the space literal and unquoted. `APPSFLYER_APP_IDS`/`APPSFLYER_EVENT_NAMES` are plain
comma-separated values, **not** JSON arrays (the app's `CsvList` type disables JSON decoding, so a
`[...]` literal would be mis-split on commas).

Optional: `APPSFLYER_DAILY_LOOKBACK_DAYS=3` widens the daily run to a trailing 3-day
window ending yesterday, re-capturing AppsFlyer late/offline-cached events at no extra
report-download quota (default when unset: 1 = yesterday only; see
`deploy/appsflyer.env.example` for the full rationale).

Optional: `APPSFLYER_CHUNK_DAYS` narrows the per-call date window below AppsFlyer's 31-day
ceiling (default when unset: 31). Only relevant today for shrinking a retry's blast radius;
becomes load-bearing if the media-source/event-name filters are ever removed (see
`deploy/appsflyer.env.example` for the row-cap arithmetic) -- at that point it is the sizing
mechanism that keeps a single call under the client's 1,000,000-row cap, since a chunk that
still overflows gets bisected and each half costs its own report-download quota.

`APPSFLYER_TIMEZONE=Europe/Riga` (issue #53) makes AppsFlyer express report times — and
interpret the from/to day boundaries — in that zone instead of UTC. Required in this
deployment: the analytics team's reference exports are Europe/Riga, and UTC pulls land
3 hours behind them. The value must match the app-level timezone setting in AppsFlyer
exactly; a malformed zone name fails startup, but a valid-but-wrong one silently falls
back to UTC server-side.

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

`check-connection` should print the MariaDB server version and **each** target table's status —
since BAF-11 stage 4 that is two lines, `DB_TABLE` and `DB_TABLE_INSTALLS`. `create-table` is
idempotent and likewise covers both, so expect two "is ready." lines; the in-app-events table
already exists in production, the installs one is created on first run. Provisioning the installs
table does **not** by itself make any run pull installs — that is `APPSFLYER_ENABLED_REPORTS`'s
job (§5), and it is deliberately scoped the other way so the table can be prepared ahead of a real
cutover.

**Schema note (2026-07-08, issue #14; updated 2026-07-10):** the live table gained an `id`
PRIMARY KEY and an `idx_app_attr_time (app_id, attribution_type, event_time)` covering index for
the DELETE/query pattern `load_events` already uses — via the one-time migration in
`sql/migrations/2026-07-08-add-id-pk-and-index.sql`, run once by hand against the already-existing
production table. `create-table`/`sql/create_table.sql` include both in any future fresh install.
The `TIMESTAMP` vs `DATETIME` question is resolved: the schema owner recreated the production
table on 2026-07-10 with `DATETIME` time columns (wiping all rows), and the repo DDL matches.
That recreation did **not** carry over the #14 `id`/PRIMARY KEY/index — re-run the migration
above to restore them.

## 7. Install the unit files and enable the timer

```bash
sudo install -m 644 \
  /opt/appsflyer/appsflyer-to-analytics-db/deploy/appsflyer-daily.service \
  /opt/appsflyer/appsflyer-to-analytics-db/deploy/appsflyer-daily.timer \
  /opt/appsflyer/appsflyer-to-analytics-db/deploy/appsflyer-alert@.service \
  /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now appsflyer-daily.timer   # enable the TIMER only, never the .service directly
# appsflyer-alert@.service needs no `enable` -- OnFailure= starts it on demand (see §10).
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

## 9. First backfill — and the two AppsFlyer limits that shape every run

> **Rewritten 2026-09-07 (BAF-11 stage 5).** Two facts from AppsFlyer's own documentation
> ("Data availability windows" and "Report generation quotas", support.appsflyer.com) explain
> every "weird limit" this project has hit. Read them before running anything by hand.

**Availability window (how far back data exists).** The API *accepts* dates up to 90 days back
(older → HTTP 400) but only *serves*:

| Report type | Data available |
|---|---|
| In-app events (`in_app_events_report`, `in-app-events-retarget`) | **31 out of the last 90 days** |
| Installs / retargeting conversions (`installs_report`, `installs-retarget`) | **60 out of the last 90 days** |

Between the window and 90 days the API returns HTTP 200 with a valid header and **zero rows** —
indistinguishable from a quiet day (issue #45's "~35-day" observation). Our table is the only copy
of anything older. The pipeline therefore **hard-clamps** every report to its window (a requested
start before it is moved up with a WARNING; a window entirely before it is skipped, "0/0 windows",
exit 0), and `load_events` **refuses** to replace a populated window with an empty fetch (a FAILED
window, exit 1, rows untouched). BAF-2's "backfill from 2025-01-01" is unsatisfiable via this API.
Do not try to work around either guard by hand.

**Download quota (how many calls per day).** Per **report type × app × calendar day (00:00 UTC =
03:00 Europe/Riga)**, plus an account-level cap; the advertiser values depend on the subscription
and are not published — measured ~6-7/day for in-app events. It counts **calls, not rows**: a
31-day chunk costs the same as one day, and **`--dry-run` costs the same as a real run.** The four
report types are four separate quotas. The raw-data export page in the AppsFlyer UI has its own,
separate quota (a manual UI export never competes with the pipeline; a Pull API script using the
same token does). The scheduled timer fires at 05:00 Riga, after the reset. Observed live:
`HTTP 400 "You've reached your maximum number of in-app event reports that can be downloaded today
for this app"` — a plain 4xx, correctly not retried; chunk isolation means only that combo fails.

Cost of the standard operations, per (app × report type):

| Operation | Calls per combo | Total, 2 apps × 4 reports |
|---|---:|---:|
| `daily` in full mode | 1 | 8 |
| `backfill` (no args): in-app events, 31 days, chunk 31 | 1 | 4 |
| `backfill` (no args): installs, 60 days, chunk 31 | 2 | 8 |
| **Cutover day = daily + one backfill** | **≤ 3** | **20** |

The only way to exhaust the quota is repeated same-day testing on the same combo. Rules: never
dry-run and then run the same window the same day; never re-run a whole backfill to "fix" one
failed window — wait for 03:00 Riga and re-run just that window with `--start-date/--end-date`.

**The backfill itself.** Go straight to the real call (a preview would spend the same quota) via
`systemd-run`, so it uses the same secrets/sandbox as the daily job and lands in journald:

```bash
sudo systemd-run --collect --unit=appsflyer-backfill \
  --property=Type=oneshot --property=TimeoutStartSec=10800 \
  --property=User=appsflyer --property=Group=appsflyer \
  --property=WorkingDirectory=/opt/appsflyer/appsflyer-to-analytics-db \
  --property=EnvironmentFile=/etc/appsflyer/appsflyer.env \
  /opt/appsflyer/appsflyer-to-analytics-db/.venv/bin/appsflyer-pipeline backfill
journalctl -u appsflyer-backfill -f
```

Expect two `clamping ...` WARNINGs (one per report family — the nominal 90-day request being cut
to 31/60 days), then one `OK` line per chunk. The no-root stopgap (§14) uses the same command with
`--user`, no `User=`/`Group=`, and the `$HOME/...` paths.

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

**Alerting on failure (issue #16):** both `appsflyer-daily.service` variants declare
`OnFailure=appsflyer-alert@%n.service`, so a non-zero exit triggers
`appsflyer-alert@appsflyer-daily.service` — a stub unit that logs a loud, `crit`-priority,
greppable journal entry. It does **not** page anyone yet; there is no webhook/mail backend
configured. Wire one in by editing only the alert unit's `ExecStart` (root:
`deploy/appsflyer-alert@.service`; user-level: `deploy/user-level/appsflyer-alert@.service`) —
`appsflyer-daily.service` itself never needs to change again.

Test the wiring without breaking the real job:
```bash
# user-level (what's actually live):
systemctl --user start appsflyer-alert@appsflyer-daily.service
journalctl --user -u appsflyer-alert@appsflyer-daily.service --no-pager

# root-based (once live):
sudo systemctl start appsflyer-alert@appsflyer-daily.service
sudo journalctl -u appsflyer-alert@appsflyer-daily.service --no-pager
```

**Structural limit:** `OnFailure=` only fires when the service *exits non-zero* — it cannot detect
the timer silently failing to fire at all (a broken timer, a machine that never boots back up).
If that failure mode matters, add a dead-man's switch instead/in addition: ping a
healthchecks.io-style URL as the last step of a successful `daily` run, and let the external
monitor alert on missed pings.

## 11. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `203/EXEC`, "No such file or directory" | `ExecStart` path wrong, or venv not built | Verify `.venv/bin/appsflyer-pipeline` exists; re-run `uv sync --frozen --no-dev` (§4). If the shebang is >127 bytes ("bad interpreter"), switch to the `python -m appsflyer_pipeline.cli` form (§7). |
| `200/CHDIR` | `WorkingDirectory` missing or unreadable by `appsflyer` | Check the install dir exists and is owned/readable by the service user. |
| "Failed to load environment files" / pydantic `ValidationError` | Env-file perms or format wrong | `ls -l /etc/appsflyer/appsflyer.env` (must be 600, owned by the service user); re-check §5/§6 — no JSON arrays, no `export`, literal unquoted spaces. |
| `PipelineError: Could not connect...` | DB unreachable | `nc -vz <DB_HOST> 3306`; check firewall/security group and `DB_USER` grants; confirm `RestrictAddressFamilies` in the unit still includes `AF_INET`/`AF_UNIX` (needed for DNS). |
| `AppsFlyerAPIError: HTTP 401/403` | Bad/expired API token | Get a fresh token from Mark Malovichko. |
| `AppsFlyerAPIError: HTTP 404` or empty result | Wrong `APPSFLYER_APP_IDS` | Confirm app IDs. |
| `AppsFlyerAPIError: HTTP 400 "...maximum number of ... reports that can be downloaded today..."` | That report type's daily download quota for that app is exhausted (per report type × app × UTC day — §9) | Don't retry today — it will fail again. Wait for 03:00 Europe/Riga (00:00 UTC), then re-run just the failed window(s) with `backfill --start-date/--end-date`. |
| WARNING `clamping <report> ... before the N-day retention floor` / `skipping <report> ... entirely before` | The requested window reaches past that report's availability window (31 days in-app events, 60 installs — §9) | Expected for a no-args `backfill`. Nothing to fix; the data simply no longer exists at the source. |
| `PipelineError: refusing to wipe populated window ... fetched 0 rows but N already loaded` (a FAILED window, exit 1) | AppsFlyer returned a valid-but-empty report for a window we hold rows for — an upstream anomaly, or an availability-floor edge (issue #45) | Nothing was deleted. Re-run the window later; if the source has genuinely gone to zero for that window and you want ours to match, that is a deliberate manual `DELETE` (or `load_events(..., allow_wipe=True)` from a Python shell), not a pipeline re-run. |
| `AppsFlyerAPIError: ... empty response body` or `TransformError: ... missing expected column(s)` on a window that used to load fine | AppsFlyer sent an anomalous 200 (truly empty or error-text body), or the export's header set drifted — a legitimate empty report always carries the full CSV header row (issue #26, live-verified 2026-07-09) | Nothing was deleted — the window's previously loaded rows are intact. Re-run just that window with `--dry-run` to inspect; if AppsFlyer renamed columns, update `reports._IN_APP_EVENTS_COLUMN_MAP`; otherwise re-run the window once the upstream anomaly clears. |
| Job killed / times out | `TimeoutStartSec` too low for a slow AppsFlyer day | 7200s for daily (8 full-mode windows × the retry policy's ~10 min worst case ≈ 83 min, plus headroom — issue #35) / 10800s for backfill in the §9 example; raise further if needed. |
| `SIGSYS` or crash right at startup | A hardening directive is too tight | Comment out `MemoryDenyWriteExecute` if enabled, then loosen `SystemCallFilter`; `daemon-reload` and retry. |
| `status=218/CAPABILITIES`, "Failed to drop capabilities" (user-level unit, §14) | `ProtectClock`/`ProtectKernelModules`/`ProtectKernelLogs` in a `systemd --user` unit on a host that forbids unprivileged user namespaces (Ubuntu 24.04 ships `kernel.apparmor_restrict_unprivileged_userns=1`) — hit live on the first scheduled fire, issue #19 | Remove those three directives from the user-level unit only (they're security no-ops without root anyway; the root-based unit keeps them). Re-copy to `~/.config/systemd/user/`, `systemctl --user daemon-reload`, then `systemctl --user start appsflyer-daily.service` once to confirm and to load the day the failed fire missed. |

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

> **Check §5's upgrade note before pulling.** A release that adds a *required* env var breaks every
> invocation until the EnvironmentFile has it — BAF-11 stage 4's `DB_TABLE_INSTALLS` is the current
> instance. Update the EnvironmentFile first, then `git pull`.

```bash
sudo -u appsflyer env HOME=/opt/appsflyer bash -c '
  cd /opt/appsflyer/appsflyer-to-analytics-db && git pull && uv sync --frozen --no-dev'
```
If the unit files themselves changed, re-run §7's `install` + `daemon-reload`. No explicit "restart"
is needed otherwise — it's a oneshot; the next timer fire automatically uses the updated venv.

## 14. No-root stopgap deployment (what's actually live right now)

`<deploy-user>` has no sudo on the target server as of 2026-07-07 — `sudo -n true` prompts for a
password, and the account isn't in the `sudo` group (a request to be added is in with the backend
team). §§1-13 above need root for the dedicated system user, `/etc/systemd/system/`, and
`/etc/appsflyer/`. Until that's granted, the pipeline runs as a **systemd `--user` (per-user manager)**
deployment instead — functionally equivalent, but scoped entirely to `<deploy-user>`'s own account
with no root anywhere. This is what's actually installed and running today.

**What differs from §§1-13:**

| | Root-based (§§1-13) | No-root stopgap (this section) |
|---|---|---|
| Runs as | dedicated `appsflyer` system user | `<deploy-user>` (existing personal account) |
| Install dir | `/opt/appsflyer/appsflyer-to-analytics-db` | `~/GitHubRepos/appsflyer-to-analytics-db` |
| Secrets file | `/etc/appsflyer/appsflyer.env` (mode 600) | `~/appsflyer-secrets/appsflyer.env` (mode 600, **outside** the git working directory — never inside the repo clone, to rule out an accidental `git add -A` ever sweeping real credentials into this public repo) |
| Units live in | `/etc/systemd/system/`, managed by the system manager | `~/.config/systemd/user/`, managed by `systemctl --user` |
| Unit files | `deploy/appsflyer-daily.{service,timer}` | `deploy/user-level/appsflyer-daily.{service,timer}` |
| Runs without login? | always (system manager) | only with `loginctl enable-linger <deploy-user>` (done — see below; this itself needed no root) |
| Hardening | full set incl. `ProtectHome=true`/`ProtectSystem=strict` | reduced — `ProtectHome`/`ProtectSystem=strict` are dropped, since they'd hide `/home` entirely, which is where the repo/venv/secrets all live for a per-user deployment |

**Install steps actually run** (uv installed user-locally, no root needed anywhere):
```bash
git clone https://github.com/baubek-yesim/appsflyer-to-analytics-db.git ~/GitHubRepos/appsflyer-to-analytics-db
curl -LsSf https://astral.sh/uv/install.sh | sh          # installs to ~/.local/bin, no root
cd ~/GitHubRepos/appsflyer-to-analytics-db && ~/.local/bin/uv sync --frozen --no-dev

mkdir -p ~/appsflyer-secrets && chmod 700 ~/appsflyer-secrets
# .env copied in via scp from a machine that already had it configured, then:
chmod 600 ~/appsflyer-secrets/appsflyer.env

mkdir -p ~/.config/systemd/user
# deploy/user-level/appsflyer-daily.{service,timer} and appsflyer-alert@.service copied to
# ~/.config/systemd/user/
systemctl --user daemon-reload

loginctl enable-linger "$(whoami)"   # exit 0, no root needed -- self-linger is polkit-allowed here
systemctl --user enable --now appsflyer-daily.timer
```

**Verified live through this exact path** (2026-07-07): `check-connection` via a transient
`systemd-run --user` unit connected successfully and confirmed the table has 1,397 rows;
`daily --dry-run` via the same mechanism correctly parsed the `EnvironmentFile` (including the
space in `APPSFLYER_MEDIA_SOURCE=Facebook Ads` and the comma-separated `APPSFLYER_APP_IDS`) and made
real AppsFlyer API calls. 2 of 4 windows in that dry-run hit AppsFlyer's daily per-app quota (see the
refined note in §9 — heavy same-day testing across dry-runs, the real backfill, and this verification
had already spent most of that quota for 2 of the 4 app/attribution combos); the other 2 succeeded
end-to-end. `appsflyer-daily.service` itself was never manually started (to avoid spending more of an
already-thin quota) — its **first real fire is the scheduled one**, `2026-07-08` around `05:00`
server-local time (confirmed via `systemctl --user list-timers`).

> **Verification gap, learned the hard way (issue #19):** `systemd-run` transient preflights like the
> ones below set only `WorkingDirectory`/`EnvironmentFile` — they do **not** carry the installed unit
> file's hardening directives, so they can pass while the real unit is unstartable. The first scheduled
> fire (2026-07-08) failed with `218/CAPABILITIES` on directives no transient check had ever exercised.
> Always finish verification by starting the real service once (`systemctl --user start
> appsflyer-daily.service`) — it's idempotent and doubles as the §8 smoke test.

**Preflight/verification commands** — identical to §6/§8's `systemd-run` pattern, minus `--property=User=`/
`Group=` (meaningless for a user-manager unit) and with the no-root paths substituted:
```bash
systemd-run --user --wait --collect --unit=appsflyer-preflight \
  --property=WorkingDirectory=$HOME/GitHubRepos/appsflyer-to-analytics-db \
  --property=EnvironmentFile=$HOME/appsflyer-secrets/appsflyer.env \
  --property=StandardOutput=journal \
  $HOME/GitHubRepos/appsflyer-to-analytics-db/.venv/bin/appsflyer-pipeline check-connection
journalctl --user -u appsflyer-preflight.service -o cat --no-pager
```
Monitoring: `systemctl --user list-timers appsflyer-daily.timer`, `journalctl --user -u
appsflyer-daily.service`. Rollback: `systemctl --user disable --now appsflyer-daily.timer`.

**Migrating to the root-based setup once sudo lands:** install per §§2-7 as normal (dedicated
`appsflyer` system user, `/opt/appsflyer/...`, `/etc/appsflyer/...`), verify it end-to-end exactly like
this section did, *then* tear down the stopgap so the job isn't running twice:
```bash
systemctl --user disable --now appsflyer-daily.timer
loginctl disable-linger "$(whoami)"
rm -rf ~/.config/systemd/user/appsflyer-daily.{service,timer} ~/appsflyer-secrets
# ~/GitHubRepos/appsflyer-to-analytics-db can stay as a dev clone, or be removed too
```


## 15. BAF-11 cutover — from Facebook-purchases-only to the full raw export

Applies to the live no-root deployment (§14); substitute the root paths from §§4-7 if that has
migrated. Every step that writes to production or spends quota is marked **[write]**. The whole
procedure spends **at most 3 report downloads per (app × report type) on any one day** (§9), so it
cannot trip the quota unless someone also runs manual pulls the same day — ask Mark not to run his
Pull API scripts on days D..D+2 (UI exports are fine, separate quota).

### Day D — deploy the new code with the OLD behavior ("parallel", Mark's condition)

0. **Capture the pre-cutover baseline and audit the live state first — do this before touching
   anything.** Record the deployed commit, 60 days of `journalctl` history, whether the filter keys
   are present in the `EnvironmentFile` (names/presence only, never values, in anything that reaches
   this public repo), `SHOW INDEX FROM appsflyer_events_fb`, and the per-day Facebook-purchase
   count/revenue baseline that step 10 below needs and cannot reconstruct afterwards (the table is the
   only copy of anything older than the availability floor). Commit the write-up as a dated file under
   `docs/`; keep the baseline's row-level detail out of the public repo (commercially sensitive) —
   store it under `~/appsflyer-secrets/` instead, per §14's existing convention for keeping sensitive
   material outside the git working directory. See `docs/2026-09-08-production-audit.md` for the
   template and the 2026-09-08 run's results (which also found: production was *already* on `main`
   HEAD by that date, and `appsflyer_events_fb` has **no PRIMARY KEY and no index at all** — settling
   the Этап 7 dispute against issue #14's "verified live" comment).
1. **[write]** Add the one new required key to `~/appsflyer-secrets/appsflyer.env` **before**
   pulling (§5): `DB_TABLE_INSTALLS=appsflyer_installs_fb`. Leave the two filter lines and
   everything else as they are.
2. `cd ~/GitHubRepos/appsflyer-to-analytics-db && git pull && ~/.local/bin/uv sync --frozen --no-dev`
3. Preflight through systemd (§14 pattern, quota 0): `check-connection` → two table lines, the
   installs one "does not exist yet".
4. **[write, DDL]** `create-table` through the same pattern → creates `appsflyer_installs_fb`
   (130 columns, `idx_app_attr_install`). Idempotent.
5. **[write, DDL]** Этап 7: run `sql/migrations/2026-07-08-add-id-pk-and-index.sql` once against
   `appsflyer_events_fb`. **Confirmed necessary as of 2026-09-08** — `SHOW INDEX` returned empty and
   `SHOW CREATE TABLE` has no `id`/PRIMARY KEY/index clause at all; the 2026-07-10 recreation did drop
   them, and issue #14's 2026-07-08 "verified live" comment no longer reflects reality. `power_bi_user`
   holds `INDEX, ALTER` (re-verified 2026-09-08 via `SHOW GRANTS`). Verify:
   `SHOW INDEX FROM appsflyer_events_fb` lists `idx_app_attr_time`.
6. Install the updated unit (`TimeoutStartSec=7200` — already live as of 2026-09-08, so this step may
   already be a no-op; check the installed unit file first):
   `cp deploy/user-level/appsflyer-daily.service ~/.config/systemd/user/ && systemctl --user daemon-reload`
7. **Finish with one real `systemctl --user start appsflyer-daily.service`.** This resolves the
   apparent conflict with §14's rule (`docs/RUNBOOK.md` §14, "verification gap" callout below step 14):
   after any unit-file change, a transient `systemd-run` preflight does **not** exercise the installed
   unit's hardening directives, so it can pass while the real unit is unstartable (issue #19's
   `218/CAPABILITIES` incident happened exactly this way, on an unattended scheduled fire). §14's rule
   takes precedence here — do not defer the first real start to the next scheduled fire. Expect
   `4/4 windows OK` and old-filter behavior (no `filters:` WARNING line — the filter keys are already
   set per step 0's audit). Quota spent: the usual 1 per combo.

### Day D+1 — flip, then backfill once

8. Confirm step 7's run was clean. Then **[write]** edit the env file:
   - comment out `APPSFLYER_MEDIA_SOURCE` and `APPSFLYER_EVENT_NAMES` (absent = no filter; a
     present-but-blank line is a startup error — §5);
   - add `APPSFLYER_ENABLED_REPORTS=in_app_events_non_organic,in_app_events_retargeting,installs_non_organic,installs_retargeting`;
   - add `APPSFLYER_DAILY_LOOKBACK_DAYS=3` (issue #8; zero extra quota).
9. **[write + quota]** Exactly **one** real `backfill`, **no `--dry-run` first** (§9's command with
   `--user`, `TimeoutStartSec=10800`). No date arguments: the clamp yields 31 days of in-app events
   and 60 of installs by itself (two `clamping` WARNINGs are expected). Mark's ordering rule
   ("Meta first, then everything") is satisfied by construction — the Facebook rows for those 31
   days are already in the table, and each window is replaced by its superset. Expect **12/12**
   windows; a `refusing to wipe` FAILED window means the source returned empty for a day we hold —
   stop and look, don't re-run.
10. Verify with read-only SQL (`power_bi_user`, `SELECT` only):
    - per `DATE(event_time)` in the last 31 days, `COUNT(*) WHERE media_source='Facebook Ads' AND
      event_name IN ('af_purchase','af_purchase_YC')` is **≥** the same count taken before step 9
      (the full export cannot contain fewer Facebook purchases than the filtered one did);
    - `media_source` now has many values (googleadwords_int, any_source, Email, …) and
      `event_name` includes `screen_*`/`alert_*`;
    - `SELECT COUNT(*) FROM appsflyer_installs_fb GROUP BY app_id, attribution_type` — non-zero for
      both apps; `com.yesimmobile` on the order of 1.5-2k installs and ~0.9k retargeting per day
      (2026-08-13 measurement);
    - `journalctl --user -u appsflyer-backfill` has no `wiped` and no `refusing to wipe`.
11. If a window failed on quota (HTTP 400): wait for 03:00 Riga, re-run **only** that window with
    `backfill --start-date/--end-date`.

### Day D+2 — first scheduled run in full mode

12. `journalctl --user -u appsflyer-daily.service --since today`: `8/8 windows OK`, run time well
    under 5 minutes, `deleted>0 inserted>0` for the three lookback days (that is the lookback
    re-pull, not a wipe), no WARNING other than the dedupe counters. Two clean days = stable.
13. Report in BAF-11 (in Russian; see the 2026-09-07 plan for the content): what runs where, the
    31/60-day availability model and its consequence for the "before/after cutover" gap in the
    table, the quota rules for manual pulls, the two dedupe decisions for sign-off, and a request
    to spot-check one day against a UI export.

### Rollback

- After step 6: `git checkout 89a1b91 && ~/.local/bin/uv sync --frozen --no-dev`, remove
  `DB_TABLE_INSTALLS` from the env file → the pre-BAF-11 behavior. (Any commit before stage 4
  works; 89a1b91 is what ran until 2026-09.)
- After step 8: restore the two filter lines and remove `APPSFLYER_ENABLED_REPORTS` → the next
  fire pulls Facebook purchases only again. Rows already loaded in full mode stay; the next runs
  replace only the lookback window with the filtered subset — a deliberate loss of the non-Facebook
  rows for those days, acceptable only as a conscious rollback.
- The installs table is new and unread by anyone; drop it only by explicit decision.
