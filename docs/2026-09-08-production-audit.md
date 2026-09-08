# 2026-09-08 production audit — pre-cutover baseline (RUNBOOK §15 "step 0")

Read-only observation pass against the live no-root deployment (`docs/RUNBOOK.md` §14) before
executing any of §15's Day D/D+1/D+2 steps. Nothing was written to the database, no unit was
restarted, no AppsFlyer API call was made. Real host/access details are kept out of this public repo
per §0's existing convention — ask whoever owns BAF-2 for them.

## A. Server state

- **Deployed commit:** `98d8074065673f492a38aeb2a12f45cae5daddf8` (`main` HEAD, merge of PR #63,
  authored 2026-09-07 14:14:47 +0500) — `git status --porcelain` clean. This **overturns** the
  previously assumed deploy commit (`89a1b91`, believed live as of 2026-08-13): `main` had in fact
  already been pulled and the venv rebuilt on the server by the time of this audit. **This also
  overturns the "prod is 57 commits behind" framing used to scope this task** — the gap is now zero at
  the code level; what remains open is the *behavioral* cutover (§15 D+1/D+2), not the deploy.
- **Timer:** `appsflyer-daily.timer` enabled, last fire `2026-09-08 05:02:03` (server-local, exit
  clean — `systemctl --user is-failed appsflyer-daily.service` → `inactive`, i.e. not failed), next
  fire `2026-09-09 05:04:42`.
- **Unit file** (`~/.config/systemd/user/appsflyer-daily.service`): `TimeoutStartSec=7200` —
  issue #35's fix **is live** on the box, not just in the repo. `OnFailure=appsflyer-alert@%n.service`
  present as documented (issue #30's naming concern was not re-verified here — out of scope for this
  audit; still open).
- **`EnvironmentFile` key names** (values were never read or transcribed — only presence/absence,
  per this task's own redaction rule for a public repo):

  | Key | State |
  |---|---|
  | `APPSFLYER_MEDIA_SOURCE` | present |
  | `APPSFLYER_EVENT_NAMES` | present |
  | `APPSFLYER_TIMEZONE` | present |
  | `APPSFLYER_DAILY_LOOKBACK_DAYS` | absent (defaults to 1) |
  | `DB_TABLE_INSTALLS` | **present** |
  | `APPSFLYER_ENABLED_REPORTS` | absent (defaults to the two in-app-events reports only) |
  | `APPSFLYER_CHUNK_DAYS` | absent (defaults to 31) |

  Also present: `APPSFLYER_API_TOKEN`, `APPSFLYER_APP_IDS`, `DB_HOST`, `DB_NAME`, `DB_PASSWORD`,
  `DB_PORT`, `DB_TABLE`, `DB_USER`.

  **This closes the single most dangerous open question going into §15:** the two filter keys are
  present, so §15 step 2's `git pull` does **not** silently flip the timer to the unfiltered export —
  main's flipped defaults (`config.py:109-110`, `None` = no filter) never take effect here because the
  keys are explicitly set. `DB_TABLE_INSTALLS` being present already means §15 step 1 is done.
- **Probe leftovers:** `/tmp/baf11-probe/` does not exist — already cleaned up (Этап 0's raw CSVs with
  IP/Advertising ID/Customer User ID are gone).
- **60-day journal** (`journalctl --user -u appsflyer-daily.service --since -60d`, 2026-08-28 through
  today): every fire is `4/4 windows OK`, no `refusing to wipe`, no `wiped`, no `Traceback`/`FAILED`.
  One expected `WARNING` (2026-08-31, `com.yesimmobile`/`non_organic`): a kept conflicting dedup-key
  row, per the documented Этап 8(a) behavior. Starting with the **2026-09-08 05:02** fire, log lines
  gained a `report=in_app_events` field absent from every prior fire back to 08-28 — direct evidence
  that the code deployed between the 09-07 and 09-08 fires and ran clean on its first live execution.

## B. Database state (read-only, `power_bi_user`)

- **Grants:** `power_bi_user` (via `power_bi_role` plus direct grants) holds
  `SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX, ALTER` on `analytics_statistics.*` — i.e. it
  is **not** restricted to `SELECT`; the "read-only" label used elsewhere in the docs describes how
  we've chosen to *use* this account for verification, not a database-enforced restriction. Only
  `SHOW`/`SELECT` statements were executed in this audit. This also settles the grant question
  Этап 7 raised (`docs/superpowers/plans/2026-08-13-baf-11-full-raw-export.md:577`): `ALTER`/`INDEX`
  are present.
- **`SHOW INDEX FROM appsflyer_events_fb`** → **empty result. No indexes, no PRIMARY KEY.**
  `SHOW CREATE TABLE appsflyer_events_fb` confirms: 17 columns, no `id` column, no `PRIMARY KEY`
  clause, no `INDEX` clause, `ENGINE=InnoDB`. **This resolves the Этап 7 / issue #14 dispute in favor
  of `docs/RUNBOOK.md`/`sql/create_table.sql`: the 2026-07-10 recreation did drop the PK/index, and
  issue #14's 2026-07-08 "verified live" comment describes a state that no longer holds.** §15 step 5
  (`sql/migrations/2026-07-08-add-id-pk-and-index.sql`) is required, not optional, before any
  full-mode backfill widens the DELETE-by-window volume.
- **`SHOW TABLES LIKE 'appsflyer_installs_fb'`** → empty. The installs table **does not exist yet** —
  §15 step 4 (`create-table`) has not been run, even though `DB_TABLE_INSTALLS` is already set in the
  environment (harmless today, since `APPSFLYER_ENABLED_REPORTS` doesn't request installs).
- **Overall table shape:** 12,132 rows, `event_time` spanning 2026-05-31 → 2026-09-07,
  **`COUNT(DISTINCT media_source) = 1`**, 2 distinct `app_id`s. The table is still exclusively
  Facebook-Ads-purchases, confirming §15 D+1 (the filter flip) has **not** happened — only Day D
  (deploy with old behavior) has.
- **Pre-cutover baseline** (§15 step 10's comparison point, which no step currently instructs anyone
  to capture): per `(DATE(event_time), app_id, attribution_type)` over the trailing 35 days, filtered
  to `media_source='Facebook Ads' AND event_name IN ('af_purchase','af_purchase_YC')` —
  **4,492 rows, $56,882.85 revenue, across 106 date×app×attribution_type groups**
  (self-consistency verified: `SUM(rows_cnt)` over the grouped query equals the ungrouped
  `COUNT(*)`). **Revenue and per-day breakdowns are commercially sensitive and are not committed to
  this public repo.** Full detail lives at `~/appsflyer-secrets/baseline-2026-09-08.tsv` (mode 600,
  outside the git working directory per §14's existing convention) and a matching local copy held by
  whoever ran this audit. When §15 step 10 runs, compare its post-cutover per-day Facebook-purchase
  counts against this file — the post-cutover count must be **≥** the pre-cutover count for every
  overlapping day.
- **Dedup-conflict check** (coarse key — `app_id, attribution_type, event_time, event_name,
  appsflyer_id`, without `Event Value`, over the trailing 35 days): **2 conflicting groups.** Given the
  one `WARNING` observed in the same window (08-31, a deliberately-kept conflict per Этап 8(a)), this
  is consistent with by-design retained conflicts rather than evidence that
  `sql/migrations/2026-08-14-dedupe-keep-latest-install-time.sql` was never applied — but this audit
  did **not** independently confirm the migration was run, only that the current row count of
  coarse-key conflicts is small and explainable by known WARNINGs.

## What this changes about the state assessed on 2026-09-08 (workflow audit, same day)

- Production is **not** 57 commits behind `main` — it is caught up. §15 steps 1-2 (add
  `DB_TABLE_INSTALLS`, `git pull` + `uv sync`) have already happened; the risk framed as "a bare
  `git pull` could silently flip the timer to unfiltered" did not materialize, because the filter keys
  were already present before the pull.
- §15 step 5 (PK/index ALTER) is now a confirmed **required** step, not a disputed one — the table has
  zero indexes.
- §15 step 4 (installs `create-table`) is still outstanding.
- The still-open items from the earlier synthesis are unaffected by this audit: the §14/§15
  contradiction on the manual-start step, the §15-step-10 baseline (now captured here), Mark's
  go-ahead for full-mode collection (gates step 9, not addressed here), and the short-fetch half of
  issue #45.

## Next step

§15 Day D (steps 3, 4, 6, 7 remain — installs `create-table`, the PK/index ALTER now confirmed
necessary, the unit reinstall/start decision) can proceed; see `docs/RUNBOOK.md` §15 step 0 (added
alongside this file) for the updated entry point.
