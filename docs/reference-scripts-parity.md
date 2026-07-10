# Parity verification: pipeline vs. the BAF-2 reference scripts

**Date:** 2026-07-09 · **Pipeline state:** `main` @ `4f7430a` · **Verified by:** line-by-line
behavior extraction of both reference scripts, an independent adversarial review pass, and
read-only production probes. This is the definitive answer to "does the pipeline implement all
the core logic of the two reference scripts?" — re-verify only if the scripts or the client/
transform/pipeline modules change materially.

The reference scripts (`af_events_meta_allday.py`, `af_events_meta_1day.py`, Mark Malovichko,
BAF-2 comment 62293; kept local-only in gitignored `reference/mark-scripts/`) are ~90/~120-line
pandas+requests scripts that fetch both Pull API reports and assemble a DataFrame — the DB-save
block was explicitly left for this pipeline to add.

## Verdict

**Yes — every core behavior is implemented.** Wire-level and window-arithmetic parity is exact;
one cosmetic header is deliberately omitted (documented below); every other departure is a
deliberate, documented improvement. In several places the pipeline is *more* faithful to the
source data than the scripts themselves (see "Where the scripts would corrupt data").

## Exact parity (verified byte/date-level where applicable)

| Behavior | Both do |
|---|---|
| URL | `https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/{endpoint}/v5` |
| Endpoints | `in_app_events_report` (non_organic) / `in-app-events-retarget` (retargeting) |
| Query params | `from`, `to`, `event_name=af_purchase,af_purchase_YC`, `media_source=Facebook Ads` — identical wire encoding verified. Deliberate addition since: optional `timezone` (issue #53), which the scripts never send — their pulls (and this pipeline's, when unset) return UTC times, 3h behind the analytics team's Europe/Riga references |
| Auth/headers | `Authorization: Bearer <token>`, `Accept: text/csv` |
| Timeout | 120 s per request |
| Redirects | followed (requests: default; httpx: explicit `follow_redirects=True`) |
| Backfill window | end = yesterday (local date), start = end − 89 → 90 days inclusive |
| Daily window | from = to = yesterday (pipeline default `APPSFLYER_DAILY_LOOKBACK_DAYS=1`) |
| Chunk walk | inclusive boundaries, next chunk starts at prev end + 1 day |
| Loop order | app_id → [non_organic, retargeting] → chunks |
| Target columns | the same 17 (15 mapped + `attribution_type` + `app_id`); scripts' lowercase/underscore normalization of the 15 raw headers maps 1:1 to the pipeline's explicit `_COLUMN_MAP` |

Chunk size differs — scripts 30 days, pipeline 31 (`MAX_CHUNK_DAYS`, the API's per-call cap per
the same BAF-2 comment). Verified: identical date-range union, identical request count (3 per
app/source for 90 days), only the seam dates differ. Not data-affecting.

## The one true omission (deliberate as of this document)

**`User-Agent` browser spoof.** The scripts send a Chrome 120 UA string; the pipeline sends
httpx's default (`python-httpx/x.y`). Live evidence across every production run (Stage 3–7
backfills, scheduled dailies, probes) shows the API does not care. Omitted on purpose — an
honest client identity beats a spoof — but if AppsFlyer's edge ever starts gating non-browser
UAs, this is the first thing to try, and this paragraph is the breadcrumb.

## Deliberate divergences (all documented at their code sites)

- **Errors fail the window loudly instead of becoming empty data.** The scripts turn any non-200,
  network error, or unparseable/empty body into an empty DataFrame and exit 0. The pipeline
  retries 429/5xx/transport, fails fast on 4xx, isolates the failure to its window, preserves
  previously loaded rows, and exits 1 (issues #11, #26; systemd OnFailure alerting). With a DB
  attached, the scripts' semantics would have been the delete-then-insert-nothing wipe bug.
- **Schema drift raises instead of null-padding.** Scripts fill missing columns with `None` and
  keep loading; the pipeline raises `TransformError` (design-spec risk table; `transform.py`
  module docstring).
- **Dual-attribution filter** (issue #7): non_organic rows with `Is Primary Attribution=false`
  are dropped. The scripts load them (verified 2026-07-08 during the #7 work). This is the
  largest systematic data delta vs. the scripts and is the subject of the open policy decision
  with the schema owner (issue #46 / BAF-2 thread).
- **Exact-duplicate collapse + conflict guard** (issue #23) — implements the dedup key Mark
  himself specified after writing the scripts (BAF-2 comment 62585).
- **Defensive client-side re-filter** on `Media Source`/`Event Name`. The scripts trust the API
  params. Corner: a drifted value (casing/whitespace/privacy-masked) would be silently dropped by
  the pipeline where the scripts would keep it — accepted; revisit with #46's outcome.
- **Config from environment** (validated, #9/#29) instead of hardcoded constants; secrets never
  in code. The scripts also carry two commented-out future app IDs — `id6753973280`,
  `id1525236866` — recorded here because the pipeline's `APPSFLYER_APP_IDS` env list is the only
  other place the app roster lives; enabling them is a config edit, no code change.
- **The entire DB layer is pipeline-only** (idempotent per-window delete-then-insert, preflight,
  wipe visibility, 1M-row cap guard, CLI exit codes, tests, deploy) — the scripts print a
  DataFrame.

## Where the scripts would corrupt data (pipeline is more faithful)

- pandas type inference (`low_memory=False`) turns all-numeric 17-digit Meta
  `campaign_id`/`adset_id`/`ad_id` values into int64/float64 — mangling them (e.g. `1.20E+17`,
  exactly as seen in exported sheets); revenue becomes float64. The pipeline reads everything as
  strings (`infer_schema_length=0`) and types explicitly: exact string IDs, `Decimal` revenue,
  strictly parsed timestamps.
- pandas coerces literal `"NA"`/`"NULL"`/`"None"`/`"N/A"`/`"nan"` strings to NULL; polars keeps
  them as text. **Verified unrealized in production** (2026-07-09: zero such literals across all
  nine nullable string columns, 3,402 rows) — AppsFlyer emits genuinely empty fields for these
  apps. If a future audit finds such literals, decide normalization then.

## Method note

Behaviors extracted per script line; each classified parity / superset / deliberate divergence /
missing against `appsflyer_client.py`, `transform.py`, `pipeline.py`, `loader.py`, `config.py`,
`cli.py`; an independent adversarial reviewer repeated the exercise with instructions to disprove
parity; disagreements reconciled against live evidence (wire-encoding checks, date-math
equivalence runs, production column scans).
