-- One-off dedupe rewrite of `appsflyer_events_fb` (requested by data analytics, 2026-08-14).
--
-- Rule: within the dedup key, keep exactly ONE row -- the one with the latest
-- `install_time`. Replaces the pipeline's current behaviour, which keeps every
-- conflicting row and logs a WARNING (transform.py `_dedupe_rows`).
--
-- Run top to bottom. Steps 0-2 are read-only; step 5 is the only destructive one
-- and it preserves the original table under `_bak_20260814` (step 7 rolls back).
--
-- BEFORE RUNNING: stop the scheduled job, otherwise rows inserted between step 3
-- and step 5 land in the old table and are lost by the swap:
--     systemctl --user stop appsflyer-daily.timer
-- and re-enable it after step 6:
--     systemctl --user start appsflyer-daily.timer
--
-- NOTE: this rewrite does NOT stick. `transform._dedupe_rows` still keeps both
-- conflicting rows, and `loader.load_events` re-inserts each window wholesale, so
-- the next `daily`/`backfill` run over an overlapping window reintroduces them.
-- Making the rule permanent means changing `_dedupe_rows` to match -- separate change.


-- ---------------------------------------------------------------------------
-- Step 0. Schema check: does the production table carry `id` / PRIMARY KEY?
-- ---------------------------------------------------------------------------
-- sql/create_table.sql:10-13 warns the table recreated on 2026-07-10 may lack
-- them. The answer decides which column list to use in step 4 and whether the
-- `id DESC` tiebreaker is available.

SHOW CREATE TABLE `appsflyer_events_fb`;


-- ---------------------------------------------------------------------------
-- Step 1. Baseline -- record this output, it is the completeness reference.
-- ---------------------------------------------------------------------------

SELECT
    COUNT(*)                  AS total_rows,
    COUNT(DISTINCT app_id)    AS apps,
    MIN(event_time)           AS first_event,
    MAX(event_time)           AS last_event,
    SUM(event_revenue)        AS total_revenue
FROM `appsflyer_events_fb`;

SELECT
    app_id,
    attribution_type,
    COUNT(*)           AS rows_,
    SUM(event_revenue) AS revenue
FROM `appsflyer_events_fb`
GROUP BY app_id, attribution_type
ORDER BY app_id, attribution_type;


-- ---------------------------------------------------------------------------
-- Step 2. Impact -- how many rows the rule drops, under each candidate key.
-- ---------------------------------------------------------------------------
-- `dropped_3col` is the key chosen for this run. `dropped_5col` is the same rule
-- with app_id/attribution_type in the partition. The DIFFERENCE between them is
-- the dual-attribution twin purchases (~14/month, issues #7/#46/#47) that the
-- 3-column key collapses -- rows accepted by design on 2026-07-09.

SELECT
    COUNT(*)                                                   AS total,
    COUNT(*) - COUNT(DISTINCT event_time, event_name, appsflyer_id)
                                                               AS dropped_3col,
    COUNT(*) - COUNT(DISTINCT app_id, attribution_type, event_time, event_name, appsflyer_id)
                                                               AS dropped_5col
FROM `appsflyer_events_fb`;

-- Does `install_time` actually discriminate inside a conflict? Where
-- distinct_install_times = 1 the ORDER BY is a coin flip and the surviving row
-- is arbitrary -- this is the 3-vs-4-EUR same-second case (design-spec.md:134).
SELECT
    event_time, event_name, appsflyer_id, attribution_type,
    COUNT(*)                        AS rows_in_key,
    COUNT(DISTINCT install_time)    AS distinct_install_times,
    COUNT(DISTINCT event_revenue)   AS distinct_revenues,
    SUM(event_revenue)              AS revenue_in_key
FROM `appsflyer_events_fb`
GROUP BY event_time, event_name, appsflyer_id, attribution_type
HAVING COUNT(*) > 1
ORDER BY event_time DESC;


-- ---------------------------------------------------------------------------
-- Step 3. Build the replacement table (empty clone -- keeps PK, indexes, charset).
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS `appsflyer_events_fb_new`;
CREATE TABLE `appsflyer_events_fb_new` LIKE `appsflyer_events_fb`;


-- ---------------------------------------------------------------------------
-- Step 4. Fill it with the winners (rn = 1).
-- ---------------------------------------------------------------------------
-- Use variant A if step 0 showed an `id` column, variant B if it did not.
--
-- Tiebreaker matters: `install_time DESC` alone is not deterministic, because
-- rows sharing an appsflyer_id share an install_time. Variant A breaks the tie on
-- `id DESC` (last row as delivered by AppsFlyer -- no revenue bias). Variant B has
-- no row identity to fall back on and breaks the tie on `event_revenue DESC`,
-- which biases total revenue UPWARD on every tied pair. Swap to ASC to bias down.

-- ---- Variant A: table HAS `id` -------------------------------------------
INSERT INTO `appsflyer_events_fb_new` (
    `id`, `event_time`, `install_time`, `attributed_touch_time`, `event_name`,
    `event_revenue`, `media_source`, `channel`, `campaign`, `campaign_id`,
    `adset`, `adset_id`, `ad`, `ad_id`, `appsflyer_id`, `customer_user_id`,
    `attribution_type`, `app_id`
)
SELECT
    `id`, `event_time`, `install_time`, `attributed_touch_time`, `event_name`,
    `event_revenue`, `media_source`, `channel`, `campaign`, `campaign_id`,
    `adset`, `adset_id`, `ad`, `ad_id`, `appsflyer_id`, `customer_user_id`,
    `attribution_type`, `app_id`
FROM (
    SELECT t.*,
           ROW_NUMBER() OVER (
               -- To keep dual-attribution twins, add: app_id, attribution_type,
               PARTITION BY `event_time`, `event_name`, `appsflyer_id`
               ORDER BY `install_time` DESC, `id` DESC
           ) AS rn
    FROM `appsflyer_events_fb` t
) ranked_events
WHERE rn = 1;

-- ---- Variant B: table has NO `id` ----------------------------------------
-- INSERT INTO `appsflyer_events_fb_new` (
--     `event_time`, `install_time`, `attributed_touch_time`, `event_name`,
--     `event_revenue`, `media_source`, `channel`, `campaign`, `campaign_id`,
--     `adset`, `adset_id`, `ad`, `ad_id`, `appsflyer_id`, `customer_user_id`,
--     `attribution_type`, `app_id`
-- )
-- SELECT
--     `event_time`, `install_time`, `attributed_touch_time`, `event_name`,
--     `event_revenue`, `media_source`, `channel`, `campaign`, `campaign_id`,
--     `adset`, `adset_id`, `ad`, `ad_id`, `appsflyer_id`, `customer_user_id`,
--     `attribution_type`, `app_id`
-- FROM (
--     SELECT t.*,
--            ROW_NUMBER() OVER (
--                PARTITION BY `event_time`, `event_name`, `appsflyer_id`
--                ORDER BY `install_time` DESC, `event_revenue` DESC
--            ) AS rn
--     FROM `appsflyer_events_fb` t
-- ) ranked_events
-- WHERE rn = 1;


-- ---------------------------------------------------------------------------
-- Step 5. Verify the new table BEFORE swapping. Do not proceed on a surprise.
-- ---------------------------------------------------------------------------
-- Expected: new_rows = total_rows - dropped_3col (step 2), and every remaining
-- key holds exactly one row.

SELECT
    COUNT(*)           AS new_rows,
    MIN(event_time)    AS first_event,
    MAX(event_time)    AS last_event,
    SUM(event_revenue) AS total_revenue
FROM `appsflyer_events_fb_new`;

SELECT
    app_id,
    attribution_type,
    COUNT(*)           AS rows_,
    SUM(event_revenue) AS revenue
FROM `appsflyer_events_fb_new`
GROUP BY app_id, attribution_type
ORDER BY app_id, attribution_type;

-- Must return zero rows.
SELECT event_time, event_name, appsflyer_id, COUNT(*)
FROM `appsflyer_events_fb_new`
GROUP BY event_time, event_name, appsflyer_id
HAVING COUNT(*) > 1;


-- ---------------------------------------------------------------------------
-- Step 6. Swap. Atomic in a single RENAME statement; original kept as _bak.
-- ---------------------------------------------------------------------------

RENAME TABLE `appsflyer_events_fb`     TO `appsflyer_events_fb_bak_20260814`,
             `appsflyer_events_fb_new` TO `appsflyer_events_fb`;


-- ---------------------------------------------------------------------------
-- Step 7. Rollback, if the completeness check fails.
-- ---------------------------------------------------------------------------

-- RENAME TABLE `appsflyer_events_fb`               TO `appsflyer_events_fb_deduped`,
--              `appsflyer_events_fb_bak_20260814`  TO `appsflyer_events_fb`;

-- Drop the backup only once analytics has signed off on the new table:
-- DROP TABLE `appsflyer_events_fb_bak_20260814`;
