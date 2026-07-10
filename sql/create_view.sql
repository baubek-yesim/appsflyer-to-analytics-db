-- BAF-2 issue #55: de-duplicated read over appsflyer_events_fb. Keeps the
-- primary row of a cross-attribution purchase pair, passes singletons through.
-- `appsflyer-pipeline create-view` creates this programmatically (idempotent)
-- using the DB_TABLE-configured name; keep the two in sync.
CREATE OR REPLACE VIEW `appsflyer_events_fb_deduped` AS
SELECT `event_time`, `install_time`, `attributed_touch_time`, `event_name`,
       `event_revenue`, `media_source`, `channel`, `campaign`, `campaign_id`,
       `adset`, `adset_id`, `ad`, `ad_id`, `appsflyer_id`, `customer_user_id`,
       `attribution_type`, `is_primary_attribution`, `app_id`
FROM (
    SELECT `event_time`, `install_time`, `attributed_touch_time`, `event_name`,
           `event_revenue`, `media_source`, `channel`, `campaign`, `campaign_id`,
           `adset`, `adset_id`, `ad`, `ad_id`, `appsflyer_id`, `customer_user_id`,
           `attribution_type`, `is_primary_attribution`, `app_id`,
           ROW_NUMBER() OVER (
               PARTITION BY `event_time`, `event_name`, `appsflyer_id`
               ORDER BY `is_primary_attribution` DESC, `attribution_type` ASC
           ) AS _dedup_rn
    FROM `appsflyer_events_fb`
) ranked
WHERE _dedup_rn = 1;
