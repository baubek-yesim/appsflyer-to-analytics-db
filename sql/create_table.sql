-- BAF-2: AppsFlyer purchase events (Non-Organic + Retargeting, Facebook Ads).
-- Column set and NOT NULL constraints per Mark Malovichko's DDL (BAF-2 comment 62293).
-- Matches the table already provisioned in production as `appsflyer_events_fb`
-- (confirmed via `SHOW CREATE TABLE`). This file documents that schema for reference
-- and manual execution; `appsflyer-pipeline create-table` creates it programmatically
-- (idempotent) using the table name configured via DB_TABLE — keep the two in sync.

CREATE TABLE IF NOT EXISTS `appsflyer_events_fb` (
    `event_time`            TIMESTAMP      NOT NULL,
    `install_time`          TIMESTAMP      NULL,
    `attributed_touch_time` TIMESTAMP      NULL,
    `event_name`            VARCHAR(100)   NOT NULL,
    `event_revenue`         DECIMAL(18,4)  NULL,
    `media_source`          VARCHAR(100)   NULL,
    `channel`               VARCHAR(255)   NULL,
    `campaign`              VARCHAR(255)   NULL,
    `campaign_id`           VARCHAR(255)   NULL,
    `adset`                 VARCHAR(255)   NULL,
    `adset_id`              VARCHAR(255)   NULL,
    `ad`                    VARCHAR(255)   NULL,
    `ad_id`                 VARCHAR(255)   NULL,
    `appsflyer_id`          VARCHAR(100)   NOT NULL,
    `customer_user_id`      VARCHAR(255)   NULL,
    `attribution_type`      VARCHAR(50)    NOT NULL,
    `app_id`                VARCHAR(100)   NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
