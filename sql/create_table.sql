-- BAF-2: AppsFlyer purchase events (Non-Organic + Retargeting, Facebook Ads).
-- Column set and NOT NULL constraints per Mark Malovichko's DDL (BAF-2 comment 62293).
-- Time columns are DATETIME, not TIMESTAMP (schema owner's decision, applied to the
-- production table 2026-07-10): DATETIME stores the literal wall-clock value the pipeline
-- writes, with no session-timezone conversion on insert or read.
-- This file documents the schema for reference and manual execution;
-- `appsflyer-pipeline create-table` creates it programmatically (idempotent) using the
-- table name configured via DB_TABLE — keep the two in sync.
--
-- `id`/PRIMARY KEY/idx_app_attr_time added 2026-07-08 (issue #14); an already-provisioned
-- table needs the one-time migration in sql/migrations/2026-07-08-add-id-pk-and-index.sql
-- instead — CREATE TABLE IF NOT EXISTS does not retrofit an existing table. The production
-- table recreated on 2026-07-10 lacks them (it predates re-running that migration).
--
-- `is_primary_attribution` added 2026-07-10 (issue #55); an already-provisioned table needs
-- the one-time migration in sql/migrations/2026-07-10-add-is-primary-attribution.sql instead.

CREATE TABLE IF NOT EXISTS `appsflyer_events_fb` (
    `id`                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `event_time`            DATETIME       NOT NULL,
    `install_time`          DATETIME       NULL,
    `attributed_touch_time` DATETIME       NULL,
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
    `is_primary_attribution` TINYINT(1)    NOT NULL,
    `app_id`                VARCHAR(100)   NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_app_attr_time` (`app_id`, `attribution_type`, `event_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
