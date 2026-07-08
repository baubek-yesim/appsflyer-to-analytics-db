-- One-time migration for issue #14: appsflyer_events_fb was originally created without a
-- PRIMARY KEY or any secondary index. This adds both to bring an already-provisioned table in
-- line with the schema in sql/create_table.sql (2026-07-08 onward).
--
-- NOT idempotent, NOT run automatically by the pipeline (create-table only ever does
-- `CREATE TABLE IF NOT EXISTS`, which does not retrofit an existing table) -- run this by hand,
-- once, against any environment that was provisioned before this date. A fresh environment
-- (a clean database, CI's mysql:8 service container) never needs this file: it gets the full
-- schema directly from sql/create_table.sql.
ALTER TABLE `appsflyer_events_fb`
    ADD COLUMN `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST,
    ADD KEY `idx_app_attr_time` (`app_id`, `attribution_type`, `event_time`);
