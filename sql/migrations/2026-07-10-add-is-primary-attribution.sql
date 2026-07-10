-- BAF-2 issue #55: add the is_primary_attribution flag to the already-provisioned
-- production table (CREATE TABLE IF NOT EXISTS does not retrofit an existing table).
-- Run PHASE 1, then re-backfill the retained window to populate real flags, then
-- (only after verifying zero NULLs) run PHASE 2. See docs/RUNBOOK.md.
-- Replace `appsflyer_events_fb` with the DB_TABLE value if it differs.

-- PHASE 1 — add nullable so pre-deploy inserts keep working and existing rows
-- read as "not yet populated" (a DEFAULT 0 would mislabel all history as false):
ALTER TABLE `appsflyer_events_fb`
    ADD COLUMN `is_primary_attribution` TINYINT(1) NULL AFTER `attribution_type`;

-- PHASE 2 — run ONLY after the re-backfill repopulates every row and
-- `SELECT COUNT(*) ... WHERE is_primary_attribution IS NULL` returns 0:
-- ALTER TABLE `appsflyer_events_fb`
--     MODIFY `is_primary_attribution` TINYINT(1) NOT NULL;
