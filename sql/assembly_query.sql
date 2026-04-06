-- ============================================================
-- IS License Inventory — Daily Assembly Query
-- ============================================================
-- Purpose:    Read from all staging views and write a new daily
--             snapshot into the license_inventory serving table.
--
-- This query contains NO business logic. All field mapping,
-- renaming, filtering, and calculation belongs in the staging
-- views (is_staging.stg_*). This query's only job is to
-- assemble the staging output and add pipeline metadata.
--
-- Schedule:   Daily at 06:00 UTC
-- Destination: saas-data-platform-prd.is_analytics.license_inventory
-- Write mode: Append
-- Owner:      IS Data Analyst
-- ============================================================


-- ── STEP 1: IDEMPOTENCY GUARD ────────────────────────────────────────────────
-- Delete today's rows before inserting fresh ones.
-- This makes the job safe to re-run manually if something goes wrong.
-- Without this, re-running the job on the same day would create
-- duplicate rows for the same snapshot_date.

DELETE FROM `saas-data-platform-prd.is_analytics.license_inventory`
WHERE snapshot_date = CURRENT_DATE();


-- ── STEP 2: ASSEMBLE AND INSERT ──────────────────────────────────────────────
-- Stack all staging views on top of each other using UNION ALL,
-- then add pipeline metadata columns before writing to the serving table.
--
-- Adding a new system in Phase 2 requires only one change to this query:
-- uncomment its UNION ALL line in the all_sources CTE below.
-- Nothing else in this query ever needs to change.

INSERT INTO `saas-data-platform-prd.is_analytics.license_inventory`

WITH all_sources AS (

  -- All five Salesforce instances are handled in a single combined staging view.
  -- stg_salesforce internally contains the UNION ALL across Instance C, Instance B,
  -- Instance A, Instance E, and Instance D. See that view for instance-level detail.
  SELECT * FROM `saas-data-platform-prd.is_staging.stg_salesforce`

  -- ── PHASE 2 ADDITIONS ──────────────────────────────────────
  -- Uncomment one line per system as it is onboarded in Phase 2.
  -- The staging view must be created and validated before uncommenting.
  --
  -- UNION ALL SELECT * FROM `saas-data-platform-prd.is_staging.stg_outreach`
  -- UNION ALL SELECT * FROM `saas-data-platform-prd.is_staging.stg_calendly`
  -- UNION ALL SELECT * FROM `saas-data-platform-prd.is_staging.stg_textexpander`
  -- ───────────────────────────────────────────────────────────

)

SELECT
  -- The date this snapshot represents.
  -- Used as the table's partition key and for all date filtering in the dashboard.
  CURRENT_DATE()                                                      AS snapshot_date,

  -- The eight staging contract columns, passed through unchanged.
  -- All transformation has already happened inside the staging view.
  source_system,
  license_type,
  total_licenses,
  used_licenses,
  available_licenses,

  -- Utilization percentage, computed here and stored in the serving table
  -- so Looker does not recompute it on every dashboard load.
  -- SAFE_DIVIDE prevents a division-by-zero error if total_licenses is 0.
  ROUND(SAFE_DIVIDE(used_licenses, total_licenses) * 100, 1)         AS utilization_pct,

  ingestion_type,
  data_quality_flag,
  used_licenses_last_updated,

  -- The timestamp when this assembly job ran.
  -- If data ever looks unexpected, compare this against the 06:00 UTC
  -- expected run time to confirm the job executed on schedule.
  CURRENT_TIMESTAMP()                                                 AS ingested_at

FROM all_sources;
