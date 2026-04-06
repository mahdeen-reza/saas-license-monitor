-- ============================================================
-- IS License Inventory — Salesforce Staging View
-- ============================================================
-- Purpose: Produce a combined license summary for all five Salesforce instances.
-- Instances covered: Instance C, Instance B, Instance A, Instance E, Instance D
-- Owner: IS Data Analyst
--
-- Design note: All five instances are combined in one view rather than five
-- separate views. This keeps the Salesforce logic self-contained in a single
-- file and matches the structure of the working query already in use.
-- When Phase 2 systems are added, they each get their own separate staging view
-- following the template in P2.3 — this combined approach is specific to
-- Salesforce because all five instances share the same source system type.
--
-- Maintenance notes:
-- - Instance C hardcoded license totals must be reviewed annually at contract renewal
-- - If a new Salesforce instance is added, add a new UNION ALL branch at the bottom
-- - If an existing instance is decommissioned, remove its branch and add a comment explaining why
-- ============================================================

CREATE OR REPLACE VIEW `saas-data-platform-prd.is_staging.stg_salesforce` AS

-- ── INSTANCE C ─────────────────────────────────────────────────────────────
-- Instance C does not expose a user_license table through Fivetran.
-- License counts are reconstructed by querying the raw user table directly.

-- Step 1: Read all active Instance C users and normalize their user_type
-- to the human-readable license category names used in the dashboard.
-- Exclude system-level user types that are not real human users and should
-- not count against license usage:
--   AutomatedProcess = background automation accounts
--   Guest            = unauthenticated or public-facing users
--   PowerPartner     = partner community users on a separate license type
WITH instance_c_users AS (
  SELECT
    CASE
      WHEN user_type = 'Standard'             THEN 'Salesforce'
      WHEN user_type = 'CloudIntegrationUser' THEN 'Analytics Cloud Integration User'
      WHEN user_type = 'CsnOnly'              THEN 'Chatter External'
      ELSE                                         'Unknown'
    END AS license_type,
    id
  FROM `saas-data-platform-prd.salesforce_instance_c.user`
  WHERE is_active = TRUE
    AND user_type NOT IN ('AutomatedProcess', 'Guest', 'PowerPartner')
),

-- Step 2: Aggregate Instance C users by license type to get used_licenses per type,
-- and apply the hardcoded total license counts from the Instance C contract.
-- The -2 on the Salesforce type accounts for two system/admin accounts that hold
-- licenses but are not real provisioned users. This rule is specific to Instance C.
instance_c_agg AS (
  SELECT
    license_type,

    -- Hardcoded totals from the Instance C contract. Review annually at renewal.
    CASE
      WHEN license_type = 'Salesforce'                       THEN 86
      WHEN license_type = 'Analytics Cloud Integration User' THEN 1
      WHEN license_type = 'Chatter External'                 THEN 500
      ELSE 0
    END AS total_licenses,

    CASE
      WHEN license_type = 'Salesforce' THEN COUNT(*) - 2
      ELSE COUNT(*)
    END AS used_licenses

  FROM instance_c_users
  WHERE license_type != 'Unknown'
  GROUP BY license_type
)

-- Instance C final output — conforms to the staging contract
SELECT
  'Salesforce Instance C'             AS source_system,
  license_type,
  total_licenses,
  used_licenses,
  total_licenses - used_licenses      AS available_licenses,
  'fivetran'                          AS ingestion_type,
  NULL                                AS data_quality_flag,
  -- Instance C has no source-side last_updated timestamp, so we use the
  -- current pipeline run time as a proxy.
  CURRENT_TIMESTAMP()                 AS used_licenses_last_updated
FROM instance_c_agg

UNION ALL

-- ── INSTANCE B ─────────────────────────────────────────────────────────────
-- Fivetran syncs a user_license table from Instance B that already contains
-- aggregated counts. No reconstruction needed — just rename columns to match
-- the staging contract and filter to active license types only.
SELECT
  'Salesforce Instance B'             AS source_system,
  master_label                        AS license_type,
  total_licenses,
  used_licenses,
  total_licenses - used_licenses      AS available_licenses,
  'fivetran'                          AS ingestion_type,
  NULL                                AS data_quality_flag,
  -- used_licenses_last_updated is a Salesforce system field indicating when
  -- the license record was last refreshed on the Salesforce side.
  used_licenses_last_updated
FROM `saas-data-platform-prd.salesforce_instance_b.user_license`
-- Inactive license types are retired and should not appear in capacity planning
WHERE status = 'Active'

UNION ALL

-- ── INSTANCE A ─────────────────────────────────────────────────────────────
-- Same pattern as Instance B. Note: Instance A data lands in the
-- salesforce_instance_a dataset — this is a Fivetran naming convention
-- specific to this instance, not an error.
SELECT
  'Salesforce Instance A'             AS source_system,
  master_label                        AS license_type,
  total_licenses,
  used_licenses,
  total_licenses - used_licenses      AS available_licenses,
  'fivetran'                          AS ingestion_type,
  NULL                                AS data_quality_flag,
  used_licenses_last_updated
FROM `saas-data-platform-prd.salesforce_instance_a.user_license`
WHERE status = 'Active'

UNION ALL

-- ── INSTANCE E ─────────────────────────────────────────────────────────────
-- Same pattern as Instance B and Instance A.
SELECT
  'Salesforce Instance E'             AS source_system,
  master_label                        AS license_type,
  total_licenses,
  used_licenses,
  total_licenses - used_licenses      AS available_licenses,
  'fivetran'                          AS ingestion_type,
  NULL                                AS data_quality_flag,
  used_licenses_last_updated
FROM `saas-data-platform-prd.salesforce_instance_e.user_license`
WHERE status = 'Active'

UNION ALL

-- ── INSTANCE D ─────────────────────────────────────────────────────────────
-- Same pattern as Instance B, Instance A, and Instance E.
SELECT
  'Salesforce Instance D'             AS source_system,
  master_label                        AS license_type,
  total_licenses,
  used_licenses,
  total_licenses - used_licenses      AS available_licenses,
  'fivetran'                          AS ingestion_type,
  NULL                                AS data_quality_flag,
  used_licenses_last_updated
FROM `saas-data-platform-prd.salesforce_instance_d.user_license`
WHERE status = 'Active';

-- ── FUTURE SALESFORCE INSTANCES ────────────────────────────────────────────
-- If a new Salesforce instance is onboarded, add a new UNION ALL branch here
-- following the same pattern as Instance B / A / E / D above.
-- If the new instance lacks a user_license table (like Instance C), follow
-- the Instance C CTE pattern at the top of this view instead.
