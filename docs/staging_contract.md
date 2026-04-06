# Staging Contract

Every staging view in this pipeline must output exactly the following 8 columns. This is the contract that makes the serving layer source-agnostic — the assembly query assembles all staging views via `UNION ALL` without knowing anything about the source systems underneath.

When onboarding a new system in Phase 2, the staging view for that system must conform to this schema before the assembly query line is uncommented.

---

## Schema

| Column | Type | Description |
|---|---|---|
| `source_system` | `STRING` | Human-readable display name for the system. Used as the primary identifier in the dashboard. Example: `'Salesforce Instance A'` |
| `license_type` | `STRING` | License category within the system. Example: `'Salesforce'`, `'Chatter External'` |
| `total_licenses` | `INT64` | Total contracted seats for this license type |
| `used_licenses` | `INT64` | Seats currently assigned to active users |
| `available_licenses` | `INT64` | `total_licenses - used_licenses`, computed in the staging view — not in the serving layer |
| `ingestion_type` | `STRING` | How data was loaded into BigQuery. One of: `fivetran`, `api`, `manual`, `scraper` |
| `data_quality_flag` | `STRING` | `NULL` means data is healthy. Any non-null value indicates a data quality issue and will surface in the dashboard health tile |
| `used_licenses_last_updated` | `TIMESTAMP` | The source system's own timestamp for when license usage was last refreshed. If no source-side timestamp exists, use `CURRENT_TIMESTAMP()` as a proxy |

---

## Rules

**`available_licenses` is always computed in staging, never in the serving layer.** The assembly query passes this column through unchanged. This keeps the serving table self-contained and avoids recomputation on every dashboard load.

**`data_quality_flag` is `NULL` when healthy.** Staging views should set this to a descriptive string when a known data quality issue exists — for example, `'stale_data'` when the source timestamp is more than 48 hours old, or `'scraper_failure'` when a Cloud Run job did not produce output. The LookML `has_data_quality_issue` dimension filters on `IS NOT NULL`.

**`ingestion_type` is informational only.** It is not used in any dashboard calculation or alert logic. It exists to help analysts understand data provenance when investigating anomalies.

**`used_licenses_last_updated` drives staleness detection.** When a source system provides its own refresh timestamp, use it. When it does not (e.g. systems reconstructed from raw user tables), use `CURRENT_TIMESTAMP()` and document this in the staging view comment header.

---

## Adding a New System

To onboard a new system to the pipeline:

1. Get raw data landing in BigQuery (via Fivetran, REST API, Google Sheets External Table, or scraper)
2. Write a new staging view (`is_staging.stg_<system_name>`) that outputs exactly these 8 columns
3. Validate the view produces correct output for at least one full day
4. Uncomment the corresponding `UNION ALL` line in `assembly_query.sql`

The serving table, LookML model, and Looker dashboard require no changes.
