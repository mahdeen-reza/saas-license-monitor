# license_inventory.view.lkml
# Purpose: Define all dimensions and measures available from the license_inventory
# serving table. This is the only file that Looker uses to read license data.
# Looker reads from is_analytics.license_inventory — it has no knowledge of
# the staging layer or the raw Salesforce tables underneath.

view: license_inventory {

  # Point Looker at the serving table in BigQuery.
  sql_table_name: `saas-data-platform-prd.is_analytics.license_inventory` ;;


  # ── DIMENSIONS ─────────────────────────────────────────────────────────────
  # Dimensions are used for filtering, grouping, and labeling in the dashboard.
  # They map directly to columns in the license_inventory table.

  dimension: source_system {
    type:  string
    sql:   ${TABLE}.source_system ;;
    label: "System"
    description: "The name of the IS-managed system, e.g. 'Salesforce Instance C'"
  }

  dimension: license_type {
    type:  string
    sql:   ${TABLE}.license_type ;;
    label: "License type"
    description: "The license category within the system, e.g. 'Salesforce', 'Chatter External'"
  }

  dimension: ingestion_type {
    type:  string
    sql:   ${TABLE}.ingestion_type ;;
    label: "Ingestion type"
    description: "How this data was loaded into BigQuery: fivetran, api, manual, or scraper"
  }

  dimension: data_quality_flag {
    type:  string
    sql:   ${TABLE}.data_quality_flag ;;
    label: "Data quality flag"
    description: "NULL means data is healthy. Any other value indicates a data quality issue."
  }

  # This yes/no dimension makes it easy to filter the dashboard to only rows
  # with data quality issues, and to display a count of issues on the health tile.
  dimension: has_data_quality_issue {
    type:  yesno
    sql:   ${data_quality_flag} IS NOT NULL ;;
    label: "Has data quality issue?"
  }

  dimension: total_licenses {
    type:  number
    sql:   ${TABLE}.total_licenses ;;
    label: "Total licenses"
  }

  dimension: used_licenses {
    type:  number
    sql:   ${TABLE}.used_licenses ;;
    label: "Used licenses"
  }

  dimension: available_licenses {
    type:  number
    sql:   ${TABLE}.available_licenses ;;
    label: "Available licenses"
  }

  dimension: utilization_pct {
    type:         number
    sql:          ${TABLE}.utilization_pct ;;
    label:        "Utilization %"
    value_format: "0.0\%"
  }

  # alert_status drives conditional formatting on the dashboard.
  # Critical = immediate action required (< 5 seats left)
  # Warning   = monitor closely (< 10 seats left)
  # Healthy   = no action needed
  # Thresholds are defined here. To change them, update this view file only.
  dimension: alert_status {
    type:  string
    sql:
      CASE
        WHEN ${available_licenses} < 5  THEN 'critical'
        WHEN ${available_licenses} < 10 THEN 'warning'
        ELSE                                 'healthy'
      END ;;
    label: "Alert status"
    description: "Critical = < 5 available, Warning = < 10 available, Healthy = 10+"
  }

  # Snapshot date dimension group — allows filtering and grouping by
  # day, week, month, quarter, or year in the dashboard.
  dimension_group: snapshot {
    type:       time
    timeframes: [date, week, month, quarter, year]
    sql:        ${TABLE}.snapshot_date ;;
    label:      "Snapshot"
  }


  # ── MEASURES ────────────────────────────────────────────────────────────────
  # Measures are aggregations — they compute a single value across multiple rows.
  # Used in the summary tiles and charts on the dashboard.

  # Max is used rather than sum here because license_inventory stores one row
  # per license type per system per day. For a single day, max = the actual value.
  # Using sum would double-count if multiple rows exist for the same type.
  measure: latest_total {
    type:  max
    sql:   ${total_licenses} ;;
    label: "Total licenses"
  }

  measure: latest_used {
    type:  max
    sql:   ${used_licenses} ;;
    label: "Used licenses"
  }

  measure: latest_available {
    type:  max
    sql:   ${available_licenses} ;;
    label: "Available licenses"
  }

  measure: avg_utilization {
    type:         average
    sql:          ${utilization_pct} ;;
    label:        "Avg utilization %"
    value_format: "0.0\%"
  }

  # Count of distinct systems currently at critical threshold.
  # Used in the health summary tile at the top of the dashboard.
  measure: systems_at_critical {
    type:  count_distinct
    sql:   CASE WHEN ${alert_status} = 'critical' THEN ${source_system} END ;;
    label: "Systems at critical"
  }

  measure: systems_at_warning {
    type:  count_distinct
    sql:   CASE WHEN ${alert_status} = 'warning' THEN ${source_system} END ;;
    label: "Systems at warning"
  }

}
