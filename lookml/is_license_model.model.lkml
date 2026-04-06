# is_license_model.model.lkml
# Purpose: Register the license_inventory view as a Looker Explore.
# An Explore is what users and dashboard tiles query against.
# This file connects the BigQuery connection to the view definition above.

# Replace "your_bigquery_connection_name" with the name of the BigQuery
# connection configured in your Looker Admin panel under Connections.
connection: "your_bigquery_connection_name"

# Include the view file defined above.
include: "/views/license_inventory.view"

explore: license_inventory {
  label:       "IS License Monitor"
  description: "License capacity across all IS-managed systems. Always filtered to latest snapshot by default."

  # always_filter ensures every query against this Explore defaults to today's data.
  # Without this, a user opening the Explore would see all historical rows, which
  # is expensive and confusing. Users can override this filter manually if needed.
  always_filter: {
    filters: [license_inventory.snapshot_date: "1 days"]
  }
}
