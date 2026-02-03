#!/bin/bash

# Confluent Cloud Billing - Get Costs by Resource using CLI
# Usage: ./script.sh [start_date] [end_date]
# Example: ./script.sh 2026-01-01 2026-01-22

END_DATE="${2:-$(date +%Y-%m-%d)}"
START_DATE="${1:-$(date -v-30d +%Y-%m-%d)}"

echo "Fetching costs from $START_DATE to $END_DATE..."
confluent billing cost list --start-date "$START_DATE" --end-date "$END_DATE" -o json > /tmp/confluent_costs.json

if [ $? -ne 0 ]; then
  echo "Error fetching costs"
  exit 1
fi

echo ""
echo "=== Costs by Resource (with line type breakdown) ==="
jq -r '
  group_by(.resource_name) 
  | .[] 
  | . as $items
  | {
      resource_name: (.[0].resource_name // "N/A"),
      resource_id: (.[0].resource // "N/A"),
      subtotals: (group_by(.line_type) | map({
        line_type: .[0].line_type,
        total: (map(.original_amount | gsub("[$,]"; "") | tonumber) | add)
      })),
      total: (map(.original_amount | gsub("[$,]"; "") | tonumber) | add)
    }
  | "\n\(.resource_name) (\(.resource_id)): $\(.total | . * 100 | round / 100)\n" + 
    (.subtotals | map("  - \(.line_type): $\(.total | . * 100 | round / 100)") | join("\n"))
' /tmp/confluent_costs.json

echo ""
echo "=== Costs by Product ==="
jq -r '
  group_by(.product) 
  | .[] 
  | {
      product: .[0].product,
      total: (map(.original_amount | gsub("[$,]"; "") | tonumber) | add)
    }
  | "\(.product): $\(.total | . * 100 | round / 100)"
' /tmp/confluent_costs.json | sort -t'$' -k2 -rn

echo ""
echo "=== Total ==="
total=$(jq '[.[].original_amount | gsub("[$,]"; "") | tonumber] | add | . * 100 | round / 100' /tmp/confluent_costs.json)
echo "Total: \$${total}"

rm -f /tmp/confluent_costs.json
