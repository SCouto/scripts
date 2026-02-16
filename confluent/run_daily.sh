#!/bin/bash

if [ $# -lt 3 ]; then
  echo "Usage: $0 <init_date> <end_date> <grep_pattern> [output_file]"
  echo "Example: $0 2026-01-15 2026-01-20 kafka-cluster-pro-01 costs.txt"
  exit 1
fi

INIT_DATE=$1
END_DATE=$2
GREP_PATTERN=$3
OUTPUT_FILE=${4:-cost.txt}

# Remove output file if it exists
rm -f "$OUTPUT_FILE"

# Convert dates to epoch for comparison
if [[ "$OSTYPE" == "darwin"* ]]; then
  # macOS
  init_epoch=$(date -j -f "%Y-%m-%d" "$INIT_DATE" "+%s")
  end_epoch=$(date -j -f "%Y-%m-%d" "$END_DATE" "+%s")
else
  # Linux
  init_epoch=$(date -d "$INIT_DATE" "+%s")
  end_epoch=$(date -d "$END_DATE" "+%s")
fi

current_epoch=$init_epoch

while [ $current_epoch -lt $end_epoch ]; do
  if [[ "$OSTYPE" == "darwin"* ]]; then
    current_date=$(date -j -f "%s" "$current_epoch" "+%Y-%m-%d")
    next_epoch=$((current_epoch + 86400))
    next_date=$(date -j -f "%s" "$next_epoch" "+%Y-%m-%d")
  else
    current_date=$(date -d "@$current_epoch" "+%Y-%m-%d")
    next_epoch=$((current_epoch + 86400))
    next_date=$(date -d "@$next_epoch" "+%Y-%m-%d")
  fi

  output=$(./script.sh "$current_date" "$next_date" | grep "$GREP_PATTERN")
  echo "$current_date $next_date $output" >> "$OUTPUT_FILE"

  current_epoch=$next_epoch
done

echo "Results saved to $OUTPUT_FILE"
