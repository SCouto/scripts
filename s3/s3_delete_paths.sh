#!/usr/bin/env bash
# Deletes S3 paths listed in a paths file (one s3:// URI per line)
# Usage: bash s3_delete_paths.sh [--execute] [--file paths_file]
# Dry run by default; pass --execute to actually delete
set -euo pipefail

DRY_RUN=1
PATHS_FILE="$(dirname "$0")/paths_to_delete.txt"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --execute) DRY_RUN=0 ;;
    --file) PATHS_FILE="$2"; shift ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
  shift
done

if [[ ! -f "$PATHS_FILE" ]]; then
  echo "Paths file not found: $PATHS_FILE" >&2
  exit 1
fi

count=0
while IFS= read -r uri || [[ -n "$uri" ]]; do
  [[ -z "$uri" || "$uri" == \#* ]] && continue
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[DRY RUN] aws s3 rm --recursive $uri"
  else
    echo "Deleting $uri ..."
    aws s3 rm --recursive "$uri"
  fi
  (( count++ ))
done < "$PATHS_FILE"

echo "Done. $count paths processed."
