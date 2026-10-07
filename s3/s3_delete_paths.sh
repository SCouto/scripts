#!/usr/bin/env bash
# Deletes S3 paths listed in a paths file (one s3:// URI per line)
# Usage: bash s3_delete_paths.sh [--execute] [--file paths_file] [--reverse]
# Dry run by default; pass --execute to actually delete
set -euo pipefail

DRY_RUN=1
REVERSE=0
PATHS_FILE="$(dirname "$0")/paths_to_delete.txt"
LOG_DIR="$(dirname "$0")/log"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/s3_delete_$(date +%Y%m%d_%H%M%S).log"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --execute) DRY_RUN=0 ;;
    --file) PATHS_FILE="$2"; shift ;;
    --reverse) REVERSE=1 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
  shift
done

if [[ ! -f "$PATHS_FILE" ]]; then
  echo "Paths file not found: $PATHS_FILE" >&2
  exit 1
fi

log() {
  local msg="[$(date +%Y-%m-%dT%H:%M:%S)] $*"
  echo "$msg" >> "$LOG_FILE"
  echo "$msg"
}

# Count total (non-empty, non-comment) lines
total=$(grep -c -v '^\s*$\|^\s*#' "$PATHS_FILE" || true)

log "Starting. Total paths: $total. Log: $LOG_FILE"
[[ "$DRY_RUN" == "1" ]] && log "Mode: DRY RUN" || log "Mode: EXECUTE"
[[ "$REVERSE" == "1" ]] && log "Order: REVERSE (bottom to top)"

if [[ "$REVERSE" == "1" ]]; then
  _INPUT=$(mktemp)
  trap 'rm -f "$_INPUT"' EXIT
  command -v tac &>/dev/null && tac "$PATHS_FILE" > "$_INPUT" || tail -r "$PATHS_FILE" > "$_INPUT"
else
  _INPUT="$PATHS_FILE"
fi

count=0
while IFS= read -r uri || [[ -n "$uri" ]]; do
  [[ -z "$uri" || "$uri" == \#* ]] && continue
  (( count++ ))
  pct=$(( count * 100 / total ))
  if [[ "$DRY_RUN" == "1" ]]; then
    log "[DRY RUN] ($count/$total ${pct}%) aws s3 rm --recursive $uri"
  else
    log "($count/$total ${pct}%) Deleting $uri ..."
    aws s3 rm --recursive "$uri" >> "$LOG_FILE" 2>&1
  fi
  printf "\r  Progress: %d/%d (%d%%)   " "$count" "$total" "$pct"
done < "$_INPUT"

printf "\n"
log "Done. $count/$total paths processed."
