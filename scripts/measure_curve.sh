#!/usr/bin/env bash
# Run the ingest memory harness across several dataset sizes and print the
# memory-vs-size curve. A rising curve is the OOM bug; a flat curve is the
# fixed out-of-core behaviour.
set -euo pipefail

cd "$(dirname "$0")/.."

CONTENT_BYTES="${CONTENT_BYTES:-1500}"
SIZES="${SIZES:-20000 80000 160000}"

echo "rows,content_bytes,written,elapsed_s,peak_rss_mb"
for rows in $SIZES; do
  line=$(poetry run python scripts/measure_ingest.py \
    --rows "$rows" --content-bytes "$CONTENT_BYTES" 2>/dev/null \
    | grep '^RESULT ')
  # RESULT rows=.. content_bytes=.. written=.. elapsed_s=.. peak_rss_mb=..
  echo "$line" | sed -E 's/RESULT //; s/[a-z_]+=//g; s/ /,/g'
done
