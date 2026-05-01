#!/bin/sh
set -eu

if [ ! -f resources/references.json.gz ]; then
  curl -L -o resources/references.json.gz \
    https://raw.githubusercontent.com/zanfranceschi/rinha-de-backend-2026/main/resources/references.json.gz
fi

mkdir -p test data

if [ ! -f test/test-data.json ]; then
  curl -L -o test/test-data.json \
    https://raw.githubusercontent.com/zanfranceschi/rinha-de-backend-2026/main/test/test-data.json
fi

cargo build --release

index_size=0
if [ -f data/references.idx ]; then
  index_size="$(wc -c < data/references.idx | tr -d ' ')"
fi

if [ "${REBUILD_INDEX:-0}" = "1" ] || [ ! -f data/references.idx ] || [ "$index_size" -lt 90000000 ]; then
  scripts/build-index.sh resources/references.json.gz data/references.idx
fi

INDEX_PATH="${INDEX_PATH:-data/references.idx}" \
MIN_CANDIDATES="${MIN_CANDIDATES:-30000}" \
MAX_CANDIDATES="${MAX_CANDIDATES:-120000}" \
FAST_PATH="${FAST_PATH:-true}" \
target/release/rinha-fraud eval test/test-data.json
