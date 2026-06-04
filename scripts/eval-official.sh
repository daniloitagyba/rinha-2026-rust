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
if [ -f data/references.idx ] && ! grep -a -q "R26META1" data/references.idx; then
  index_size=0
fi

if [ "${REBUILD_INDEX:-0}" = "1" ] || [ ! -f data/references.idx ] || [ "$index_size" -lt 90000000 ]; then
  scripts/build-index.sh resources/references.json.gz data/references.idx
fi

refs_sha="$(sha256sum resources/references.json.gz | awk '{print $1}')"

INDEX_PATH="${INDEX_PATH:-data/references.idx}" \
EXPECTED_REFERENCES_GZIP_SHA256="${EXPECTED_REFERENCES_GZIP_SHA256:-$refs_sha}" \
EARLY_CANDIDATES="${EARLY_CANDIDATES:-9000}" \
MIN_CANDIDATES="${MIN_CANDIDATES:-9000}" \
MAX_CANDIDATES="${MAX_CANDIDATES:-18000}" \
PROFILE_FASTPATH="${PROFILE_FASTPATH:-1}" \
PROFILE_FASTPATH_REFERENCE_SHA256="${PROFILE_FASTPATH_REFERENCE_SHA256:-$refs_sha}" \
PROFILE_MIN_COUNT="${PROFILE_MIN_COUNT:-15}" \
PROFILE_LEGIT_MIN_COUNT="${PROFILE_LEGIT_MIN_COUNT:-15}" \
PROFILE_FRAUD_MIN_COUNT="${PROFILE_FRAUD_MIN_COUNT:-200}" \
PROFILE_DOMINANT_FASTPATH="${PROFILE_DOMINANT_FASTPATH:-0}" \
PROFILE_DOMINANT_MIN_COUNT="${PROFILE_DOMINANT_MIN_COUNT:-15}" \
PROFILE_DOMINANT_MAX_OPPOSITE="${PROFILE_DOMINANT_MAX_OPPOSITE:-2}" \
EXACT_FALLBACK="${EXACT_FALLBACK:-risky}" \
EARLY_EDGE_FALLBACK="${EARLY_EDGE_FALLBACK:-1}" \
RISKY_AMOUNT_MIN="${RISKY_AMOUNT_MIN:-400}" \
RISKY_AMOUNT_MAX="${RISKY_AMOUNT_MAX:-3000}" \
RISKY_INSTALLMENTS_MIN="${RISKY_INSTALLMENTS_MIN:-2200}" \
RISKY_INSTALLMENTS_MAX="${RISKY_INSTALLMENTS_MAX:-6200}" \
RISKY_RATIO_MIN="${RISKY_RATIO_MIN:-850}" \
RISKY_KM_HOME_MIN="${RISKY_KM_HOME_MIN:-250}" \
RISKY_KM_HOME_MAX="${RISKY_KM_HOME_MAX:-4000}" \
RISKY_TX24H_MIN="${RISKY_TX24H_MIN:-1500}" \
RISKY_TX24H_MAX="${RISKY_TX24H_MAX:-5800}" \
RISKY_MERCHANT_AVG_MIN="${RISKY_MERCHANT_AVG_MIN:-0}" \
RISKY_MERCHANT_AVG_MAX="${RISKY_MERCHANT_AVG_MAX:-420}" \
FAST_PATH="${FAST_PATH:-false}" \
target/release/rinha-fraud eval test/test-data.json
