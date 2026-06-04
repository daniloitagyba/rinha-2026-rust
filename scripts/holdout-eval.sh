#!/bin/sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
WORK_DIR="${WORK_DIR:-/root/rinha-2026-rust-holdout}"
BIN="${BIN:-$ROOT/target/release/rinha-fraud}"
HOLDOUT_MOD="${HOLDOUT_MOD:-100}"
HOLDOUT_OFFSET="${HOLDOUT_OFFSET:-0}"

if [ ! -x "$BIN" ]; then
  cargo build --release
fi

mkdir -p "$WORK_DIR"

if [ ! -f "$ROOT/resources/references.json.gz" ]; then
  mkdir -p "$ROOT/resources"
  curl -L -o "$ROOT/resources/references.json.gz" \
    https://raw.githubusercontent.com/zanfranceschi/rinha-de-backend-2026/main/resources/references.json.gz
fi

refs_sha="$(sha256sum "$ROOT/resources/references.json.gz" | cut -d ' ' -f 1)"
train_json="$WORK_DIR/references-train-m${HOLDOUT_MOD}-o${HOLDOUT_OFFSET}.json"
holdout_json="$WORK_DIR/references-holdout-m${HOLDOUT_MOD}-o${HOLDOUT_OFFSET}.json"
index_path="$WORK_DIR/references-train-m${HOLDOUT_MOD}-o${HOLDOUT_OFFSET}.idx"

if [ "${REFRESH_HOLDOUT:-0}" = "1" ] || [ ! -f "$train_json" ] || [ ! -f "$holdout_json" ]; then
  gzip -dc "$ROOT/resources/references.json.gz" | \
    "$BIN" split-references "$train_json" "$holdout_json" "$HOLDOUT_MOD" "$HOLDOUT_OFFSET"
fi

if [ "${REBUILD_INDEX:-0}" = "1" ] || [ ! -f "$index_path" ]; then
  REFERENCES_GZIP_SHA256="$refs_sha" "$BIN" build-index "$index_path" < "$train_json"
fi

INDEX_PATH="$index_path" \
EXPECTED_REFERENCES_GZIP_SHA256="$refs_sha" \
PROFILE_FASTPATH_REFERENCE_SHA256="$refs_sha" \
EARLY_CANDIDATES="${EARLY_CANDIDATES:-9000}" \
MIN_CANDIDATES="${MIN_CANDIDATES:-9000}" \
MAX_CANDIDATES="${MAX_CANDIDATES:-18000}" \
PROFILE_FASTPATH="${PROFILE_FASTPATH:-1}" \
PROFILE_MIN_COUNT="${PROFILE_MIN_COUNT:-15}" \
PROFILE_LEGIT_MIN_COUNT="${PROFILE_LEGIT_MIN_COUNT:-15}" \
PROFILE_FRAUD_MIN_COUNT="${PROFILE_FRAUD_MIN_COUNT:-200}" \
PROFILE_DOMINANT_FASTPATH="${PROFILE_DOMINANT_FASTPATH:-0}" \
PROFILE_DOMINANT_MIN_COUNT="${PROFILE_DOMINANT_MIN_COUNT:-15}" \
PROFILE_DOMINANT_MAX_OPPOSITE="${PROFILE_DOMINANT_MAX_OPPOSITE:-2}" \
PROFILE_EXACT_TRIGGERS="${PROFILE_EXACT_TRIGGERS:-0}" \
EXACT_FALLBACK="${EXACT_FALLBACK:-off}" \
BUCKET_EXACT_FALLBACK="${BUCKET_EXACT_FALLBACK:-0}" \
SELECTIVE_BUCKET_EXACT="${SELECTIVE_BUCKET_EXACT:-1}" \
BUCKET_EXACT_WARM_CANDIDATES="${BUCKET_EXACT_WARM_CANDIDATES:-0}" \
EARLY_EDGE_FALLBACK="${EARLY_EDGE_FALLBACK:-1}" \
RISKY_AMOUNT_MIN="${RISKY_AMOUNT_MIN:-350}" \
RISKY_AMOUNT_MAX="${RISKY_AMOUNT_MAX:-3200}" \
RISKY_INSTALLMENTS_MIN="${RISKY_INSTALLMENTS_MIN:-2000}" \
RISKY_INSTALLMENTS_MAX="${RISKY_INSTALLMENTS_MAX:-6500}" \
RISKY_RATIO_MIN="${RISKY_RATIO_MIN:-750}" \
RISKY_KM_HOME_MIN="${RISKY_KM_HOME_MIN:-200}" \
RISKY_KM_HOME_MAX="${RISKY_KM_HOME_MAX:-4300}" \
RISKY_TX24H_MIN="${RISKY_TX24H_MIN:-1500}" \
RISKY_TX24H_MAX="${RISKY_TX24H_MAX:-6000}" \
RISKY_MERCHANT_AVG_MIN="${RISKY_MERCHANT_AVG_MIN:-0}" \
RISKY_MERCHANT_AVG_MAX="${RISKY_MERCHANT_AVG_MAX:-450}" \
FAST_PATH="${FAST_PATH:-false}" \
"$BIN" eval-references "$holdout_json"
