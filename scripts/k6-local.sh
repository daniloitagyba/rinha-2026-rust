#!/bin/sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
MODE="${MODE:-submission}"
PROJECT_NAME="${PROJECT_NAME:-rinha-rust-local}"
K6_IMAGE="${K6_IMAGE:-grafana/k6:latest}"
KEEP_SERVICES="${KEEP_SERVICES:-0}"
REFRESH_DATA="${REFRESH_DATA:-0}"
PULL="${PULL:-0}"
OVERRIDE_FILE=""

if [ "$MODE" = "submission" ]; then
  COMPOSE_FILE="$ROOT/submission/docker-compose.yml"
elif [ "$MODE" = "build" ]; then
  COMPOSE_FILE="$ROOT/docker-compose.yml"
else
  echo "MODE must be submission or build" >&2
  exit 2
fi

if [ "$REFRESH_DATA" = "1" ] || [ ! -f "$ROOT/test/test-data.json" ]; then
  mkdir -p "$ROOT/test"
  curl -L -o "$ROOT/test/test-data.json" \
    https://raw.githubusercontent.com/zanfranceschi/rinha-de-backend-2026/main/test/test-data.json
fi

if [ "$MODE" = "build" ] && { [ "$REFRESH_DATA" = "1" ] || [ ! -f "$ROOT/resources/references.json.gz" ]; }; then
  mkdir -p "$ROOT/resources"
  curl -L -o "$ROOT/resources/references.json.gz" \
    https://raw.githubusercontent.com/zanfranceschi/rinha-de-backend-2026/main/resources/references.json.gz
fi

compose() {
  if [ -n "$OVERRIDE_FILE" ]; then
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" -f "$OVERRIDE_FILE" "$@"
  else
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
  fi
}

cleanup() {
  if [ "$KEEP_SERVICES" != "1" ]; then
    compose down --remove-orphans >/dev/null 2>&1 || true
  fi

  if [ -n "$OVERRIDE_FILE" ]; then
    rm -f "$OVERRIDE_FILE"
  fi
}
trap cleanup EXIT INT TERM

if [ -n "${EARLY_CANDIDATES:-}" ] || \
   [ -n "${MIN_CANDIDATES:-}" ] || \
   [ -n "${MAX_CANDIDATES:-}" ] || \
   [ -n "${PROFILE_FASTPATH:-}" ] || \
   [ -n "${PROFILE_MIN_COUNT:-}" ] || \
   [ -n "${EXACT_FALLBACK:-}" ] || \
   [ -n "${FAST_PATH:-}" ] || \
   [ -n "${WORKERS:-}" ]; then
  OVERRIDE_FILE="${TMPDIR:-/tmp}/${PROJECT_NAME}.override.yml"
  {
    echo "services:"
    for service in api1 api2; do
      echo "  $service:"
      echo "    environment:"
      [ -n "${EARLY_CANDIDATES:-}" ] && echo "      EARLY_CANDIDATES: \"$EARLY_CANDIDATES\""
      [ -n "${MIN_CANDIDATES:-}" ] && echo "      MIN_CANDIDATES: \"$MIN_CANDIDATES\""
      [ -n "${MAX_CANDIDATES:-}" ] && echo "      MAX_CANDIDATES: \"$MAX_CANDIDATES\""
      [ -n "${PROFILE_FASTPATH:-}" ] && echo "      PROFILE_FASTPATH: \"$PROFILE_FASTPATH\""
      [ -n "${PROFILE_MIN_COUNT:-}" ] && echo "      PROFILE_MIN_COUNT: \"$PROFILE_MIN_COUNT\""
      [ -n "${EXACT_FALLBACK:-}" ] && echo "      EXACT_FALLBACK: \"$EXACT_FALLBACK\""
      [ -n "${FAST_PATH:-}" ] && echo "      FAST_PATH: \"$FAST_PATH\""
      [ -n "${WORKERS:-}" ] && echo "      WORKERS: \"$WORKERS\""
    done
  } > "$OVERRIDE_FILE"
fi

if [ "$PULL" = "1" ] || [ "$MODE" = "submission" ]; then
  compose pull
fi

if [ "$MODE" = "build" ]; then
  docker build -t ghcr.io/daniloitagyba/rinha-2026-rust:latest "$ROOT"
  compose up -d --remove-orphans
else
  compose up -d --remove-orphans
fi

ready=0
for _ in $(seq 1 90); do
  if curl -fsS "http://127.0.0.1:9999/ready" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [ "$ready" != "1" ]; then
  echo "backend did not become ready on http://127.0.0.1:9999/ready" >&2
  exit 1
fi

docker run --rm \
  --network "${PROJECT_NAME}_default" \
  -e BASE_URL="http://lb:9999" \
  -e RESULTS_PATH="/scripts/results.json" \
  -e TARGET_RATE \
  -e RAMP_DURATION \
  -e START_RATE \
  -e PRE_ALLOCATED_VUS \
  -e MAX_VUS \
  -e REQUEST_TIMEOUT \
  -v "$ROOT/test:/scripts" \
  "$K6_IMAGE" run /scripts/test.js
