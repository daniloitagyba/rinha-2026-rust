#!/bin/sh
set -eu

INDEX_PATH="${INDEX_PATH:-/app/data/references.idx}"

if [ ! -f "$INDEX_PATH" ]; then
  if [ -f /app/resources/references.json.gz ]; then
    mkdir -p "$(dirname "$INDEX_PATH")"
    gzip -dc /app/resources/references.json.gz | rinha-fraud build-index "$INDEX_PATH"
  else
    echo "missing index at $INDEX_PATH and no /app/resources/references.json.gz fallback" >&2
    exit 1
  fi
fi

exec rinha-fraud serve
