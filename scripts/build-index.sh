#!/bin/sh
set -eu

INPUT="${1:-resources/references.json.gz}"
OUTPUT="${2:-data/references.idx}"
BIN="${BIN:-target/release/rinha-fraud}"

if [ ! -x "$BIN" ]; then
  echo "missing binary at $BIN; run cargo build --release first" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT")"

case "$INPUT" in
  *.gz) gzip -dc "$INPUT" | "$BIN" build-index "$OUTPUT" ;;
  *) "$BIN" build-index "$OUTPUT" < "$INPUT" ;;
esac
