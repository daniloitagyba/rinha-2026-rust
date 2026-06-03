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
  *.gz)
    refs_sha="$(sha256sum "$INPUT" | awk '{print $1}')"
    gzip -dc "$INPUT" | REFERENCES_GZIP_SHA256="$refs_sha" "$BIN" build-index "$OUTPUT"
    ;;
  *) "$BIN" build-index "$OUTPUT" < "$INPUT" ;;
esac
