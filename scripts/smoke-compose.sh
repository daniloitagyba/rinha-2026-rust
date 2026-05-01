#!/bin/sh
set -eu

docker compose -f docker-compose.yml -f docker-compose.smoke.yml build
docker compose -f docker-compose.yml -f docker-compose.smoke.yml up -d

cleanup() {
  docker compose -f docker-compose.yml -f docker-compose.smoke.yml down
}
trap cleanup EXIT INT TERM

ready=0
for _ in $(seq 1 20); do
  if curl -fsS http://127.0.0.1:9999/ready >/tmp/rinha-smoke-ready.out 2>/dev/null; then
    ready=1
    break
  fi
  sleep 1
done

if [ "$ready" != "1" ]; then
  echo "compose smoke failed: /ready did not become healthy" >&2
  exit 1
fi

cat /tmp/rinha-smoke-ready.out
echo
curl -fsS -H 'Content-Type: application/json' \
  --data-binary @resources/example-payload-legit.json \
  http://127.0.0.1:9999/fraud-score
echo
curl -fsS -H 'Content-Type: application/json' \
  --data-binary @resources/example-payload-fraud.json \
  http://127.0.0.1:9999/fraud-score
echo
