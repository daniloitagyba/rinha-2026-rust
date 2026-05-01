#!/bin/sh
set -eu

compose_files="-f docker-compose.yml -f docker-compose.arm64.yml"

docker compose $compose_files build
docker compose $compose_files up -d --force-recreate

for _ in $(seq 1 60); do
  if curl -fsS http://localhost:9999/ready >/dev/null; then
    break
  fi
  sleep 1
done

curl -fsS http://localhost:9999/ready >/dev/null
(cd test && k6 run --quiet test.js)
