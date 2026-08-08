#!/bin/sh
set -eu

until curl -fsS http://localhost:8083/connectors >/dev/null; do
  sleep 2

curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  http://localhost:8083/connectors/platform-postgres/config \
  --data @/connector.json
