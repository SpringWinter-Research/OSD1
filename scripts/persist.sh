#!/bin/sh
# Restart the container, then recreate it, and read the same volume both times.
set -eu
cd "$(dirname "$0")/.."

export OSD1_API_TOKEN="${OSD1_API_TOKEN:-persist-secret-token}"
compose() {
  docker compose -f deploy/compose.yml "$@"
}
cleanup() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_health() {
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "osd1 did not become healthy" >&2
  compose logs osd1 >&2 || true
  exit 1
}

query() {
  curl -sS -H "Authorization: Bearer $OSD1_API_TOKEN" -H "content-type: application/json" \
    -d "$1" \
    "http://127.0.0.1:8080/accounts/local/d1/database/osd1/query"
}

echo "starting osd1"
compose up -d --build
wait_health

echo "inserting persist marker"
query '{"sql":"CREATE TABLE kept (id INTEGER PRIMARY KEY, note TEXT); INSERT INTO kept (note) VALUES (?);","params":["persist-restart-ok"]}' >/dev/null

echo "directory and database modes"
dir_mode=$(compose exec -T osd1 stat -c %a /data | tr -d '[:space:]')
db_mode=$(compose exec -T osd1 stat -c %a /data/osd1.db | tr -d '[:space:]')
echo "data_dir_mode $dir_mode"
echo "db_file_mode $db_mode"
test "$dir_mode" = "700"
test "$db_mode" = "600"
if compose exec -T osd1 sh -c 'test -f /data/osd1.db-wal'; then
  wal_mode=$(compose exec -T osd1 stat -c %a /data/osd1.db-wal | tr -d '[:space:]')
  echo "wal_file_mode $wal_mode"
  test "$wal_mode" = "600"
fi

echo "log redaction"
logs=$(compose logs osd1 2>&1 || true)
if echo "$logs" | grep -F -q "$OSD1_API_TOKEN"; then
  echo "api token appeared in container logs" >&2
  exit 1
fi
echo "token_absent_from_logs yes"

echo "restarting container"
compose restart osd1
wait_health
body=$(query '{"sql":"SELECT note FROM kept"}')
echo "$body"
echo "$body" | grep -q 'persist-restart-ok'

echo "recreating container"
compose up -d --force-recreate --no-deps osd1
wait_health
body=$(query '{"sql":"SELECT note FROM kept"}')
echo "$body"
echo "$body" | grep -q 'persist-restart-ok'
echo "persist ok"
