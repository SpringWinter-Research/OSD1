#!/bin/sh
# Restore one known row through Litestream and MinIO.
# Durability is claimed only when this script exits 0.
set -eu

cd "$(dirname "$0")/.."
export OSD1_API_TOKEN="${OSD1_API_TOKEN:-dev-token}"
compose() {
  docker compose -f deploy/compose.yml -f deploy/compose.backup.yml --profile backup "$@"
}

cleanup() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "starting minio and osd1"
compose up -d --build minio minio-init osd1
i=0
while [ "$i" -lt 60 ]; do
  if curl -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 2
done
if ! curl -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; then
  echo "osd1 did not become healthy" >&2
  compose logs osd1 >&2 || true
  exit 1
fi

echo "inserting recovery marker"
curl -fsS -H "Authorization: Bearer $OSD1_API_TOKEN" -H "content-type: application/json" \
  -d '{"sql":"CREATE TABLE markers (id INTEGER PRIMARY KEY, note TEXT); INSERT INTO markers (note) VALUES (?);","params":["litestream-restore-ok"]}' \
  "http://127.0.0.1:8080/accounts/local/d1/database/osd1/query" >/dev/null

echo "starting litestream"
compose up -d litestream

echo "waiting for replica ltx files"
found=0
i=0
while [ "$i" -lt 30 ]; do
  # exec does not use the image entrypoint, so the binary name is required.
  if out=$(compose exec -T litestream litestream ltx -config /etc/litestream.yml /data/osd1.db 2>/dev/null); then
    if echo "$out" | grep -E -q '[0-9a-f]{16}'; then
      echo "$out"
      found=1
      break
    fi
  fi
  i=$((i + 1))
  sleep 2
done
if [ "$found" -ne 1 ]; then
  echo "litestream did not publish ltx files" >&2
  compose logs litestream >&2 || true
  exit 1
fi

echo "stopping osd1 and litestream"
compose stop osd1
# Stop the sidecar before deleting the file so shutdown sync can finish
# and a missing primary does not start a new empty replica generation.
compose stop litestream

echo "removing the primary database"
docker run --rm -v osd1_osd1-data:/data alpine:3.20 \
  rm -f /data/osd1.db /data/osd1.db-wal /data/osd1.db-shm

echo "restoring from the replica"
compose run --rm --no-deps litestream \
  restore -config /etc/litestream.yml /data/osd1.db
docker run --rm -v osd1_osd1-data:/data alpine:3.20 \
  chown -R 10001:10001 /data

echo "checking the restored row"
compose start osd1
ok=0
i=0
while [ "$i" -lt 30 ]; do
  if curl -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; then
    body=$(curl -fsS -H "Authorization: Bearer $OSD1_API_TOKEN" -H "content-type: application/json" \
      -d '{"sql":"SELECT note FROM markers"}' \
      "http://127.0.0.1:8080/accounts/local/d1/database/osd1/query")
    echo "$body"
    echo "$body" | grep -q "litestream-restore-ok"
    ok=1
    break
  fi
  i=$((i + 1))
  sleep 1
done
if [ "$ok" -ne 1 ]; then
  echo "restored database did not serve the marker row" >&2
  exit 1
fi

echo "recovery ok"
