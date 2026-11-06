#!/bin/sh
# Recovery cases that must fail closed: a deleted volume, and a restore that cannot reach the replica.
set -eu
cd "$(dirname "$0")/.."

export OSD1_API_TOKEN="${OSD1_API_TOKEN:-dev-token}"
compose() {
  docker compose -f deploy/compose.yml "$@"
}
cleanup() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "failed restore against an unreachable endpoint"
fail_dir=$(mktemp -d)
mkdir -p "$fail_dir/out"
cat >"$fail_dir/litestream.yml" <<'YAML'
dbs:
  - path: /out/osd1.db
    replica:
      type: s3
      bucket: osd1-missing
      path: osd1.db
      endpoint: http://127.0.0.1:9
      region: us-east-1
      force-path-style: true
      access-key-id: nope
      secret-access-key: nope
YAML
set +e
docker run --rm \
  -v "$fail_dir/litestream.yml:/etc/litestream.yml:ro" \
  -v "$fail_dir/out:/out" \
  litestream/litestream:0.5 \
  restore -config /etc/litestream.yml /out/osd1.db \
  >"$fail_dir/restore.log" 2>&1
rc=$?
set -e
echo "failed_restore_exit $rc"
cat "$fail_dir/restore.log"
if [ "$rc" -eq 0 ]; then
  echo "restore unexpectedly succeeded" >&2
  rm -rf "$fail_dir"
  exit 1
fi
if [ -f "$fail_dir/out/osd1.db" ] && grep -a -q "litestream-restore-ok" "$fail_dir/out/osd1.db"; then
  echo "failed restore wrote the marker row" >&2
  rm -rf "$fail_dir"
  exit 1
fi
if [ -f "$fail_dir/out/osd1.db" ]; then
  echo "failed_restore_output_file present"
else
  echo "failed_restore_output_file absent"
fi
rm -rf "$fail_dir"
echo "failed restore ok"

echo "missing volume"
compose up -d --build
i=0
while [ "$i" -lt 60 ]; do
  if curl -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
if ! curl -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; then
  echo "osd1 did not become healthy" >&2
  compose logs osd1 >&2 || true
  exit 1
fi
curl -fsS -H "Authorization: Bearer $OSD1_API_TOKEN" -H "content-type: application/json" \
  -d '{"sql":"CREATE TABLE gone (id INTEGER PRIMARY KEY, note TEXT); INSERT INTO gone (note) VALUES (?);","params":["not-on-next-volume"]}' \
  "http://127.0.0.1:8080/accounts/local/d1/database/osd1/query" >/dev/null
compose down -v --remove-orphans
compose up -d
i=0
while [ "$i" -lt 60 ]; do
  if curl -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
body=$(curl -sS -w '\n%{http_code}' -H "Authorization: Bearer $OSD1_API_TOKEN" -H "content-type: application/json" \
  -d '{"sql":"SELECT note FROM gone"}' \
  "http://127.0.0.1:8080/accounts/local/d1/database/osd1/query")
echo "$body"
status=$(echo "$body" | tail -n 1)
echo "$body" | grep -q "no such table"
test "$status" = "400"
echo "missing volume ok"
