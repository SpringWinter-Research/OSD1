#!/bin/sh
# Same program as scripts/litestream-leader.sh.
# Replicate the database only while this process should own the replica.
# Simple mode has no osd1_raft_role series, so it replicates the whole time.
# Raft mode replicates only while osd1_raft_role is 1 (leader).
# A successful `litestream ltx` listing touches OSD1_LITESTREAM_HEARTBEAT.
set -eu

metrics_url="${OSD1_METRICS_URL:-http://127.0.0.1:8080/metrics}"
heartbeat="${OSD1_LITESTREAM_HEARTBEAT:-/data/litestream-ok}"
config="${LITESTREAM_CONFIG:-/etc/litestream.yml}"
db="${OSD1_DB_PATH:-/data/osd1.db}"
litestream="${LITESTREAM_BIN:-litestream}"

fetch() {
  if command -v wget >/dev/null 2>&1; then
    wget -q -O - "$metrics_url" 2>/dev/null || true
  elif command -v curl >/dev/null 2>&1; then
    curl -fsS "$metrics_url" 2>/dev/null || true
  else
    echo ""
  fi
}

should_replicate() {
  metrics=$(fetch)
  if ! printf '%s\n' "$metrics" | grep -q '^osd1_raft_role '; then
    return 0
  fi
  printf '%s\n' "$metrics" | grep -q '^osd1_raft_role 1$'
}

touch_if_replica() {
  if "$litestream" ltx -config "$config" "$db" 2>/dev/null | grep -E -q '[0-9a-f]{16}'; then
    touch "$heartbeat"
  fi
}

stop_child() {
  if [ -n "${child:-}" ]; then
    kill "$child" 2>/dev/null || true
    wait "$child" 2>/dev/null || true
    child=""
  fi
}

trap 'stop_child; exit 0' INT TERM

child=""
while true; do
  if should_replicate; then
    if [ -z "$child" ]; then
      "$litestream" replicate -config "$config" &
      child=$!
    fi
    touch_if_replica || true
  else
    stop_child
  fi
  sleep 15
done
