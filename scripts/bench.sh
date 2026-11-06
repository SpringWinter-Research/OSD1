#!/bin/sh
# Measure 1,000 inserts and 1,000 point selects against a local osd1 process.
set -eu
cd "$(dirname "$0")/.."

. "$HOME/.cargo/env"
cargo build -q -p osd1-server

dir=$(mktemp -d)
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
OSD1_DB_PATH="$dir/osd1.db" OSD1_HTTP_ADDR="127.0.0.1:$port" \
  ./target/debug/osd1 >"$dir/server.log" 2>&1 &
pid=$!
cleanup() {
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  rm -rf "$dir"
}
trap cleanup EXIT

i=0
while [ "$i" -lt 50 ]; do
  if curl -fsS "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 0.1
done

python3 - "$port" <<'PY'
import json, sys, time, urllib.request
port = sys.argv[1]
url = f"http://127.0.0.1:{port}/accounts/bench/d1/database/bench/query"

def post(sql, params=None):
    body = {"sql": sql}
    if params is not None:
        body["params"] = params
    data = json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, headers={"content-type": "application/json"})
    started = time.perf_counter()
    with urllib.request.urlopen(request, timeout=10) as response:
        payload = response.read()
    elapsed = time.perf_counter() - started
    return elapsed, payload

post("CREATE TABLE bench (id INTEGER PRIMARY KEY, n INTEGER)")
started = time.perf_counter()
inserts = []
for n in range(1000):
    elapsed, _ = post("INSERT INTO bench (n) VALUES (?)", [str(n)])
    inserts.append(elapsed)
selects = []
for n in range(1000):
    elapsed, payload = post("SELECT n FROM bench WHERE n = ?", [str(n)])
    selects.append(elapsed)
    if n == 0 or n == 999:
        decoded = json.loads(payload)
        assert decoded["result"][0]["results"][0]["n"] == n
samples = inserts + selects
print(f"requests {len(samples)}")
print(f"wall_seconds {time.perf_counter() - started:.6f}")
print(f"insert_avg_ms {sum(inserts)/len(inserts)*1000:.3f}")
print(f"select_avg_ms {sum(selects)/len(selects)*1000:.3f}")
print(f"max_ms {max(samples)*1000:.3f}")
PY
