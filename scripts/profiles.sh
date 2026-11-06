#!/bin/sh
# Release-binary latency profiles. Prints p50/p95/p99, throughput, and max RSS.
# Numbers are client-observed HTTP time against one local process.
set -eu
cd "$(dirname "$0")/.."

. "$HOME/.cargo/env"
cargo build -q --release -p osd1-server

dir=$(mktemp -d)
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
OSD1_DB_PATH="$dir/osd1.db" OSD1_HTTP_ADDR="127.0.0.1:$port" \
  ./target/release/osd1 >"$dir/server.log" 2>&1 &
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
if ! curl -fsS "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
  echo "osd1 did not become healthy" >&2
  cat "$dir/server.log" >&2 || true
  exit 1
fi

echo "binary target/release/osd1"
echo "host $(uname -sm)"
python3 - "$port" "$pid" <<'PY'
import json, math, subprocess, sys, time, urllib.error, urllib.request

port, pid = sys.argv[1], sys.argv[2]
url = f"http://127.0.0.1:{port}/accounts/bench/d1/database/bench/query"
ops = 2000
preload = 4000

def post(sql, params=None):
    body = {"sql": sql}
    if params is not None:
        body["params"] = params
    data = json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, headers={"content-type": "application/json"})
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = response.read()
            status = response.status
    except urllib.error.HTTPError as error:
        payload = error.read()
        status = error.code
    elapsed = time.perf_counter() - started
    if status != 200:
        raise SystemExit(f"HTTP {status} for {sql[:80]}: {payload[:300]!r}")
    return elapsed, payload

def rss_kb():
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", pid], text=True).strip()
    return int(out.split()[0]) if out else 0

def nearest_rank(samples, pct):
    ordered = sorted(samples)
    rank = math.ceil(pct / 100 * len(ordered))
    rank = min(max(rank, 1), len(ordered))
    return ordered[rank - 1]

post("CREATE TABLE items (id INTEGER PRIMARY KEY, n INTEGER NOT NULL, touch INTEGER NOT NULL)")
post("CREATE INDEX items_n ON items (n)")
values = ",".join(f"({i},{i},0)" for i in range(preload))
post(f"INSERT INTO items (id, n, touch) VALUES {values}")
_, plan = post("EXPLAIN QUERY PLAN SELECT touch FROM items WHERE n = ?", [0])
if b"items_n" not in plan:
    raise SystemExit(f"indexed select did not use items_n: {plan!r}")
print(f"preload_rows {preload}")
print("index_plan items_n")

next_id = preload

def select():
    n = next_id % preload
    return post("SELECT touch FROM items WHERE n = ?", [n])

def insert():
    global next_id
    row_id = next_id
    next_id += 1
    return post("INSERT INTO items (id, n, touch) VALUES (?, ?, 0)", [row_id, row_id])

def update():
    row_id = next_id % preload
    return post("UPDATE items SET touch = touch + 1 WHERE id = ?", [row_id])

def scan():
    return post("SELECT n FROM items WHERE n < 2000")

def run(name, choose):
    samples = []
    peak = rss_kb()
    started = time.perf_counter()
    for i in range(ops):
        elapsed, _ = choose(i)
        samples.append(elapsed)
        if i % 25 == 0:
            peak = max(peak, rss_kb())
    wall = time.perf_counter() - started
    peak = max(peak, rss_kb())
    print(f"profile {name}")
    print(f"ops {ops}")
    print(f"wall_seconds {wall:.6f}")
    print(f"throughput_ops_per_s {ops / wall:.3f}")
    print(f"p50_ms {nearest_rank(samples, 50) * 1000:.3f}")
    print(f"p95_ms {nearest_rank(samples, 95) * 1000:.3f}")
    print(f"p99_ms {nearest_rank(samples, 99) * 1000:.3f}")
    print(f"max_ms {max(samples) * 1000:.3f}")
    print(f"max_rss_kb {peak}")

# 95% indexed SELECT, 5% INSERT (1 of every 20).
run("read-heavy", lambda i: insert() if i % 20 == 0 else select())
# 50% SELECT, 50% INSERT/UPDATE.
run("balanced", lambda i: select() if i % 2 == 0 else (insert() if i % 4 == 1 else update()))
# 10% SELECT, 90% INSERT/UPDATE (2/20 select, 9/20 insert, 9/20 update).
def write_heavy(i):
    slot = i % 20
    if slot < 2:
        return select()
    if slot < 11:
        return insert()
    return update()
run("write-heavy", write_heavy)
# Short indexed selects, with a 2,000-row scan every 20th call.
run("mixed-latency", lambda i: scan() if i % 20 == 0 else select())
PY
