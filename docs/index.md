---
title: Overview
description: One SQLite database behind an HTTP API. Architecture, system design, and operations.
---

# OSD1

OSD1 is one SQLite file behind an HTTP API. Clients send native SQL in the Cloudflare D1 request shape. One thread executes that SQL. Raft, when enabled at startup, replicates committed statements. Litestream, when you run it, copies the leader’s file from outside the process.

## Start here

<div class="docs-grid">
  <a class="docs-card" href="#quick-start">
    <span class="docs-kicker">Get started</span>
    <span class="docs-card-title">Quick start</span>
    <span class="docs-card-text">Pull the image, check health, and run a query.</span>
  </a>
  <a class="docs-card" href="architecture.html">
    <span class="docs-kicker">Architecture</span>
    <span class="docs-card-title">Overview</span>
    <span class="docs-card-text">Client, distribution, and storage. One SQL thread and an optional raft log.</span>
  </a>
  <a class="docs-card" href="system-design.html">
    <span class="docs-kicker">Architecture</span>
    <span class="docs-card-title">System design</span>
    <span class="docs-card-text">As-built review of the HTTP surface, the SQL thread, and the raft directory.</span>
  </a>
  <a class="docs-card" href="low-level.html">
    <span class="docs-kicker">Architecture</span>
    <span class="docs-card-title">Low-level design</span>
    <span class="docs-card-text">Locks, the command id, the log record, and idempotent apply.</span>
  </a>
</div>

## Readers

<div class="docs-audiences">
  <div>
    <p class="docs-kicker">Develop</p>
    <p>Call <code>/query</code> and <code>/raw</code> with the Cloudflare client. Start with the <a href="api-examples.html">API examples</a> and the <a href="compatibility.html">SDK differences</a>.</p>
  </div>
  <div>
    <p class="docs-kicker">Operate</p>
    <p>Run one process per database. See <a href="operations.html">operations</a>, <a href="monitoring.html">monitoring</a>, and <a href="backup-restore.html">backup and restore</a>.</p>
  </div>
  <div>
    <p class="docs-kicker">Design</p>
    <p>The layer picture is the <a href="architecture.html">architecture</a>. Locks and bytes are the <a href="low-level.html">low-level design</a>. The review is the <a href="system-design.html">system design</a>.</p>
  </div>
</div>

## Quick start

**1. Start the server**

```bash
docker pull public.ecr.aws/c5l0h3s1/osd1:latest

docker run --rm -p 8080:8080 \
  -e OSD1_API_TOKEN=dev-token \
  -v osd1-data:/data \
  public.ecr.aws/c5l0h3s1/osd1:latest
```

`latest` and `2026-11-06` include `linux/arm64` and `linux/amd64`. The database file is `/data/osd1.db`. To build from the repo instead: `docker compose -f deploy/compose.yml up -d --build`.

**2. Check that it is up**

```bash
curl -sS http://127.0.0.1:8080/health
```

```json
{"ok":true,"ready":true}
```

**3. Run a query**

```bash
curl -sS -H "Authorization: Bearer dev-token" -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  http://127.0.0.1:8080/accounts/a/d1/database/b/query
```

`/query` returns one JSON object per row. `/raw` returns column names and array rows. The ids in the path are ignored unless `OSD1_DATABASE_ID` or `OSD1_ROUTES` is set. One process still has one database. Health does not use the token. While the process is up the status stays 200. In raft mode `ready` is false when apply lags commit by more than 32. A node that is leaving returns 503 and `{"ok":false,"ready":false}`.

From source:

```bash
cargo run -p osd1-server
```

`OSD1_DB_PATH` defaults to `./data/osd1.db` and `OSD1_HTTP_ADDR` defaults to `127.0.0.1:8080`.

## Usage

```js
import Cloudflare from "cloudflare";

const client = new Cloudflare({
  apiToken: "dev-token",
  baseURL: "http://127.0.0.1:8080",
  maxRetries: 0,
});

for await (const row of client.d1.database.query(
  "00000000-0000-0000-0000-000000000001",
  { account_id: "023e105f4ecef8ad9ca31a8372d0c353", sql: "SELECT 1 AS n" },
)) {
  console.log(row);
}
```

`node spike/sdk-compat.mjs` runs this client against a local binary. The last run printed `sdk-compat ok`. Differences from the SDK are listed in [compatibility](compatibility.md).

| Status | Code | Meaning |
| --- | --- | --- |
| 200 | | The SQL ran |
| 400 | 7500 | SQLite or the authorizer rejected it, or the JSON answer is too large |
| 400 | 7400 | Malformed body, SQL, parameter, or `x-osd1-min-commit-index` |
| 401 | 10000 | Missing or wrong bearer token |
| 413 | | Body is over the size limit. SQL does not run |
| 429 | 10001 | The queue is full (`x-should-retry: false`) |
| 504 | 7500 | Timed out. Simple mode and raft reads interrupt the statement at `OSD1_REQUEST_TIMEOUT_MS`. A raft read waiting on `x-osd1-min-commit-index` uses that same deadline. A raft write waits for raft |
| 500 | 7500 | The SQL thread has stopped |
| 503 | 7500 | The node is leaving the cluster. New SQL is not queued. Message `node is leaving the cluster` |

Parameters may be null, text, integers, or finite floats. `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ATTACH`, and `DETACH` are refused. Several statements in one request share one transaction in simple mode, on a leader read, and on a raft apply. A follower read steps its statements with no transaction.

## Features

- One SQLite file, one connection, one thread that runs SQL
- Native SQL, with parameters bound by SQLite
- One transaction per HTTP request in simple mode, on a leader read, and on a raft apply
- A fixed-size queue. Extra requests get 429
- Bearer-token authentication
- A table explorer at `/` and `/explorer`
- `GET /metrics` in Prometheus text format, for Grafana
- Optional Litestream sidecar for backup to S3
- Optional Raft mode: followers serve local reads and forward writes to the leader. A write returns `x-osd1-commit-index`. A read can send it back as `x-osd1-min-commit-index` and wait for this node to apply that far. `/health` stays 200 while the process is up. `ready` is false when apply lags commit by more than 32
- Published images for `linux/arm64` and `linux/amd64`

The read wait uses the metrics watch and the same deadline as the SQL statement. A stored apply body younger than 3 seconds survives a log purge until a later purge. That clock is memory on the SQL thread. Removing a member writes `<database>.raft/removed`, returns 503, finishes SQL already accepted (up to 5 seconds), and then stops HTTP. The byte layout is on the [low-level design](low-level.md) page. The layer picture is on the [architecture](architecture.md) page. What to alert on is on the [monitoring](monitoring.md) page.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `OSD1_DB_PATH` | `./data/osd1.db` | Database file. The image uses `/data/osd1.db` |
| `OSD1_HTTP_ADDR` | `127.0.0.1:8080` | Listen address. The image uses `0.0.0.0:8080` |
| `OSD1_API_TOKEN` | empty | Bearer token. Empty disables auth |
| `OSD1_QUEUE_CAPACITY` | `64` | Requests allowed to wait |
| `OSD1_MAX_BODY_BYTES` | 1 MiB | Largest request body |
| `OSD1_MAX_RESPONSE_BYTES` | 1 MiB | Largest success JSON |
| `OSD1_REQUEST_TIMEOUT_MS` | `30000` | Deadline for simple mode and for a raft read, including a read that waits on `x-osd1-min-commit-index`. The statement is interrupted. `0` waits without a deadline. A raft write waits for raft |
| `OSD1_SERVED_BY_COLO` | `LOC` | Value copied into result metadata |
| `OSD1_NODE_ID` | unset | Raft node id, a `u64`. Set with the variables below to leave simple mode |
| `OSD1_RAFT_LISTEN` | unset | Raft TCP address, `host:port` |
| `OSD1_RAFT_SECRET` | unset | Shared secret on the raft port |
| `OSD1_RAFT_BOOTSTRAP` | unset | `1` or `true` starts a new cluster. Ignored when `<database>.raft` already has state |
| `OSD1_RAFT_JOIN` | unset | Raft address of a node already in the cluster. Ignored when this node's raft directory already has state |
| `OSD1_RAFT_VOTER` | unset | `1` or `true` makes this join a voter. The default join is a learner |
| `OSD1_RAFT_REMOVE` | unset | Node id to drop. Also set `OSD1_RAFT_JOIN` and `OSD1_RAFT_SECRET`. The caller exits after the call. The removed node returns 503, finishes accepted SQL for up to 5 seconds, then exits. A later start of that database exits too |
| `OSD1_ROUTER` | unset | `1` or `true` serves the route table and does not open a database |
| `OSD1_ROUTES` | unset | File of `database_id http://host:port` lines. Blank lines and `#` comments are skipped |
| `OSD1_DATABASE_ID` | unset | Path id this process serves from its own file |
| `OSD1_LITESTREAM_HEARTBEAT` | unset | File the sidecar touches. `/metrics` then exports `osd1_litestream_last_ok_seconds` |

A directory OSD1 creates is mode `0700`. The database, WAL, shared-memory, and raft files are mode `0600`. The API token is redacted from the startup log. How the files fit together is on the [architecture](architecture.md) page.

## Benchmarks

Apple M4, 6 Nov 2026, release build, loopback. `SELECT 1` over keep-alive HTTP. At 32 clients the rate stopped climbing: the SQL thread was full.

{% include scale-chart.html %}

| Clients | Requests/s |
| ---: | ---: |
| 1 | 25,467 |
| 8 | 82,129 |
| 16 | 102,234 |
| 32 | 111,075 |

On the same machine, SQLite with no HTTP did 2,516,325 in-memory transactions/s, 543,065 WAL transactions/s with `synchronous=FULL`, and 3,447,361 indexed reads/s inside one transaction.

A later run, 25 Nov 2026, kept the SQL the same and changed one thing: in-process SQLite 3.46.0 versus OSD1 over one keep-alive connection. Indexed point select, one transaction per call, one client, 2,000 calls.

| Engine | p50 | Calls/s |
| --- | ---: | ---: |
| SQLite, in process | 0.002 ms | 414,450 |
| OSD1 HTTP | 0.033 ms | 27,513 |

The setup, the `SELECT 1` pair, and the raw output are on the [benchmark](benchmark.md#same-sql-one-client) page. That run does not change the 111,075 ceiling above. It is not a full-text result from another site’s Hacker News suite.

A single Python client against four workloads (2,000 calls each, 4,000 indexed rows loaded):

{% include latency-chart.html %}

| Workload | p50 | p95 | p99 | Requests/s |
| --- | ---: | ---: | ---: | ---: |
| Read-heavy (95% select) | 0.17 ms | 0.49 ms | 0.78 ms | 2,335 |
| Balanced | 0.16 ms | 0.47 ms | 0.95 ms | 2,554 |
| Write-heavy (90% write) | 0.14 ms | 0.48 ms | 1.64 ms | 2,568 |
| Mixed, with a large scan | 0.16 ms | 0.36 ms | 0.45 ms | 3,413 |

Peak resident memory in that run was 6.5 MB. Full output is on the [benchmark](benchmark.md) page.

## Testing

```bash
cargo test --workspace
node spike/sdk-compat.mjs
./scripts/profiles.sh
```

`cargo test --workspace` passed on 6 Nov 2026: engine 11, HTTP 18, server 1, suite 9, types 5. That run covered SQL, types, transactions, 10/100/500 clients, a full queue, the SDK, process kill, and malformed input.

Docker checks use port 8080. Run them one at a time:

```bash
./scripts/persist.sh          # restart and recreate keep the row
./scripts/tls-check.sh        # TLS terminates at nginx
./scripts/recovery-cases.sh   # missing volume, failed restore
./scripts/recovery.sh         # Litestream restore and integrity check
./scripts/scan.sh             # cargo audit and Trivy
```

`cargo audit` reported no advisories in 77 crates. Trivy reported 225 findings in the Debian base image, including one critical `zlib` CVE. Details are in [testing](testing.md) and the [security scan](security-scan.md).

## Documentation

- [Architecture](architecture.md)
- [System design](system-design.md)
- [Low-level design](low-level.md)
- [API examples](api-examples.md)
- [SDK compatibility](compatibility.md)
- [Operations](operations.md)
- [Monitoring](monitoring.md)
- [Backup and restore](backup-restore.md)
- [Benchmarks](benchmark.md)
- [Testing](testing.md)
- [Security scan](security-scan.md)

## License

[MIT](https://github.com/SpringWinter-Research/OSD1/blob/main/LICENSE)
