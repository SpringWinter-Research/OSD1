# OSD1

OSD1 is an HTTP server for one SQLite database. You send native SQLite SQL. `/query` and `/raw` follow the Cloudflare D1 REST shape used by the official [`cloudflare`](https://www.npmjs.com/package/cloudflare) TypeScript SDK.

Simple mode is the default: one process and one database file. Raft mode is optional. It replicates writes across several processes, and a process chooses it only at startup.

[Documentation](https://springwinter-research.github.io/OSD1/) · [Image](https://gallery.ecr.aws/c5l0h3s1/osd1) · [License: MIT](LICENSE)

## Quick start

**1. Start the server**

```bash
docker pull public.ecr.aws/c5l0h3s1/osd1:latest

docker run --rm -p 8080:8080 \
  -e OSD1_API_TOKEN=dev-token \
  -v osd1-data:/data \
  public.ecr.aws/c5l0h3s1/osd1:latest
```

`latest` and `2026-11-06` include `linux/arm64` and `linux/amd64`. The database file is `/data/osd1.db`. To build from this repo instead: `docker compose -f deploy/compose.yml up -d --build`.

**2. Check that it is up**

```bash
curl -sS http://127.0.0.1:8080/health
```

```json
{"ok":true,"ready":true}
```

Open [http://127.0.0.1:8080/](http://127.0.0.1:8080/) for the table explorer. Sign in with the same bearer token, or leave it blank when the server has no token. The page lists tables, creates them, and runs SQL through `/raw`.

**3. Run a query**

```bash
curl -sS -H "Authorization: Bearer dev-token" -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  http://127.0.0.1:8080/accounts/a/d1/database/b/query
```

`/query` returns one JSON object per row. `/raw` returns column names and array rows. The ids in the path are ignored. There is one database. Health does not use the token.

From source:

```bash
cargo run -p osd1-server
```

That command starts simple mode. `OSD1_DB_PATH` defaults to `./data/osd1.db` and `OSD1_HTTP_ADDR` defaults to `127.0.0.1:8080`. Raft mode is described under [Modes](#modes).

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

`node spike/sdk-compat.mjs` runs this client against a local binary. The last run printed `sdk-compat ok`. Differences from the SDK are listed in [docs/compatibility.md](docs/compatibility.md).

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

## Modes

Simple mode never opens a Raft log. Leave `OSD1_NODE_ID` and every `OSD1_RAFT_*` variable unset. The quick start above is simple mode.

Raft mode starts when any of those variables is set. The process then requires `OSD1_NODE_ID`, `OSD1_RAFT_LISTEN`, `OSD1_RAFT_SECRET`, and exactly one of `OSD1_RAFT_BOOTSTRAP=1` or `OSD1_RAFT_JOIN`. A running process stays in the mode it started in. The process shuts down on SIGINT and SIGTERM.

```bash
# First node. This bootstraps the cluster.
OSD1_NODE_ID=1 \
OSD1_RAFT_LISTEN=127.0.0.1:7001 \
OSD1_RAFT_SECRET=raft-secret \
OSD1_RAFT_BOOTSTRAP=1 \
OSD1_HTTP_ADDR=127.0.0.1:8080 \
OSD1_DB_PATH=./data/node1.db \
OSD1_API_TOKEN=dev-token \
cargo run -p osd1-server

# Second node. It joins the first node's raft address.
OSD1_NODE_ID=2 \
OSD1_RAFT_LISTEN=127.0.0.1:7002 \
OSD1_RAFT_SECRET=raft-secret \
OSD1_RAFT_JOIN=127.0.0.1:7001 \
OSD1_HTTP_ADDR=127.0.0.1:8081 \
OSD1_DB_PATH=./data/node2.db \
OSD1_API_TOKEN=dev-token \
cargo run -p osd1-server
```

Each process has its own database file, HTTP port, node id, and raft port. Every node uses the same `OSD1_RAFT_SECRET`. `OSD1_RAFT_BOOTSTRAP` also accepts `true`.

The bootstrap process is the first leader. A joined node is a learner. It replicates, and it serves local reads. It does not vote. Set `OSD1_RAFT_VOTER=1` on a join when that process should be one of the three voters. Clients call `/query` and `/raw` on any node's HTTP address. A follower serves reads from its own file and sets `served_by_primary` to false. It forwards writes to the leader and returns the usual 200 envelope. `served_by_region` stays `WNAM`. A local read can lag during a partition.

A read that sends `x-osd1-min-commit-index` waits until this node's apply index reaches that value. The wait reads the metrics watch and ends at the read deadline with HTTP 504. `/health` stays 200 while the process is up. The body is `{"ok":true,"ready":true}`. `ready` is false when the commit index is more than 32 ahead of the apply index (`osd1_raft_apply_lag`). The image healthcheck is `wget` of `/health`, so a lagging follower stays up. A node that is leaving returns 503 and `{"ok":false,"ready":false}`. See [Monitoring](docs/monitoring.md).

A write commits when a majority of the voters has stored the log entry. Two voters commit only after both have stored it. Three voters commit after two. Learners do not change that count.

The append-only log, the vote, the membership, and the next command id are files in `<database>.raft` beside the database file. `log.dat` starts with `OSD1LOG2`. Each record carries a CRC-32 of its payload. Applied results live in the SQLite table `osd1_applied`, in the same transaction as the user SQL. After the log purges an index, a stored body applied in the last 3 seconds stays until a later purge. That clock is memory on the SQL thread, so a restart drops it. Snapshots copy pages with SQLite’s backup API. The layout, the write path, and the byte formats are in [Architecture](docs/architecture.md).

Restart a process with the same node id, the same `OSD1_RAFT_LISTEN`, and the same `OSD1_DB_PATH`. An existing raft directory skips bootstrap and join. Bind every voter again before expecting a new write: one voter by itself cannot form a majority, and the process stays up until the others return. A fresh directory still needs `OSD1_RAFT_BOOTSTRAP=1` or `OSD1_RAFT_JOIN`.

`OSD1_RAFT_REMOVE=4`, with `OSD1_RAFT_JOIN` pointed at a live member and the same secret, drops node 4 and exits. The leader tells node 4. That process writes `<database>.raft/removed`, returns 503 from `/health`, rejects new SQL with `node is leaving the cluster`, finishes SQL it has already accepted (up to 5 seconds), stops HTTP, and exits. A later start of the same database exits too. Joins and removals run one at a time. Use removal while a majority is still up.

Litestream, when you run it, stays a sidecar. `scripts/litestream-leader.sh` replicates while this process is the leader (or all the time in simple mode, which has no raft role). Nodes catch up through the raft log. The restore steps for a raft cluster are in [Backup and restore](docs/backup-restore.md).

Raft mode refuses `random`, `randomblob`, `date`, `time`, `datetime`, `julianday`, `strftime`, `unixepoch`, `current_date`, `current_time`, and `current_timestamp`, so a committed statement replays the same way on every node. `last_insert_rowid` stays allowed.

One database is one process. To front several databases, run `OSD1_ROUTER=1` with `OSD1_ROUTES` pointing each database id at `http://host:port`. Set `OSD1_DATABASE_ID` on a process that should serve only that id. Leave both unset and the path ids stay ignored.

`/metrics` adds `osd1_raft_role`, `osd1_raft_term`, `osd1_raft_commit_index`, `osd1_raft_apply_index`, `osd1_raft_apply_lag`, and `osd1_raft_peers` in raft mode. `osd1_raft_role` is 0 for a follower, 1 for a leader, 2 for a candidate, and 3 for a learner.

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

A directory OSD1 creates is mode `0700`. The database, WAL, shared-memory, and raft files are mode `0600`. The API token is redacted from the startup log.

## Benchmarks

Apple M4, 6 Nov 2026, release build, loopback. `SELECT 1` over keep-alive HTTP. At 32 clients the rate stopped climbing: the SQL thread was full.

| Clients | Requests/s |
| ---: | ---: |
| 1 | 25,467 |
| 8 | 82,129 |
| 16 | 102,234 |
| 32 | 111,075 |

On the same machine, SQLite with no HTTP did 2,516,325 in-memory transactions/s, 543,065 WAL transactions/s with `synchronous=FULL`, and 3,447,361 indexed reads/s inside one transaction.

A single Python client against four workloads (2,000 calls each, 4,000 indexed rows loaded):

| Workload | p50 | p95 | p99 | Requests/s |
| --- | ---: | ---: | ---: | ---: |
| Read-heavy (95% select) | 0.17 ms | 0.49 ms | 0.78 ms | 2,335 |
| Balanced | 0.16 ms | 0.47 ms | 0.95 ms | 2,554 |
| Write-heavy (90% write) | 0.14 ms | 0.48 ms | 1.64 ms | 2,568 |
| Mixed, with a large scan | 0.16 ms | 0.36 ms | 0.45 ms | 3,413 |

Peak resident memory in that run was 6.5 MB. Full output is in [docs/benchmark.md](docs/benchmark.md).

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

`cargo audit` reported no advisories in 77 crates. Trivy reported 225 findings in the Debian base image, including one critical `zlib` CVE. Details are in [docs/testing.md](docs/testing.md) and [docs/security-scan.md](docs/security-scan.md).

## Documentation

- [Architecture](docs/architecture.md)
- [System design](docs/system-design.md)
- [API examples](docs/api-examples.md)
- [SDK compatibility](docs/compatibility.md)
- [Operations](docs/operations.md)
- [Monitoring](docs/monitoring.md)
- [Backup and restore](docs/backup-restore.md)
- [Benchmarks](docs/benchmark.md)
- [Testing](docs/testing.md)
- [Security scan](docs/security-scan.md)

## License

[MIT](LICENSE)
