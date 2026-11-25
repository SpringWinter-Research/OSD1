---
title: Overview
description: One SQLite database file behind an HTTP API.
---

# OSD1

OSD1 is one SQLite database file behind an HTTP API. You send native SQL. The request shape matches the Cloudflare D1 client.

## The problem

A SQLite file has one writer. An application still needs HTTP, a limit on waiting requests, and a second copy of each committed write.

## The solution

OSD1 runs that file on one thread. One HTTP request is one transaction. A full queue returns HTTP 429. Raft mode copies each committed write to other processes. Litestream, when you run it, copies the leader file from outside the process.

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
    <span class="docs-card-text">Client, distribution, and storage.</span>
  </a>
  <a class="docs-card" href="system-design.html">
    <span class="docs-kicker">Architecture</span>
    <span class="docs-card-title">System design</span>
    <span class="docs-card-text">How a request works, and what can go wrong.</span>
  </a>
  <a class="docs-card" href="low-level.html">
    <span class="docs-kicker">Architecture</span>
    <span class="docs-card-title">Low-level design</span>
    <span class="docs-card-text">Locks, command ids, and the log record.</span>
  </a>
</div>

## Readers

<div class="docs-audiences">
  <div>
    <p class="docs-kicker">Develop</p>
    <p>Call <code>/query</code> and <code>/raw</code>. Start with the <a href="api-examples.html">API examples</a> and the <a href="compatibility.html">SDK differences</a>.</p>
  </div>
  <div>
    <p class="docs-kicker">Operate</p>
    <p>Run one process for each database. See <a href="operations.html">operations</a>, <a href="monitoring.html">monitoring</a>, and <a href="backup-restore.html">backup and restore</a>.</p>
  </div>
  <div>
    <p class="docs-kicker">Design</p>
    <p>The layers are the <a href="architecture.html">architecture</a>. The bytes are the <a href="low-level.html">low-level design</a>. The limits are the <a href="system-design.html">system design</a>.</p>
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

The tags `latest` and `2026-11-06` include `linux/arm64` and `linux/amd64`. The database file is `/data/osd1.db`. To build from the repo, run `docker compose -f deploy/compose.yml up -d --build`.

**2. Check that the server is up**

```bash
curl -sS http://127.0.0.1:8080/health
```

```json
{"ok":true,"ready":true}
```

Open [http://127.0.0.1:8080/](http://127.0.0.1:8080/) for the table explorer. Sign in with the same bearer token. Leave the token blank when the server has no token. Health rules are on the [monitoring](monitoring.md) page.

**3. Run a query**

```bash
curl -sS -H "Authorization: Bearer dev-token" -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  http://127.0.0.1:8080/accounts/a/d1/database/b/query
```

`/query` returns one JSON object for each row. `/raw` returns column names and array rows. The path ids are ignored unless you set `OSD1_DATABASE_ID` or `OSD1_ROUTES`. One process still has one database. Status codes are on the [API examples](api-examples.md) page.

From source:

```bash
cargo run -p osd1-server
```

`OSD1_DB_PATH` defaults to `./data/osd1.db`. `OSD1_HTTP_ADDR` defaults to `127.0.0.1:8080`.

## Highlights

- One SQLite file. One connection. One thread executes SQL.
- Native SQL. Parameters use the SQLite bind API.
- One transaction for each HTTP request in simple mode, on a leader read, and on a raft apply.
- A fixed-size queue. Extra requests get HTTP 429.
- Bearer-token authentication on `/query` and `/raw`.
- A table explorer at `/` and `/explorer`.
- `GET /metrics` in Prometheus text format.
- Optional raft mode. Followers serve local reads and forward writes to the leader.
- Optional Litestream sidecar. It copies the leader file to S3.
- Images for `linux/arm64` and `linux/amd64`.

## Questions

### Does a read see the latest commit?

A follower reads its own file. A read can lag the leader when the client omits `x-osd1-min-commit-index`. Send that header to wait until this node applies that index. The D1 SDK does not send the header. The rule is on the [architecture](architecture.md) page.

### What happens when the queue is full?

OSD1 returns HTTP 429. The header is `x-should-retry: false`. The request does not wait.

### Can I send BEGIN?

No. The authorizer denies `BEGIN`, `COMMIT`, `ROLLBACK`, and `SAVEPOINT`. OSD1 opens the transaction for the request. The full list is on the [architecture](architecture.md) page.

### How do I run more than one database?

Run one process for each database. Set `OSD1_ROUTER=1` and point `OSD1_ROUTES` at those processes. The router does not open SQLite. See [Operations](operations.md).

### How do I copy the database file?

Run Litestream beside the process. The steps are on the [backup and restore](backup-restore.md) page. Litestream is not the commit path.

### What rate is recorded?

The highest recorded rate in simple mode is 111,075 `SELECT 1` requests each second. That run uses 32 clients on an Apple M4. A one-client comparison with in-process SQLite is a different measurement. Both are on the [benchmark](benchmark.md) page.

## Where to read next

- [Architecture](architecture.md)
- [System design](system-design.md)
- [Low-level design](low-level.md)
- [API examples](api-examples.md)
- [SDK compatibility](compatibility.md)
- [Operations](operations.md)
- [Monitoring](monitoring.md)
- [Backup and restore](backup-restore.md)
- [Benchmark](benchmark.md)
- [Testing](testing.md)
- [Security scan](security-scan.md)

## License

[MIT](https://github.com/SpringWinter-Research/OSD1/blob/main/LICENSE)
