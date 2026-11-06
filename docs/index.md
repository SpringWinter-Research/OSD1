---
title: OSD1
---

{% include nav.html %}

# OSD1

OSD1 is a small HTTP server for one SQLite file. You send ordinary SQLite SQL. The `/query` and `/raw` routes match the shape used by the official Cloudflare `cloudflare` TypeScript SDK.

Each request is one transaction on one connection. One thread runs all of the SQL. If too many requests are already waiting, the server answers `429` and moves on.

Source: [SpringWinter-Research/OSD1](https://github.com/SpringWinter-Research/OSD1). License: [MIT](https://github.com/SpringWinter-Research/OSD1/blob/main/LICENSE).

## Run it

The public image is [`public.ecr.aws/c5l0h3s1/osd1`](https://gallery.ecr.aws/c5l0h3s1/osd1). Tags `latest` and `2026-11-06` are `linux/arm64` and `linux/amd64`.

```bash
docker pull public.ecr.aws/c5l0h3s1/osd1:latest

docker run --rm -p 8080:8080 \
  -e OSD1_API_TOKEN=dev-token \
  -v osd1-data:/data \
  public.ecr.aws/c5l0h3s1/osd1:latest
```

```bash
curl -sS http://127.0.0.1:8080/health
```

```json
{"ok":true}
```

```bash
curl -sS -H "Authorization: Bearer dev-token" -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  http://127.0.0.1:8080/accounts/a/d1/database/b/query
```

`/query` returns one JSON object per row. `/raw` returns column names and array rows. The account id and database id in the URL are ignored, because there is only one database. Health does not need the token.

From the repo, `docker compose -f deploy/compose.yml up -d --build` does the same job and uses `dev-token` when you do not set one.

A TypeScript client looks like this (`cloudflare@4.5.0`):

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

`node spike/sdk-compat.mjs` ran that client and printed `sdk-compat ok`. Where the server and the SDK differ is in [compatibility](compatibility.md).

## Performance

These numbers are from an Apple M4 on 6 Nov 2026. They are loopback reads of a release build. A busy disk, TLS, or a slow client will be lower. The raw logs are on the [benchmark](benchmark.md) page.

**How many `SELECT 1` requests the server can take.** Keep-alive HTTP, no token. At 32 clients the rate stopped climbing, which means the SQL thread was full.

| Clients | Requests/s |
| ---: | ---: |
| 1 | 25,467 |
| 8 | 82,129 |
| 16 | 102,234 |
| 32 | 111,075 |

SQLite alone on the same machine, with no HTTP:

| What | Ops/s |
| --- | ---: |
| In-memory transaction, `SELECT 1` | 2,516,325 |
| WAL file, `synchronous=FULL`, `SELECT 1` | 543,065 |
| Indexed reads inside one transaction | 3,447,361 |

**Four workloads, one client at a time.** Release binary, 4,000 indexed rows already loaded, 2,000 calls each. Latency is what the client saw. Memory is the high sample of the server process.

| Workload | Mix | p50 | p95 | p99 | Requests/s | Memory |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Read-heavy | 95% indexed select, 5% insert | 0.17 ms | 0.49 ms | 0.78 ms | 2,335 | 5.8 MB |
| Balanced | half select, half insert/update | 0.16 ms | 0.47 ms | 0.95 ms | 2,554 | 5.4 MB |
| Write-heavy | 10% select, 90% insert/update | 0.14 ms | 0.48 ms | 1.64 ms | 2,568 | 4.8 MB |
| Mixed | short selects, a 2,000-row scan every 20th call | 0.16 ms | 0.36 ms | 0.45 ms | 3,413 | 6.5 MB |

Those four ran one after another in a single Python client, so they show latency for that client, not the 111k ceiling above. A separate debug build of 1,000 inserts and 1,000 point selects finished in 0.52 seconds (about 0.22 ms per insert and 0.28 ms per select).

## Tests

Run on the same day. `cargo test --workspace` passed: engine 11, HTTP 18, server 1, suite 9, types 5.

| Area | Result |
| --- | --- |
| SQL | CRUD, DDL, joins, indexes, and a CTE |
| Types | null, integers, floats, and text. Blob results come back as HTTP 400 |
| Transactions | Commit, rollback, and a constraint failure. Client `BEGIN` / `COMMIT` / `ROLLBACK` are rejected |
| Concurrency | 10, 100, and 500 clients at once, every inserted row present |
| Queue | A full queue returns 429. A request that never finishes returns 504. The SQL thread is not cancelled mid-statement |
| SDK | Official TypeScript client, `query` and `raw` |
| Persistence | A row survived a container restart and a recreate. `/data` was mode 700, the database and WAL mode 600 |
| Backup | Litestream restore returned the marker row and `PRAGMA integrity_check` was `ok` |
| Recovery | A killed process kept committed rows. A deleted volume no longer has the table. A restore to a closed port fails and writes no file |
| TLS | nginx on port 8443. Missing and wrong tokens are 401. The right token returns the row |
| Fuzzing | Malformed bodies stay HTTP 400 or 413 |
| Dependencies | `cargo audit` found no advisories in 77 crates |
| Image scan | Trivy reported 225 Debian findings on the arm64 image: 85 low, 103 medium, 36 high, 1 critical (`zlib` CVE-2023-45853, will not fix) |

Commands and the per-check notes are on the [testing](testing.md) page.

## When something fails

| You see | Meaning |
| --- | --- |
| 200 | The SQL ran |
| 400, code 7500 | SQLite or the authorizer said no, or the JSON answer is over the size limit |
| 400, code 7400 | The body, the SQL, or a parameter is malformed |
| 401, code 10000 | Missing or wrong bearer token |
| 413 | The body is over `OSD1_MAX_BODY_BYTES` (1 MiB). SQL does not run |
| 429, code 10001 | The queue is full. `x-should-retry` is `false` |
| 504, code 7500 | Still waiting after `OSD1_REQUEST_TIMEOUT_MS` (30 seconds). The statement keeps running |
| 500, code 7500 | The SQL thread is gone |

Leave the token empty and auth is off. Parameters can be null, text, integers, or finite floats.

`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ATTACH`, `DETACH`, `writable_schema`, `load_extension`, `readfile`, and `writefile` are refused. Several statements in one request share the transaction, and a later failure rolls the whole request back.

## Settings

| Variable | Default | What it does |
| --- | --- | --- |
| `OSD1_DB_PATH` | `./data/osd1.db` | The database file. The image uses `/data/osd1.db` |
| `OSD1_HTTP_ADDR` | `127.0.0.1:8080` | Listen address. The image uses `0.0.0.0:8080` |
| `OSD1_API_TOKEN` | empty | Bearer token |
| `OSD1_QUEUE_CAPACITY` | `64` | How many requests may wait |
| `OSD1_MAX_BODY_BYTES` | 1 MiB | Largest request body |
| `OSD1_MAX_RESPONSE_BYTES` | 1 MiB | Largest success JSON |
| `OSD1_REQUEST_TIMEOUT_MS` | `30000` | `0` means wait forever |
| `OSD1_SERVED_BY_COLO` | `LOC` | Copied into the result metadata |

A directory the server creates is mode `0700`. The database, WAL, and shared-memory files are mode `0600`. The token is redacted from the startup log line.

Recorded requests are in the [API examples](api-examples.md). Running the image is in [operations](operations.md). Backup is in [backup and restore](backup-restore.md). The image scan is in the [security scan](security-scan.md).
