# OSD1

OSD1 is an HTTP server for one SQLite database. You send native SQLite SQL. `/query` and `/raw` follow the Cloudflare D1 REST shape used by the official [`cloudflare`](https://www.npmjs.com/package/cloudflare) TypeScript SDK.

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
{"ok":true}
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

`node spike/sdk-compat.mjs` runs this client against a local binary. The last run printed `sdk-compat ok`. Differences from the SDK are listed in [docs/compatibility.md](docs/compatibility.md).

| Status | Code | Meaning |
| --- | --- | --- |
| 200 | | The SQL ran |
| 400 | 7500 | SQLite or the authorizer rejected it, or the JSON answer is too large |
| 400 | 7400 | Malformed body, SQL, or parameter |
| 401 | 10000 | Missing or wrong bearer token |
| 413 | | Body is over the size limit. SQL does not run |
| 429 | 10001 | The queue is full (`x-should-retry: false`) |
| 504 | 7500 | Timed out waiting. The statement keeps running |
| 500 | 7500 | The SQL thread has stopped |

Parameters may be null, text, integers, or finite floats. `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ATTACH`, and `DETACH` are refused. Several statements in one request share one transaction.

## Features

- One SQLite file, one connection, one thread that runs SQL
- Native SQL, with parameters bound by SQLite
- One transaction per HTTP request
- A fixed-size queue. Extra requests get 429
- Bearer-token authentication
- A table explorer at `/` and `/explorer`
- `GET /metrics` in Prometheus text format, for Grafana
- Optional Litestream sidecar for backup to S3
- Published images for `linux/arm64` and `linux/amd64`

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `OSD1_DB_PATH` | `./data/osd1.db` | Database file. The image uses `/data/osd1.db` |
| `OSD1_HTTP_ADDR` | `127.0.0.1:8080` | Listen address. The image uses `0.0.0.0:8080` |
| `OSD1_API_TOKEN` | empty | Bearer token. Empty disables auth |
| `OSD1_QUEUE_CAPACITY` | `64` | Requests allowed to wait |
| `OSD1_MAX_BODY_BYTES` | 1 MiB | Largest request body |
| `OSD1_MAX_RESPONSE_BYTES` | 1 MiB | Largest success JSON |
| `OSD1_REQUEST_TIMEOUT_MS` | `30000` | `0` waits without a deadline |
| `OSD1_SERVED_BY_COLO` | `LOC` | Value copied into result metadata |

A directory OSD1 creates is mode `0700`. The database, WAL, and shared-memory files are mode `0600`. The API token is redacted from the startup log.

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
