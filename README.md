# OSD1

OSD1 is an HTTP server for one SQLite database. Clients send native SQLite SQL. `POST /query` and `POST /raw` follow the Cloudflare D1 REST shape used by the official `cloudflare` TypeScript SDK.

One OS thread owns the only SQLite connection and drains a bounded FIFO queue. Each request is one transaction. The process does not keep a client transaction open after the response. Litestream, when used, runs as a sidecar and is not linked into the binary.

License: [MIT](LICENSE).

## Image

Public registry: [`public.ecr.aws/c5l0h3s1/osd1`](https://gallery.ecr.aws/c5l0h3s1/osd1)

| Tag | Digest | Platforms |
| --- | --- | --- |
| `latest`, `2026-11-06` | `sha256:8c36d25fe8f3b7b0aa8a0a4c9be1f5f917a0e4b5a450c50fc5a14db316c44d2c` | `linux/arm64`, `linux/amd64` |

```bash
docker pull public.ecr.aws/c5l0h3s1/osd1:latest

docker run --rm -p 8080:8080 \
  -e OSD1_API_TOKEN=dev-token \
  -v osd1-data:/data \
  public.ecr.aws/c5l0h3s1/osd1:latest
```

The process listens on `0.0.0.0:8080` and stores the database at `/data/osd1.db`. The image runs as uid `10001`. `docker pull` selects `linux/arm64` or `linux/amd64`. The image is Debian bookworm. A Trivy scan of the arm64 image reported 225 OS findings, including one critical `zlib` CVE. The amd64 image uses the same base. Details are in [docs/security-scan.md](docs/security-scan.md).

Build the same image from this repo:

```bash
docker compose -f deploy/compose.yml up -d --build
```

Compose sets `OSD1_API_TOKEN` to `dev-token` when the variable is unset, and mounts a named volume at `/data`.

## Query

Health does not use the token and does not touch SQLite.

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

`/query` returns one JSON object per row. `/raw` returns `{ "columns", "rows" }`. The account id and database id in the path are ignored. OSD1 has one database file. Recorded request and response bodies are in [docs/api-examples.md](docs/api-examples.md).

Official client (`cloudflare@4.5.0`):

```js
import Cloudflare from "cloudflare";

const client = new Cloudflare({
  apiToken: "dev-token",
  baseURL: "http://127.0.0.1:8080",
  maxRetries: 0,
});

for await (const row of client.d1.database.query(
  "00000000-0000-0000-0000-000000000001",
  {
    account_id: "023e105f4ecef8ad9ca31a8372d0c353",
    sql: "SELECT 1 AS n",
  },
)) {
  console.log(row);
}
```

`node spike/sdk-compat.mjs` runs that client against a local binary. The last run printed `sdk-compat ok`. Where the server and the SDK differ is written down in [docs/compatibility.md](docs/compatibility.md).

## HTTP

| Status | Code | When |
| --- | --- | --- |
| 200 | | The statement ran |
| 400 | 7500 | SQLite or the authorizer rejected the SQL, or the success JSON is larger than the response limit |
| 400 | 7400 | The JSON body, SQL, or parameters are malformed |
| 401 | 10000 | The bearer token is missing or wrong. An empty `OSD1_API_TOKEN` turns auth off |
| 413 | | The body is larger than `OSD1_MAX_BODY_BYTES`. SQL does not run |
| 429 | 10001 | The queue is full. The response includes `x-should-retry: false` |
| 504 | 7500 | The queue wait plus SQL exceeded `OSD1_REQUEST_TIMEOUT_MS`. The response includes `x-should-retry: false`. SQLite is not interrupted |
| 500 | 7500 | The SQL thread is gone |

Parameters may be JSON null, strings, integers, or finite floats. Booleans, objects, and arrays are rejected. Blob results are rejected.

`BEGIN`, `COMMIT`, `ROLLBACK`, and `SAVEPOINT` from client SQL are denied. `ATTACH`, `DETACH`, `PRAGMA writable_schema`, `load_extension`, `readfile`, and `writefile` are denied. Several statements in one request share one transaction: if a later statement fails, the request rolls back.

## Configuration

| Variable | Default | |
| --- | --- | --- |
| `OSD1_DB_PATH` | `./data/osd1.db` | The only database file |
| `OSD1_HTTP_ADDR` | `127.0.0.1:8080` | The image sets `0.0.0.0:8080` |
| `OSD1_API_TOKEN` | empty | Bearer token. Empty means no auth |
| `OSD1_QUEUE_CAPACITY` | `64` | Waiting requests. Further requests get 429 |
| `OSD1_SERVED_BY_COLO` | `LOC` | Copied into result meta |
| `OSD1_MAX_BODY_BYTES` | `1048576` | |
| `OSD1_MAX_RESPONSE_BYTES` | `1048576` | |
| `OSD1_REQUEST_TIMEOUT_MS` | `30000` | `0` waits without a deadline |

A new database directory is created mode `0700`. The database, WAL, and shared-memory files are mode `0600`. An existing parent directory is left as it is. The image creates `/data` as mode `0700` owned by uid `10001`. The listen line redacts the API token.

TLS stops at a reverse proxy. `deploy/nginx-tls.conf` and `./scripts/tls-check.sh` are that setup. The OSD1 process stays on HTTP.

## Server

- One database file.
- One OS thread executes SQL. Tokio worker threads never call SQLite.
- That thread owns the only connection.
- Requests wait on a bounded FIFO. A full queue returns 429 and does not block.
- SQL is native SQLite. Parameters are bound with the SQLite bind API.
- The SQLite authorizer accepts or rejects statements. The server does not decide that by scanning SQL text.
- The on-disk format is upstream SQLite.

On an Apple M4, a release build serving `SELECT 1` over keep-alive HTTP reached 111,075 requests/s with 32 clients, and 25,467 requests/s with one client. Sixteen clients reached 102,234 requests/s, so more clients had mostly stopped helping: the SQL thread was full. That is a loopback read on this Mac. Writes, TLS, and a Linux volume with a real flush are slower. The same machine, calling SQLite with no HTTP, committed a `synchronous=FULL` WAL transaction at 543,065/s. Measured runs are in [docs/benchmark.md](docs/benchmark.md).

## Backup

Litestream replicates the database to an S3-compatible bucket. `./scripts/recovery.sh` inserts a row, deletes the primary files, restores them, and checks `PRAGMA integrity_check`. The last run exited 0 and the check returned `ok`. Steps and the recorded output are in [docs/backup-restore.md](docs/backup-restore.md).

## Development

```bash
cargo test --workspace
node spike/sdk-compat.mjs
./scripts/profiles.sh
```

Docker checks, in this order because they all publish port 8080:

```bash
./scripts/persist.sh
./scripts/tls-check.sh
./scripts/recovery-cases.sh
./scripts/recovery.sh
./scripts/scan.sh
```

What each check covers, and the recorded results, is in [docs/testing.md](docs/testing.md). Running the image, including a restart that kept the volume, is in [docs/operations.md](docs/operations.md).

Rust toolchain used for the recorded tests: 1.99.1. The workspace crates are `osd1-types`, `osd1-engine`, `osd1-http`, and `osd1-server`. The binary is `osd1`.
