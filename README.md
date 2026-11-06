# OSD1

OSD1 is an open-source HTTP server for one SQLite database. Clients send native SQLite SQL. The `/query` and `/raw` routes follow the Cloudflare D1 REST shape used by the official `cloudflare` TypeScript SDK.

One OS thread owns the only SQLite connection and drains a bounded FIFO queue. A request is one transaction. The process does not keep a client transaction open after the response.

Litestream replication is a separate sidecar, added after the API matches the SDK. See `docs/backup-restore.md` once that phase is in place.

## Docs

- `docs/api-examples.md` — requests that were run against this server
- `docs/compatibility.md` — where OSD1 and the Cloudflare SDK differ
- `docs/operations.md` — Docker image and volume
- `docs/backup-restore.md` — Litestream backup and restore
- `docs/benchmark.md` — measured timings from `scripts/bench.sh`
- `AGENTS.md` — invariants and file ownership

## Run

```bash
cargo run -p osd1-server
```

`OSD1_DB_PATH` defaults to `./data/osd1.db`. `OSD1_HTTP_ADDR` defaults to `127.0.0.1:8080`. Set `OSD1_API_TOKEN` to require `Authorization: Bearer <token>`.
