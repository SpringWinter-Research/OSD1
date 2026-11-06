# AGENTS.md

OSD1 is one SQLite database behind an HTTP API that the Cloudflare TypeScript SDK can call.

## Invariants

- One database file. The path is `OSD1_DB_PATH`.
- One OS thread executes SQL. Tokio worker threads never call SQLite.
- That thread owns the only SQLite connection. Do not open a second connection.
- Requests wait on a bounded FIFO queue. A full queue returns HTTP 429 and does not block.
- SQL is native SQLite. Parameters are bound with the SQLite bind API.
- Authorization uses the SQLite authorizer callback. Do not accept or reject statements by scanning SQL text.
- Each HTTP request is one transaction. `BEGIN`, `COMMIT`, `ROLLBACK`, and `SAVEPOINT` from client SQL are denied. The connection is in autocommit before the next request.
- The HTTP surface is `POST /accounts/:account_id/d1/database/:database_id/query`, the matching `/raw` route, and `GET /health`.
- The request and response shapes follow the installed `cloudflare` npm package. If that SDK rejects a response, change the server and re-run the SDK tests.
- Do not change SQLite’s on-disk format. Use bundled upstream SQLite.
- Litestream runs as a later-phase sidecar. It is not linked into the server binary.
- No connection pool, clustering, or multi-database support.

## Ownership

The lead is the only editor of:

- `AGENTS.md`, `README.md`, `LICENSE`, root `Cargo.toml`, `.gitignore`
- `crates/osd1-types/**`
- `crates/osd1-server/**`
- `docs/compatibility.md` when sections conflict

| Agent | May edit | Must not edit |
| --- | --- | --- |
| Engine | `crates/osd1-engine/**` | types, http, server, deploy, spike |
| Protocol | `crates/osd1-http/**` | types, engine, server, deploy |
| Backup | `deploy/litestream.yml`, `deploy/compose.backup.yml`, `docs/backup-restore.md`, `scripts/recovery.sh` | Rust crates |
| Operations | `deploy/Dockerfile`, `deploy/compose.yml`, `.dockerignore`, `docs/operations.md` | Rust crates, Litestream files |
| Testing | `tests/**`, `crates/osd1-server/tests/**`, `scripts/bench.sh`, `scripts/profiles.sh`, `scripts/persist.sh`, `scripts/recovery-cases.sh`, `scripts/tls-check.sh`, `scripts/scan.sh`, `docs/benchmark.md`, `docs/testing.md`, `docs/security-scan.md`, `docs/api-examples.md`, `spike/sdk-compat.mjs` | Production Rust, except tests |

An agent that needs a new shared type stops and asks the lead. The lead changes `crates/osd1-types` and the agent rebases. Agents do not edit that crate.

## Interfaces

`osd1-engine` implements `SqlExecutor` from `osd1-types`.
`osd1-http` depends on `osd1-types` and receives `Arc<dyn SqlExecutor>`.
`osd1-server` wires the worker to the router.

## Checks before claiming done

- `cargo test --workspace`
- `node spike/sdk-compat.mjs` against the built binary
- Docker compose serves `/query` from a volume
- `scripts/recovery.sh` has been executed before any durability claim
- `docs/benchmark.md` quotes a real run before any performance claim
- `docs/testing.md` quotes a real run before claiming the profile, persistence, TLS, recovery, or scan checks
