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
- The HTTP surface is `POST /accounts/:account_id/d1/database/:database_id/query`, the matching `/raw` route, `GET /health`, `GET /metrics`, and `GET /` plus `GET /explorer`. Those two pages are the table explorer. They are unauthenticated HTML and do not enqueue SQL. The page sends the bearer token only on `/raw`.
- `GET /metrics` is Prometheus text exposition. It is unauthenticated, like `/health` and the explorer page. Metric labels are a fixed set: no SQL text, account ids, or database ids. `/` and `/explorer` use the route label `explorer`.
- The request and response shapes follow the installed `cloudflare` npm package. If that SDK rejects a response, change the server and re-run the SDK tests.
- Do not change SQLite’s on-disk format. Use bundled upstream SQLite.
- Litestream runs as a later-phase sidecar. It is not linked into the server binary.
- No connection pool or multi-database support. Simple mode, the default, has no clustering and never opens a Raft log.

## Raft mode

Set `OSD1_NODE_ID`, `OSD1_RAFT_LISTEN`, and `OSD1_RAFT_SECRET` to leave simple mode. One process sets `OSD1_RAFT_BOOTSTRAP=1`. A reader sets `OSD1_RAFT_JOIN` to that process's raft address. A running process does not switch modes.

Simple mode still calls `execute_request` for every request. Raft mode turns on `deny_nondeterministic` so a committed statement replays the same way on every node. `datetime('now')` and `random` are denied there. `last_insert_rowid` stays allowed. Followers serve local reads and set `served_by_primary` false. They proxy writes to the leader on the raft TCP port and return the normal 200 envelope. `served_by_region` stays `WNAM`.

The raft log is in memory. Restarting a process drops it. This is not durable consensus across a reboot, and a restarted node must not rejoin a live cluster with an old database file. Litestream stays a sidecar of the leader, not the commit path.

Quorum is a majority. Two nodes commit only when both store the entry. Three nodes commit at two. The raft core waits while SQLite applies a committed entry. A statement that runs near `election_timeout_max` (300 ms) can delay heartbeats and start an election.

Each new HTTP request uses a new command id on that node. The state machine applies the pair `(origin node, id)` once, so node 2's first write is not node 1's first write. The D1 SDK does not send an id and does not retry 504, so a manual retry is a second command. A follower retries one timed-out forward with the same pair.

`/metrics` adds `osd1_raft_role`, `osd1_raft_term`, `osd1_raft_commit_index`, `osd1_raft_apply_index`, and `osd1_raft_peers` only in raft mode. Role is one gauge: 0 follower, 1 leader, 2 candidate, 3 learner. Labels stay on the fixed set.

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
