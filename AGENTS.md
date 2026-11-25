# AGENTS.md

OSD1 is one SQLite database behind an HTTP API that the Cloudflare TypeScript SDK can call.

## Invariants

- One database file. The path is `OSD1_DB_PATH`.
- One OS thread executes SQL. Tokio worker threads never call SQLite.
- That thread owns the only connection to the live database. Do not open a second connection to that file.
- Requests wait on a bounded FIFO queue. A full queue returns HTTP 429 and does not block.
- SQL is native SQLite. Parameters are bound with the SQLite bind API.
- Authorization uses the SQLite authorizer callback. Do not accept or reject statements by scanning SQL text.
- Client `BEGIN`, `COMMIT`, `ROLLBACK`, and `SAVEPOINT` are denied. A simple-mode request, a leader read, and a raft apply are one transaction each. A follower local read does not open a transaction. The connection is in autocommit before the next request.
- The HTTP surface is `POST /accounts/:account_id/d1/database/:database_id/query`, the matching `/raw` route, `POST /mcp`, `GET /health`, `GET /metrics`, and `GET /` plus `GET /explorer`. Those two pages are the table explorer. They are unauthenticated HTML and do not enqueue SQL. The page sends the bearer token only on `/raw`. `POST /mcp` is the Model Context Protocol endpoint. It uses the same bearer token and the same SQL queue. Tools build `CREATE`, `ALTER`, `DROP`, and the reads for the table list and one table's columns, or they pass SQL through. The router process does not execute SQL on `/mcp`.
- `GET /metrics` is Prometheus text exposition. It is unauthenticated, like `/health` and the explorer page. Metric labels are a fixed set: no SQL text, account ids, or database ids. `/` and `/explorer` use the route label `explorer`. `/mcp` uses the route label `mcp`. SQL from `/mcp` uses the route label `query`.
- The request and response shapes follow the installed `cloudflare` npm package. If that SDK rejects a response, change the server and re-run the SDK tests.
- Do not change SQLite’s on-disk format. Use bundled upstream SQLite.
- Litestream runs as a later-phase sidecar. It is not linked into the server binary.
- No connection pool. One process owns one database file. Scale by running one process per database.
- `OSD1_ROUTER=1` with `OSD1_ROUTES` (lines of `database_id http://host:port`) proxies `/query` and `/raw`. The router process does not open SQLite and does not execute `/mcp` tools. `OSD1_DATABASE_ID` pins a process to one path id. With both unset, path ids are ignored.
- Simple mode, the default, has no clustering and never opens a Raft log.
- The process shuts down on SIGINT and SIGTERM.

## Raft mode

Set `OSD1_NODE_ID`, `OSD1_RAFT_LISTEN`, and `OSD1_RAFT_SECRET` to leave simple mode. One process sets `OSD1_RAFT_BOOTSTRAP=1`. Another process sets `OSD1_RAFT_JOIN` to a member's raft address. A running process does not switch modes.

A join is a learner. Learners replicate and serve local reads. They do not vote, so a new reader leaves the quorum unchanged. Set `OSD1_RAFT_VOTER=1` to add a voter. Three voters is the cluster. `OSD1_RAFT_REMOVE=<id>`, together with `OSD1_RAFT_JOIN` and `OSD1_RAFT_SECRET`, asks the current leader to drop that member and then exits. Membership changes run one at a time and have no HTTP route. The leader tells a removed member on the raft port. That member writes `removed`, returns 503 from `/health`, rejects new SQL, finishes SQL it has already accepted (up to 5 seconds), stops HTTP, and exits. A later start of that directory exits the same way. A process that has never been a member stays up.

Simple mode still calls `execute_request` for every request. Raft mode turns on `deny_nondeterministic` so a committed statement replays the same way on every node. `datetime('now')` and `random` are denied there. `last_insert_rowid` stays allowed. Followers serve local reads and set `served_by_primary` false. A leader read that returns after this node has lost leadership sets `served_by_primary` false as well. That SQL still ran as `BEGIN IMMEDIATE`. They proxy writes to the leader on the raft TCP port and return the normal 200 envelope. `served_by_region` stays `WNAM`. A local read can be stale during a partition. A successful write returns `x-osd1-commit-index`. A later read on this node that sends that value as `x-osd1-min-commit-index` waits until the local apply index reaches it, inside the read deadline. That wait reads the metrics watch and does not queue behind a log append. `/health` stays 200 while the process is up. Its `ready` field is false when this node's commit index is more than 32 ahead of its apply index. The D1 SDK does not send the header.

The raft directory beside the database (`node1.db` uses `node1.raft`) holds an append-only checksummed log, the vote, membership metadata, and the next command id. Applied results live in the SQLite table `osd1_applied`, in the same transaction as the user SQL. After a log index is purged, the stored body for that index is cleared and the `(origin, id)` row stays. A body applied in the last 3 seconds stays until a later purge, so a follower's retry of that command id still sees the result. The hold is memory on the SQL thread, so a restart drops it. The raft core fsyncs new log bytes. Snapshots copy pages with SQLite's backup API on the SQL thread, into a separate file. The store does not keep those bytes. One buffer remains for the openraft send. Restart the process on the same `OSD1_RAFT_LISTEN`, the same node id, and the same `OSD1_DB_PATH`. When that directory already has state, the process does not bootstrap and does not join again. A new directory still needs bootstrap or join. Snapshots ship the SQLite file, so a joiner copies pages once the leader has a snapshot. SQL apply runs on the state-machine worker, off the raft core, so one slow statement does not by itself start an election.

Quorum is a majority of the voters. Two voters commit only when both store the entry. Three voters commit at two. Learners are outside that count. A restarted voter stays up while it waits for a majority to bind the same raft addresses.

Litestream stays a sidecar. It is not the commit path. Point it at the current leader's file. Nodes catch up through the raft log. To restore a cluster from that copy: stop every node, restore one file, bootstrap one process on it, copy that same file to the other nodes before any new write, then join them.

Each new HTTP request uses a new command id on that node. The next id is one past the highest id already in the raft log and in `osd1_applied` for this node, and it is synced in the raft directory before the command is proposed. The state machine applies the pair `(origin node, id)` once, so node 2's first write is not node 1's first write. The D1 SDK does not send an id. A raft write waits until raft accepts or rejects it, and a follower retries that same command id for a few seconds while leadership moves. `OSD1_REQUEST_TIMEOUT_MS` still bounds simple-mode work and raft reads: the statement is interrupted and the response is HTTP 504, code 7500, with `x-should-retry: false`. The SDK does not retry 504. A manual retry of a response that already failed is a second command. A client that gives up on its own timer can still observe a committed raft write.

`/metrics` adds `osd1_raft_role`, `osd1_raft_term`, `osd1_raft_commit_index`, `osd1_raft_apply_index`, `osd1_raft_apply_lag`, and `osd1_raft_peers` only in raft mode. Role is one gauge: 0 follower, 1 leader, 2 candidate, 3 learner. Labels stay on the fixed set.

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
