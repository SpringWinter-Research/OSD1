---
title: Operations
description: How to start OSD1, set the environment, and start a raft cluster.
---

# Operations

The image listens on `0.0.0.0:8080`. The only database file is `/data/osd1.db`. The image runs as user id 10001. Compose mounts a named volume at `/data` and sets `OSD1_API_TOKEN`. The default token is `dev-token` when the variable is unset.

```bash
docker compose -f deploy/compose.yml build
OSD1_API_TOKEN=dev-token docker compose -f deploy/compose.yml up -d
curl -sS -H "Authorization: Bearer dev-token" -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  http://127.0.0.1:8080/accounts/a/d1/database/b/query
docker compose -f deploy/compose.yml down
```

`down` without `-v` keeps the volume. A later `up` opens the same database file.

Recorded on this machine with image `osd1-osd1`. `GET /health` returned `{"ok":true}`. The current body also has `ready`. Status rules are on the [monitoring](monitoring.md) page. `SELECT 1 AS n` returned `result[0].results` of `[{"n":1}]`. This request created the table and inserted one row:

```json
{"sql":"CREATE TABLE kept (id INTEGER PRIMARY KEY, note TEXT); INSERT INTO kept (note) VALUES (?);","params":["survived-restart"]}
```

The response had two result elements. The insert element had `changes` 1 and `last_row_id` 1. After `docker compose -f deploy/compose.yml down` and `up -d` again, `SELECT note FROM kept` returned:

```json
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[{"note":"survived-restart"}],"meta":{"changed_db":false,"changes":0,"duration":0.010208,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":4096,"timings":{"sql_duration_ms":0.010208}}}]}
```

The named volume was `osd1_osd1-data`.

`./scripts/persist.sh` repeats that check on the current image. It inserts `persist-restart-ok`. It restarts the container and selects the row. It then recreates the container and selects the row again. It checks that `/data` is mode `700`. It checks that `osd1.db` and `osd1.db-wal` are mode `600`. It checks that the API token does not appear in the container logs. The script removes the volume on exit.

Recorded result:

```
data_dir_mode 700
db_file_mode 600
wal_file_mode 600
token_absent_from_logs yes
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[{"note":"persist-restart-ok"}],"meta":{"changed_db":false,"changes":0,"duration":0.020208,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":4096,"timings":{"sql_duration_ms":0.020208}}}]}
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[{"note":"persist-restart-ok"}],"meta":{"changed_db":false,"changes":0,"duration":0.057666,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":4096,"timings":{"sql_duration_ms":0.057666}}}]}
persist ok
```

The first JSON line is after `compose restart`. The second is after `compose up -d --force-recreate`.

Litestream is not in `deploy/compose.yml`. `deploy/compose.backup.yml` and `scripts/recovery.sh` add the S3-compatible store and Litestream. TLS for the same image is `./scripts/tls-check.sh`. TLS ends at nginx. Nginx forwards `Authorization` to OSD1.

The process stops on SIGINT and on SIGTERM.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `OSD1_DB_PATH` | `./data/osd1.db` | Database file. The image uses `/data/osd1.db` |
| `OSD1_HTTP_ADDR` | `127.0.0.1:8080` | Listen address. The image uses `0.0.0.0:8080` |
| `OSD1_API_TOKEN` | empty | Bearer token. Empty disables auth on `/query` and `/raw` |
| `OSD1_QUEUE_CAPACITY` | `64` | Requests allowed to wait |
| `OSD1_MAX_BODY_BYTES` | 1 MiB | Largest request body |
| `OSD1_MAX_RESPONSE_BYTES` | 1 MiB | Largest success JSON |
| `OSD1_REQUEST_TIMEOUT_MS` | `30000` | Deadline for simple mode and for a raft read. This includes a read that waits on `x-osd1-min-commit-index`. The statement is interrupted. `0` waits with no deadline. A raft write waits for raft |
| `OSD1_SERVED_BY_COLO` | `LOC` | Value copied into result metadata |
| `OSD1_NODE_ID` | unset | Raft node id. It is a `u64`. Set it with the variables below to leave simple mode |
| `OSD1_RAFT_LISTEN` | unset | Raft TCP address, `host:port` |
| `OSD1_RAFT_SECRET` | unset | Shared secret on the raft port |
| `OSD1_RAFT_BOOTSTRAP` | unset | `1` or `true` starts a new cluster. Ignored when `<database>.raft` already has state |
| `OSD1_RAFT_JOIN` | unset | Raft address of a node that is already in the cluster. Ignored when this raft directory already has state |
| `OSD1_RAFT_VOTER` | unset | `1` or `true` makes this join a voter. The default join is a learner |
| `OSD1_RAFT_REMOVE` | unset | Node id to drop. Also set `OSD1_RAFT_JOIN` and `OSD1_RAFT_SECRET`. The caller exits after the call |
| `OSD1_ROUTER` | unset | `1` or `true` serves the route table and does not open a database |
| `OSD1_ROUTES` | unset | File of `database_id http://host:port` lines. Blank lines and `#` comments are skipped |
| `OSD1_DATABASE_ID` | unset | Path id this process serves from its own file |
| `OSD1_LITESTREAM_HEARTBEAT` | unset | File the sidecar touches. `/metrics` then exports `osd1_litestream_last_ok_seconds` |

A directory that OSD1 creates is mode `0700`. The database, the WAL, the shared-memory file, and the raft files are mode `0600`. The API token is redacted from the startup log.

Do not set `OSD1_RAFT_BOOTSTRAP` and `OSD1_RAFT_JOIN` together. The process exits with `set OSD1_RAFT_BOOTSTRAP=1 or OSD1_RAFT_JOIN, not both`. A new node with neither value, and with no raft state, exits with `set OSD1_RAFT_BOOTSTRAP=1 or OSD1_RAFT_JOIN`.

## Raft mode

Simple mode never opens a raft log. Leave `OSD1_NODE_ID` and every `OSD1_RAFT_*` variable unset.

Raft mode starts when any of those variables is set. The process then requires `OSD1_NODE_ID`, `OSD1_RAFT_LISTEN`, `OSD1_RAFT_SECRET`, and one of `OSD1_RAFT_BOOTSTRAP=1` or `OSD1_RAFT_JOIN`. A running process stays in the mode it started in.

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

The bootstrap process is the first leader. A joined node is a learner unless you set `OSD1_RAFT_VOTER=1`. For a third voter, repeat the join. Use a new node id, a new raft port, a new HTTP port, and a new database file. Set `OSD1_RAFT_VOTER=1`. Keep the voting set at three.

On a new bootstrap or a new join, OSD1 waits up to 5 seconds for a follower, leader, or learner role. A timeout fails startup. On a restart, that timeout does not fail startup. The process still opens HTTP.

Clients call `/query` and `/raw` on any node. Quorum, local reads, and removal behavior are on the [architecture](architecture.md) page.

Restart a process with the same node id, the same `OSD1_RAFT_LISTEN`, and the same `OSD1_DB_PATH`. An existing raft directory skips bootstrap and join. Bind every voter again before you expect a new write. One voter cannot form a majority. The process stays up until the other voters return.

To drop node 4:

```bash
OSD1_RAFT_REMOVE=4 \
OSD1_RAFT_JOIN=127.0.0.1:7001 \
OSD1_RAFT_SECRET=raft-secret \
cargo run -p osd1-server
```

The caller exits after the call. The removed node returns HTTP 503, finishes accepted SQL for up to 5 seconds, and exits. A later start of that database exits 0. The log line says the node was removed from the raft cluster. Remove a member only while a majority of the voters is up.

`deploy/compose.leader-backup.yml` is an optional third file. It runs Litestream only while this process is the raft leader. Restore steps are on the [backup and restore](backup-restore.md) page.

## Router

One database is one process. To front several databases, run `OSD1_ROUTER=1` with an `OSD1_ROUTES` file. The router does not open SQLite. Proxy limits are on the [architecture](architecture.md) page.

## Metrics

`GET /metrics` is Prometheus text. It does not use the bearer token. The scrape job, the series, and a Grafana dashboard are on the [monitoring](monitoring.md) page.
