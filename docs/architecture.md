---
title: Architecture
description: Client, distribution, and storage. One SQL thread, one SQLite file, and an optional raft log beside that file.
---

# Architecture

OSD1 is one process. That process owns one SQLite file and one thread that executes SQL. Clients send native SQL in the Cloudflare D1 HTTP shape. Raft, when you turn it on at startup, copies committed statements. Litestream, when you run it, copies the leader file from outside the process.

Three rules hold the design:

1. One file and one SQL thread. Tokio tasks enqueue work. They do not call SQLite.
2. The HTTP surface matches the installed Cloudflare D1 client (`cloudflare@4.5.0`).
3. You choose replication once, at process start. A committed statement replays the same way on every node. Followers serve local reads.

Locks, the log record, and idempotent apply are on the [low-level design](low-level.md) page. Limits are on the [system design](system-design.md) page.

## Client layer

`POST /accounts/:account_id/d1/database/:database_id/query`, the matching `/raw` route, and `POST /mcp` execute SQL. They wait on the same bounded queue. A full queue returns HTTP 429 and does not block. `GET /health`, `GET /metrics`, `GET /`, and `GET /explorer` skip the queue. The explorer pages are HTML and do not check the token. The page sends the bearer token only on `/raw`. `POST /mcp` uses the same token. The router process does not execute SQL on `/mcp`.

The account id and the database id in the path are ignored unless you set `OSD1_DATABASE_ID` or `OSD1_ROUTES`. One process still has one database.

<figure>
  <img src="assets/arch-process.svg" alt="Client to HTTP to a bounded FIFO queue to one SQL thread to one SQLite file. A full queue returns HTTP 429. Health, metrics, and the explorer skip the queue." width="720">
  <figcaption>Figure 1. Simple mode. Query, raw, and MCP execute SQL on the same queue.</figcaption>
</figure>

Parameters use the SQLite bind API. The response envelope follows the installed `cloudflare` package. Status codes are on the [API examples](api-examples.md) page. SDK differences are on the [compatibility](compatibility.md) page.

## Distribution layer

Set `OSD1_NODE_ID`, `OSD1_RAFT_LISTEN`, and `OSD1_RAFT_SECRET` to leave simple mode. One process sets `OSD1_RAFT_BOOTSTRAP=1`. A join is a learner unless you set `OSD1_RAFT_VOTER=1`. The cluster has three voters. A running process stays in the mode it started in. Startup commands are on the [operations](operations.md) page.

Quorum is a majority of the voters. Two voters commit when both have stored the entry. Three voters commit at two. Learners replicate and serve local reads. They do not vote.

A follower reads its own file and sets `served_by_primary` to false. That read can lag the leader. A client can send `x-osd1-commit-index` back as `x-osd1-min-commit-index`. This node then waits until its apply index reaches that value. The wait polls every 10 ms, in front of the SQL queue. The deadline returns HTTP 504 when the index is still behind. A read with no header uses the local file at once. The D1 SDK does not send the header. `served_by_region` stays `WNAM`.

A write returns the log index in `x-osd1-commit-index`. The D1 SDK does not send a command id, so OSD1 allocates one. The read rule for `served_by_primary` is on the [low-level design](low-level.md) page. Health rules are on the [monitoring](monitoring.md) page.

<figure>
  <img src="assets/arch-write.svg" alt="A follower write syncs the next command id, forwards that pair to the leader, appends a checksummed log suffix, waits for a majority, then applies on the SQL thread into osd1_applied." width="720">
  <figcaption>Figure 2. The pair <code>(origin node, command id)</code> is applied once. A second delivery returns the stored body.</figcaption>
</figure>

The next id is in `<database>.raft/next-command-id`. OSD1 syncs that file before it proposes the command. On startup, the next id is one past the highest id already in that file, in the raft log, and in `osd1_applied` for this node. The first write from node 2 is a different pair from the first write from node 1.

The log is append-only. A bad checksum refuses to open the directory. The record layout is on the [low-level design](low-level.md#log-record) page.

Apply runs on the state-machine worker. The SQL thread inserts `(origin, id, idx)` into `osd1_applied`. `idx` is the raft log index. A new row runs the user SQL and stores the response in the same transaction. A row that already has a body returns that body. The hold time for a stored body is on the [low-level design](low-level.md) page.

Raft mode denies the functions below. A committed statement then replays the same way on every node.

`random`, `randomblob`, `date`, `time`, `datetime`, `julianday`, `strftime`, `unixepoch`, `current_date`, `current_time`, `current_timestamp`.

The compare is case-insensitive. `datetime('now')` is denied when this rule is on. `last_insert_rowid` stays allowed. Simple mode leaves the rule off.

The leader copies database pages into a snapshot after 256 commits. A joiner can install that snapshot. The byte layout is on the [low-level design](low-level.md) page.

`OSD1_RAFT_REMOVE=<id>` asks the leader to drop that member. Also set `OSD1_RAFT_JOIN` and the same secret. Membership changes run one at a time. There is no HTTP route for membership. Remove a member only while a majority of the voters is up.

After the change commits, the leader sends raft frame kind 7 to the address it saved before the change. That process writes `<database>.raft/removed`. It returns HTTP 503 from `/health`. It rejects new SQL with `node is leaving the cluster`. It finishes SQL it already accepted, for up to 5 seconds. It then stops HTTP and exits 0. A later start of the same directory exits 0. A process that was never a member stays up. A majority can commit the removal when the removed node has not stored the entry.

A client that stops its own timer can still see a committed raft write. A follower retries one forward, with the same pair, for about 3 seconds while leadership moves.

See [Low-level design](low-level.md), [System design](system-design.md), [Monitoring](monitoring.md), and [Backup and restore](backup-restore.md).

## Storage layer

### The SQL thread

`SqliteWorker` opens the only connection to the live database. Tokio tasks enqueue work. They never call SQLite on that file. The backup API opens a second connection, and that connection is to the destination path.

OSD1 registers sqlite-vec 0.1.9 on this connection before the statements below. That call is not SQL. `load_extension` stays denied.

Startup runs these statements, in order:

```sql
PRAGMA page_size=4096;
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=0;
PRAGMA quick_check;
```

`quick_check` must return the string `ok`. More than one row fails the open. `page_size` applies when the database is created empty. An existing file keeps its page size. `journal_mode`, `synchronous`, and `foreign_keys` apply on every open. The authorizer is installed after this setup.

The prepared-statement cache holds 32 entries. The column-name cache holds 16 entries. A schema change clears the column-name cache.

The authorizer is the policy. OSD1 does not scan SQL text. A simple-mode request, a leader read, and a raft apply each run as one `BEGIN IMMEDIATE` transaction. The connection is in autocommit before the next request. A follower read does not open a transaction. A writable statement on that path is refused. `served_by_primary` is false on that path.

If `COMMIT` leaves the connection outside autocommit, later jobs fail until a successful snapshot install.

The progress handler runs every 1,000 opcodes. In simple mode, and on a raft read, it interrupts the statement when `OSD1_REQUEST_TIMEOUT_MS` has passed. The HTTP status is 504, code 7500, with `x-should-retry: false`. The default deadline is 30 seconds. `0` waits with no deadline. A raft write has no deadline. It returns when raft accepts or rejects the command.

After a job, if the connection is in autocommit and the WAL file is larger than 4 MiB, the thread runs `PRAGMA wal_checkpoint(PASSIVE)`. It ignores an error. The same pragma through `/query` returns SQLite error 6, because that request is already inside `BEGIN IMMEDIATE`. `/metrics` reports the main file size. It does not report the WAL.

OSD1 itself may run `BEGIN`, `COMMIT`, `ROLLBACK`, the `osd1_applied` statements, and the passive checkpoint. A client may not.

| Message | When |
| --- | --- |
| `only the main database is allowed` | Database name other than `main` or `temp` |
| `ATTACH is not allowed` | `ATTACH` |
| `DETACH is not allowed` | `DETACH` |
| `client transactions are not allowed` | `BEGIN`, `COMMIT`, `ROLLBACK`, or `SAVEPOINT` |
| `writable_schema is not allowed` | `writable_schema` |
| `function is not allowed` | `load_extension`, `readfile`, or `writefile` |
| `nondeterministic function is not allowed` | The raft-mode function list above |
| `osd1_applied is reserved` | A client read or write of that table |
| `pragma is not allowed` | `synchronous`, `journal_mode`, `wal_autocheckpoint`, `locking_mode`, `mmap_size`, or `page_size` |

`table_info` and `integrity_check` stay allowed.

### Files

<figure>
  <img src="assets/arch-disk.svg" alt="Database file, WAL, and shared-memory sidecar beside a raft directory of log.dat, vote.json, meta.json, machine.json, snapshot.bin, next-command-id, and removed. Log records are OSD1LOG2, length, CRC-32, payload." width="720">
  <figcaption>Figure 3. <code>node1.db</code> uses the directory <code>node1.raft</code>. Restart uses the same node id, the same raft address, and the same database path.</figcaption>
</figure>

A directory that already contains raft state skips bootstrap and skips join. Bind the voters again before you expect a write. One voter cannot form a majority. The process stays up until the other voters return.

`state.json` is read when an older directory still has it. OSD1 does not write that file again. The record layout is on the [low-level design](low-level.md#log-record) page.

### Restore

Stop every node before you restore a raft cluster. The steps are on the [backup and restore](backup-restore.md) page. `scripts/recovery.sh` covers simple mode. Litestream stays a sidecar of the leader. It is not the commit path. A node that missed a generation catches up through the raft log.

### Several databases

One database is one process. `OSD1_ROUTER=1` reads `OSD1_ROUTES` and proxies `/query` and `/raw`. The router does not open SQLite. It does not execute `/mcp` tools. It copies the `Authorization` header. It returns the upstream status and the JSON body. It does not copy the commit-index headers. The process that executes SQL reads and writes those headers.

Each route line is `database_id http://host:port`. Blank lines and `#` comments are skipped. The upstream URL has no path. The router rejects a database id that contains `/`, `?`, a space, or a newline. It stops at `OSD1_MAX_RESPONSE_BYTES` of payload, or at that limit plus 64 KiB of socket bytes. The upstream request sets `Connection: close`.

`OSD1_DATABASE_ID` pins a process to one path id. With both variables unset, the account id and the database id in the URL are ignored.

See [Operations](operations.md) and [Backup and restore](backup-restore.md).
