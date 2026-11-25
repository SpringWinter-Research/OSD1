---
title: System design
description: How a request works, what OSD1 does not do, and what can go wrong.
---

# System design

This page says what OSD1 does, what it does not do, and what can go wrong. The layers are on the [architecture](architecture.md) page. Locks and bytes are on the [low-level design](low-level.md) page.

## What OSD1 is

OSD1 is one SQLite database file behind an HTTP API. The Cloudflare TypeScript SDK (`cloudflare@4.5.0`) can call that API. One process owns the file at `OSD1_DB_PATH`. One thread, named `osd1-sql`, executes SQL on the one connection.

## The problem

Applications already send the D1 request shape. They need one place that runs that SQL, limits the queue, and can hold the same committed writes on more than one process.

## How a request works

1. The client sends `POST /query`, `POST /raw`, or `POST /mcp`.
2. OSD1 checks the bearer token when a token is set.
3. A SQL call waits on a bounded queue. A full queue returns HTTP 429.
4. In simple mode, the SQL thread runs the statements in one transaction.
5. In raft mode, a write gets a new command id. OSD1 stores that id before it proposes the command.
6. A majority of the voters must store the log entry. Then the SQL thread applies the pair `(origin node, command id)` once.
7. A follower serves a local read from its own file. It forwards a write to the leader and returns the normal HTTP 200 envelope.

`POST /mcp` calls the same queue. A tool builds `CREATE`, `ALTER`, or `DROP`, reads one table's columns, or passes SQL through. A SQL failure stays inside HTTP 200 and sets `isError` to true. A missing token stays HTTP 401. A node that is leaving stays HTTP 503.

Simple mode never opens a raft log. Raft mode starts only when you set `OSD1_NODE_ID`, `OSD1_RAFT_LISTEN`, and `OSD1_RAFT_SECRET`. A running process does not change mode.

Litestream is a sidecar. It copies the leader file. It does not vote, and it does not acknowledge a commit.

## What OSD1 does not do

- OSD1 does not open a second connection to the live database file.
- OSD1 does not keep a connection pool.
- OSD1 does not wait for the leader on every read. A read waits only when the client sends `x-osd1-min-commit-index`.
- OSD1 does not offer an HTTP route to add or remove a member.
- OSD1 does not change the on-disk format of SQLite.
- The D1 SDK does not send a command id. A manual retry is a second command.
- `/metrics` does not report the size of the WAL.

## Access

Set `OSD1_API_TOKEN` when `/query`, `/raw`, and `/mcp` must require a bearer token. The match is exact. A missing token is HTTP 401. A wrong token is HTTP 401. OSD1 does not queue those requests. The startup log replaces the token with `[redacted]`.

`/health`, `/metrics`, `/`, and `/explorer` do not check the token. The explorer stores the token in browser storage. The page sends the token only on `/raw`.

Each raft frame carries the raft secret. A wrong secret closes the connection. The secret is not a metric label.

A directory that OSD1 creates is mode `0700`. Database files and raft files are mode `0600`. An existing parent directory stays as it is.

The raft log stores the SQL text and the parameters. `osd1_applied` stores the statement result in the same database file. A snapshot includes that file.

Litestream keys stay in the sidecar environment. They are not in the `osd1` binary.

Anyone who can open `GET /metrics` can read the rates and the file size. Keep that port on a network that should have this view. The series are on the [monitoring](monitoring.md) page.

## Questions

### Why is there one SQL thread?

That thread owns the connection, the transaction, the authorizer, and the checkpoint. A pool would open a second connection to the live file.

### Why does the authorizer refuse a statement?

SQLite parses the statement first. OSD1 does not accept or reject SQL by a scan of the text. The deny list is on the [architecture](architecture.md) page.

### Can a read be stale?

Yes, when the client omits `x-osd1-min-commit-index`. `served_by_region` stays `WNAM`. The D1 SDK does not send the header. The effect is in the table below.

### What if the client stops waiting for a write?

The command can still commit. A later send is a new command. The effect is in the table below.

### Where do repeated writes get their result?

The pair `(origin node, command id)` is applied once. A second delivery returns the stored body from `osd1_applied`. The steps are on the [low-level design](low-level.md) page.

## What can go wrong

| Risk | Level | What happens |
| --- | --- | --- |
| Client stops waiting on a raft write | High, for exactly-once callers | The write has no HTTP deadline and no client id. A retry gets a new command id. The first command can still commit. |
| Stale local read | High, when the header is absent | A read with no `x-osd1-min-commit-index` can lag during a partition. The D1 SDK does not send the header. A read that sends the header waits until this node applies that index, or returns HTTP 504 at the read deadline. The wait is in front of the SQL queue. A leader sample runs as one transaction. If leadership is gone when the response returns, `served_by_primary` is false on every statement. That SQL still ran as `BEGIN IMMEDIATE`. A follower read does not open a transaction. |
| Log sync blocks the raft core | High. Unmeasured under load | The core waits for the log append. That wait includes the suffix sync. On a quiet Apple M4, a release run appended 40 blank entries after 5 warmup appends. The minimum was 2.877 ms. The median was 3.001 ms. The maximum was 3.164 ms. The election timeout is 150 ms to 300 ms. Elections under load are unmeasured. This is not a raft request rate. |
| Storage lock held across SQL | Medium | The state-machine worker holds the Adaptor lock for the whole apply. The next storage call on that node waits. Heartbeat replication does not take that lock. A 400 ms apply on an in-memory cluster left the term unchanged. |
| Snapshot install and snapshot read hold the storage lock | High. Unmeasured under load | Install holds the Adaptor lock across the file replace. The snapshot read holds that lock across the read of `snapshot.bin`. The chunked send starts after that read returns. These waits are unmeasured. The election timeout is 150 ms to 300 ms. |
| Crash after the file replace, before `machine.json` | Medium | This window is accepted. Replay uses `osd1_applied` when that row committed. |
| Memory cursors move before the sync returns | Medium | A failed suffix append restores the three cursors. A failed rewrite clears the version flag, so the next append rewrites the file. A failed purge or conflict delete restores the map and the cursors. Purge also restores `last_purged_log_id`. |
| Old command id with no local entry | Medium | The next-id floor cannot see a commit that exists only on other nodes. |
| `osd1_applied` grows | Medium | One row stays for each committed command. After a log purge, `body` becomes NULL where `idx` is at or below that purge. A body from the last 3 seconds stays until a later purge. A restart drops those ages. A NULL `idx` keeps its body. OSD1 does not delete the row. |
| Parent-directory sync can fail with no error | Medium | OSD1 syncs the temp file. OSD1 then syncs the parent directory. A failure to open or sync that directory is ignored. |
| Open metrics, health, or the explorer | Medium, when the port is reachable | These routes do not check the token. SQL still requires the token when you set one. |
| Passive checkpoint ignores errors | Low | The WAL can stay large. `busy_timeout` is 0. Metrics do not show the WAL. |
| Raft directory file with no raft state | Low | Startup can fail with `raft node needs bootstrap or a join address, not both`. |
| `quick_check` returns more than one row | Low | Open fails. |
