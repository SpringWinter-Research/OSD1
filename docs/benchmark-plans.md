---
title: Benchmark plans
description: Named tests that have not been run. Recorded numbers stay on the benchmark page.
---

# Benchmark plans

Recorded runs stay on the [benchmark](benchmark.md) page. This page names tests that are not run yet.

Each table is one question. One thing changes between rows in that table. Record the percentile beside the rate.

Put a finished run on the benchmark page. Then mark the row here as run, and link that table.

The four-workload run on the benchmark page has a peak resident set of 6.5 MB. That figure belongs to that run.

## 1. Path cost

The question is the cost of one short statement.

Two runs already answer two forms of that question:

1. Many clients send `SELECT 1` over keep-alive HTTP. The table is [SELECT 1, many clients](benchmark.md).
2. One client runs the same SQL in process and over HTTP. The table is [Same SQL, one client](benchmark.md).

| Question | What changes | Status |
| --- | --- | --- |
| Indexed point select as the client count grows | Client count | Not run |
| N statements in one request, then those N statements as N requests | Request shape | Not run |
| A WAL write, in its own table from a read that does not sync | Sync | Not run |

A read that does not sync and a WAL write answer different questions. Keep them in different tables.

## 2. Workload shape

The question is the mix of statements.

The four profiles on the [benchmark](benchmark.md) page are the small warm example. The mix for each profile is on the [testing](testing.md) page.

The rows below are separate tests. Keep the machine, the table, and the client count fixed. Change the shape.

| Shape | Status |
| --- | --- |
| Point reads | Not run |
| Mixed updates | Not run |
| Read, modify, write | Not run |
| Inserts | Not run |
| Short ranges | Not run |
| A scan of a growing fraction of the table | Not run |

## 3. Working set

The question is whether the database file fits in memory.

| Question | What changes | Status |
| --- | --- | --- |
| The file fits in memory, then the file does not fit | File size | Not run |
| The same statements on a cold cache, then on a warm cache | Cache state | Not run |
| The WAL file grows past 4 MiB, so the passive checkpoint runs | WAL size | Not run |

`/metrics` reports the main file size. The WAL length is absent there. The checkpoint rule is on the [architecture](architecture.md) page.

## 4. Overload

The question is the server behavior when the offered rate is above the rate the server finishes.

| Question | What you record | Status |
| --- | --- | --- |
| Offered rate against HTTP 429, queue depth, p99, and HTTP 504 | Percentile beside the rate | Not run |

A body over the size limit and a response over the size limit are status checks. They are checks of HTTP 413 and HTTP 400. They are a different question from a rate.

One scan beside point reads shows the one SQL thread. That pair is not run. Keep it off the `SELECT 1` table.

## 5. Raft

Keep raft rows in raft tables. A simple-mode row and a three-voter row answer one question only when the statement, the machine, and the client count stay fixed.

Elections under load are not run. Leave that claim off this page until a run exists.

Raft write latency under load is a separate question. It is not run.

| Question | What changes | Status |
| --- | --- | --- |
| Simple mode, then three voters | Mode | Not run |
| Follower read with no `x-osd1-min-commit-index`, then the same read with that header | Header | Not run |
| The leader stops | Which node is up | Not run |
| A new node catches up from a snapshot, then a new node catches up from the log | Catch-up path | Not run |
| Add a learner under load, then remove a learner under load | Membership | Not run |

## 6. Durability

| Question | Status |
| --- | --- |
| Kill the process after HTTP 200, then open the file | Not run |
| Kill the process during a write | Not run. No crash matrix is recorded |

The recorded Litestream recovery is on the [backup and restore](backup-restore.md) page.

## 7. Many databases

The question is many OSD1 processes on one host.

| Question | What changes | Status |
| --- | --- | --- |
| N processes on one host | Process count | Not run |
| The router from Operations in front of those processes | Path | Not run |
| One busy process beside an idle process | Which process is busy | Not run |

Each process keeps one database file. `OSD1_ROUTER=1` does not open SQLite. The router setup is on the [operations](operations.md) page.

## 8. Text search and vectors

These rows measure SQL in this database. Keep them off the `SELECT 1` table.

The row count where a scan is too slow is not measured. Leave that number off this page.

| Question | What changes | Status |
| --- | --- | --- |
| `MATCH` as the row count grows, beside `LIKE` on the same text | Row count, then the predicate | Not run |
| A JSON vector scan at 100 rows | Row count | Not run |
| The same scan at 1,000 rows | Row count | Not run |
| The same scan at 10,000 rows | Row count | Not run |
| The same scan at 100,000 rows | Row count | Not run |

The statements for `MATCH`, for a JSON distance, and for a `vec0` query are on the [API examples](api-examples.md) page. A `vec0` rate is not run.
