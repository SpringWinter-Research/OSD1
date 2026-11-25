---
title: Benchmark
description: Recorded OSD1 runs on an Apple M4, and a same-SQL comparison with SQLite.
---

# Benchmark

Apple M4, 6 Nov 2026. The tables are the same runs as the blocks below.

A release build serving `SELECT 1` over keep-alive HTTP reached 25,467 requests/s with one client and 111,075 requests/s with 32 clients. Eight clients did 82,129/s and 16 clients did 102,234/s. The rate flattened, so the SQL thread was full.

SQLite on the same machine, with no HTTP: 2,516,325 in-memory transactions per second, 543,065 WAL `synchronous=FULL` transactions per second, and 3,447,361 indexed reads per second inside one transaction.

{% include scale-chart.html %}

{% include latency-chart.html %}

The four workloads below used one Python client against the release binary. They are latency for that client, not the 111k ceiling.

| Workload | p50 | p95 | p99 | Requests/s | Memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| Read-heavy | 0.17 ms | 0.49 ms | 0.78 ms | 2,335 | 5.8 MB |
| Balanced | 0.16 ms | 0.47 ms | 0.95 ms | 2,554 | 5.4 MB |
| Write-heavy | 0.14 ms | 0.48 ms | 1.64 ms | 2,568 | 4.8 MB |
| Mixed | 0.16 ms | 0.36 ms | 0.45 ms | 3,413 | 6.5 MB |

The 32-client `SELECT 1` rate above stays the published simple-mode ceiling. The next section is a different run: one client, two engines, the same statements.

## How to read a comparison

A benchmark is a named workload on named hardware. Throughput alone is an incomplete reading. The percentile says whether the calls were bunched near that rate or whether a few calls were slow.

Two kinds of test answer two decisions. A synthetic test, such as `SELECT 1` or one indexed point select, can run on any schema and shows the path cost. A trace from an application, such as the Hacker News queries on [db-benchmarks.com](https://db-benchmarks.com/), shows that application. One does not stand in for the other.

Rows that share a table share the question, the machine, and the data. One thing changes between those rows. A second machine, a second SQLite version, or a write that has to fsync is a different table.

## Same SQL, one client

25 Nov 2026, Apple M4, macOS 26.6.2 (25G83), Darwin arm64. SQLite 3.46.0 on both sides: rusqlite 0.32 with the bundled library in the harness, and the same version string inside `target/release/osd1`. Simple mode, no API token, loopback. 4,000 rows, index `items_n`. Each measured call is `BEGIN IMMEDIATE`, the statement, `COMMIT`. 200 warmup calls, then 2,000 measured calls. Percentiles are nearest rank. The HTTP client is one Rust TCP connection with `Connection: keep-alive`. The in-process side calls rusqlite in the harness. It does not go through the OSD1 queue.

These statements are reads. `EXPLAIN QUERY PLAN` is `SEARCH items USING INDEX items_n (n=?)` for the point select. A commit of a read does not dirty a page, so this run does not fsync. It is not the WAL write rate from 6 Nov 2026.

{% include compare-chart.html %}

| Engine | Question | p50 | p95 | p99 | Calls/s |
| --- | --- | ---: | ---: | ---: | ---: |
| SQLite, in process | Indexed point select, one transaction per call | 0.002 ms | 0.003 ms | 0.003 ms | 414,450 |
| OSD1 HTTP | Same statement | 0.033 ms | 0.064 ms | 0.072 ms | 27,513 |
| SQLite, in process | `SELECT 1`, one transaction per call | 0.002 ms | 0.002 ms | 0.002 ms | 578,083 |
| OSD1 HTTP | Same statement | 0.032 ms | 0.038 ms | 0.046 ms | 30,379 |
| SQLite, in process | Indexed point select, one transaction for all 2,000 calls | 0.001 ms | 0.001 ms | 0.001 ms | 1,000,042 |

On the indexed point select, the in-process rate was 15.1 times the OSD1 rate (414,450 / 27,513). On `SELECT 1` it was 19.0 times (578,083 / 30,379). The last row changes the transaction: 2,000 lookups share one `BEGIN IMMEDIATE`. OSD1’s HTTP request is one transaction, so that row has no OSD1 figure. The published 3,447,361 indexed reads/s remains the 6 Nov 2026 figure. This script’s one-transaction loop is 1,000,042/s.

This client is not the client from the 6 Nov ceiling, and one connection is not 32. The ceiling stays 111,075 requests/s at 32 clients.

Unedited output:

```
sqlite_version 3.46.0
index_plan SEARCH items USING INDEX items_n (n=?)
question indexed_point_select_one_transaction_per_call
engine sqlite_rusqlite
ops 2000
wall_seconds 0.004826
throughput_ops_per_s 414450.479
p50_ms 0.002
p95_ms 0.003
p99_ms 0.003
max_ms 0.009
question select_1_one_transaction_per_call
engine sqlite_rusqlite
ops 2000
wall_seconds 0.003460
throughput_ops_per_s 578083.468
p50_ms 0.002
p95_ms 0.002
p99_ms 0.002
max_ms 0.002
question indexed_point_select_one_transaction_for_the_whole_loop
engine sqlite_rusqlite
ops 2000
wall_seconds 0.002000
throughput_ops_per_s 1000042.002
p50_ms 0.001
p95_ms 0.001
p99_ms 0.001
max_ms 0.007
osd1_index_plan_contains_items_n true
http_client keep-alive one connection
question indexed_point_select_one_transaction_per_call
engine osd1_http
ops 2000
wall_seconds 0.072693
throughput_ops_per_s 27513.044
p50_ms 0.033
p95_ms 0.064
p99_ms 0.072
max_ms 0.085
question select_1_one_transaction_per_call
engine osd1_http
ops 2000
wall_seconds 0.065835
throughput_ops_per_s 30378.843
p50_ms 0.032
p95_ms 0.038
p99_ms 0.046
max_ms 0.053
```

## Full-text tests on another site

[db-benchmarks.com](https://db-benchmarks.com/?cache=fast_avg&engines=elasticsearch_tuned_8.14.3%2Cmanticoresearch_rowwise_6.0.2&tests=hn&memory=110000&queries=0%2C1%2C2%2C3%2C4%2C5%2C6%2C7%2C8%2C9%2C10%2C11%2C12%2C13%2C14%2C15%2C16%2C17%2C18%2C19%2C20%2C21%2C22%2C23%2C24%2C25%2C26%2C27) is the shape those rules produce for search engines. The linked view is:

| Setting | Value |
| --- | --- |
| Cache mode | `fast_avg` |
| Engines | Elasticsearch 8.14.3, tuned. Manticore Search 6.0.2, row-wise |
| Test | `hn` |
| Memory | `110000` |
| Queries | 0 through 27 |

The [open-source harness](https://github.com/db-benchmarks/db-benchmarks) loads one collection into each engine, can pin CPU and memory in Docker, drops caches and restarts before a cold run, and stops once the coefficient of variation has settled. The Hacker News test is full-text search and aggregation over comments. The project describes that memory setting as a 110 GB limit.

OSD1 has no run on that collection or those queries. The laptop rates on this page stay in the tables above.

The part worth copying is the row, not the leaderboard. Each row is one question. Every engine in the row ran that question. Latency is reported with the rate. [ScyllaDB’s benchmark glossary](https://www.scylladb.com/glossary/database-benchmark/) makes the same demand of a comparison: name the workload, change one thing, and read a percentile beside the throughput. Their Cassandra, DynamoDB, and Bigtable multiples are their own runs. They are not repeated here.

## Debug bench

Local process, not the Docker image. Command:

```bash
./scripts/bench.sh
```

The script builds `target/debug/osd1`, creates one table, sends 1,000 parameterized inserts, then 1,000 parameterized point selects. `wall_seconds` is the elapsed time of those 2,000 requests. Averages and the max are per request.

Host: `arm64`, macOS 26.6.2 (25G83). Docker on this machine is 29.8.2 and was not used for this run.

Unedited output from 2026-11-06:

```
requests 2000
wall_seconds 0.524200
insert_avg_ms 0.218
select_avg_ms 0.284
max_ms 1.974
```

## Profiles

Release binary, one process, no API token. Command:

```bash
./scripts/profiles.sh
```

The script loads 4,000 rows and an index named `items_n`, checks that an indexed point select uses that index, then runs 2,000 HTTP calls per profile. Latency is the client-observed time of each call. Percentiles are nearest rank. `max_rss_kb` is the highest `ps -o rss=` sample during that profile. On macOS that value is kilobytes. Profiles share one server process in this order: read-heavy, balanced, write-heavy, mixed-latency. Later profiles run against a warm process.

Host: `Darwin arm64`. Unedited output from 2026-11-06:

```
binary target/release/osd1
host Darwin arm64
preload_rows 4000
index_plan items_n
profile read-heavy
ops 2000
wall_seconds 0.856485
throughput_ops_per_s 2335.125
p50_ms 0.167
p95_ms 0.487
p99_ms 0.778
max_ms 3.585
max_rss_kb 5808
profile balanced
ops 2000
wall_seconds 0.783025
throughput_ops_per_s 2554.196
p50_ms 0.155
p95_ms 0.472
p99_ms 0.951
max_ms 26.301
max_rss_kb 5408
profile write-heavy
ops 2000
wall_seconds 0.778922
throughput_ops_per_s 2567.652
p50_ms 0.143
p95_ms 0.478
p99_ms 1.639
max_ms 11.685
max_rss_kb 4752
profile mixed-latency
ops 2000
wall_seconds 0.586024
throughput_ops_per_s 3412.831
p50_ms 0.162
p95_ms 0.357
p99_ms 0.449
max_ms 5.027
max_rss_kb 6512
```
