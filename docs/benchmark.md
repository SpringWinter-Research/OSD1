---
title: Benchmark
description: Recorded OSD1 runs on an Apple M4, and a same-SQL comparison with SQLite.
---

# Benchmark

These runs are on an Apple M4. A release build serves the HTTP rates. Read each table as one question. Do not rank a different client count, a different statement, or a different engine on the same axis.

Tests that are not run yet are on the [benchmark plans](benchmark-plans.md) page.

## SELECT 1, many clients

A release build serves `SELECT 1` over keep-alive HTTP on the loopback interface. One client does 25,467 requests each second. Eight clients do 82,129. Sixteen clients do 102,234. Thirty-two clients do 111,075. The rate stops rising at 32 clients. The SQL thread is full. 111,075 is the highest recorded rate in simple mode.

{% include scale-chart.html %}

| Clients | Requests/s |
| ---: | ---: |
| 1 | 25,467 |
| 8 | 82,129 |
| 16 | 102,234 |
| 32 | 111,075 |

SQLite on the same machine, with no HTTP, answers a different question. The in-memory transaction rate is 2,516,325 each second. The WAL rate with `synchronous=FULL` is 543,065 transactions each second. The indexed-read rate inside one transaction is 3,447,361 each second.

## One client, four workloads

One Python client calls the release binary. Each workload is 2,000 calls. The server has 4,000 indexed rows. Latency is what that client observes. This is not the 32-client rate above. The mix for each profile is on the [testing](testing.md) page.

{% include latency-chart.html %}

| Workload | p50 | p95 | p99 | Requests/s | Memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| Read-heavy | 0.17 ms | 0.49 ms | 0.78 ms | 2,335 | 5.8 MB |
| Balanced | 0.16 ms | 0.47 ms | 0.95 ms | 2,554 | 5.4 MB |
| Write-heavy | 0.14 ms | 0.48 ms | 1.64 ms | 2,568 | 4.8 MB |
| Mixed | 0.16 ms | 0.36 ms | 0.45 ms | 3,413 | 6.5 MB |

The highest resident memory in that run is 6.5 MB. The command output is under [Profiles](#profiles).

## How to read a comparison

A benchmark is a named workload on named hardware. Throughput alone is an incomplete reading. The percentile says whether the calls sit near that rate, or whether a few calls are slow.

Two kinds of test answer two decisions. A synthetic test, such as `SELECT 1` or one indexed point select, can run on any schema. It shows the path cost. A trace from an application, such as the Hacker News queries on [db-benchmarks.com](https://db-benchmarks.com/), shows that application. One does not stand in for the other.

Rows that share a table share the question, the machine, and the data. One thing changes between those rows. A second machine, a second SQLite version, or a write that must sync is a different table.

## Same SQL, one client

The host is an Apple M4. The operating system is macOS 26.6.2 (25G83), Darwin arm64. SQLite 3.46.0 is on both sides. The harness uses rusqlite 0.32 with the bundled library. `target/release/osd1` reports the same version string. The server is in simple mode, with no API token, on the loopback interface. The table has 4,000 rows and an index named `items_n`. Each measured call is `BEGIN IMMEDIATE`, the statement, and `COMMIT`. There are 200 warmup calls, then 2,000 measured calls. Percentiles use nearest rank. The HTTP client is one Rust TCP connection with `Connection: keep-alive`. The in-process side calls rusqlite in the harness. It does not go through the OSD1 queue.

These statements are reads. `EXPLAIN QUERY PLAN` for the point select is `SEARCH items USING INDEX items_n (n=?)`. A commit of a read does not dirty a page, so this run does not sync. It is not the WAL write rate from the no-HTTP run above.

{% include compare-chart.html %}

| Engine | Question | p50 | p95 | p99 | Calls/s |
| --- | --- | ---: | ---: | ---: | ---: |
| SQLite, in process | Indexed point select, one transaction for each call | 0.002 ms | 0.003 ms | 0.003 ms | 414,450 |
| OSD1 HTTP | Same statement | 0.033 ms | 0.064 ms | 0.072 ms | 27,513 |
| SQLite, in process | `SELECT 1`, one transaction for each call | 0.002 ms | 0.002 ms | 0.002 ms | 578,083 |
| OSD1 HTTP | Same statement | 0.032 ms | 0.038 ms | 0.046 ms | 30,379 |
| SQLite, in process | Indexed point select, one transaction for all 2,000 calls | 0.001 ms | 0.001 ms | 0.001 ms | 1,000,042 |

On the indexed point select, the in-process rate is 15.1 times the OSD1 rate (414,450 / 27,513). On `SELECT 1`, the in-process rate is 19.0 times the OSD1 rate (578,083 / 30,379). The last row changes the transaction. Two thousand lookups share one `BEGIN IMMEDIATE`. One OSD1 HTTP request is one transaction, so that row has no OSD1 figure. The no-HTTP indexed-read rate inside one transaction stays 3,447,361 each second. This loop is 1,000,042 calls each second.

This client is one connection. It is not the 32-client run. The highest recorded rate in simple mode stays 111,075 requests each second.

Recorded output:

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

[db-benchmarks.com](https://db-benchmarks.com/?cache=fast_avg&engines=elasticsearch_tuned_8.14.3%2Cmanticoresearch_rowwise_6.0.2&tests=hn&memory=110000&queries=0%2C1%2C2%2C3%2C4%2C5%2C6%2C7%2C8%2C9%2C10%2C11%2C12%2C13%2C14%2C15%2C16%2C17%2C18%2C19%2C20%2C21%2C22%2C23%2C24%2C25%2C26%2C27) shows the same rules for search engines. The linked view is:

| Setting | Value |
| --- | --- |
| Cache mode | `fast_avg` |
| Engines | Elasticsearch 8.14.3, tuned. Manticore Search 6.0.2, row-wise |
| Test | `hn` |
| Memory | `110000` |
| Queries | 0 through 27 |

The [open-source harness](https://github.com/db-benchmarks/db-benchmarks) loads one collection into each engine. It can pin CPU and memory in Docker. It drops caches and restarts before a cold run. It stops once the coefficient of variation has settled. The Hacker News test is full-text search and aggregation over comments. The project describes that memory setting as a 110 GB limit.

OSD1 has no run on that collection or those queries. The laptop rates on this page stay in the tables above.

Copy the row, not a leaderboard. Each row is one question. Every engine in the row ran that question. Latency is reported with the rate. The [ScyllaDB benchmark glossary](https://www.scylladb.com/glossary/database-benchmark/) asks for the same three things: name the workload, change one thing, and read a percentile beside the throughput. The Cassandra, DynamoDB, and Bigtable multiples on that page are their runs. They are not repeated here.

## Debug bench

This is a local process, not the Docker image. Command:

```bash
./scripts/bench.sh
```

The script builds `target/debug/osd1`. It creates one table. It sends 1,000 parameterized inserts, then 1,000 parameterized point selects. `wall_seconds` is the elapsed time of those 2,000 requests. Averages and the maximum are for each request.

Host: `arm64`, macOS 26.6.2 (25G83). Docker on this machine is 29.8.2. Docker was not used for this run.

Recorded output:

```
requests 2000
wall_seconds 0.524200
insert_avg_ms 0.218
select_avg_ms 0.284
max_ms 1.974
```

## Profiles

Release binary. One process. No API token. Command:

```bash
./scripts/profiles.sh
```

The script loads 4,000 rows and an index named `items_n`. It checks that an indexed point select uses that index. It then runs 2,000 HTTP calls for each profile. Latency is the time the client observes for each call. Percentiles use nearest rank. `max_rss_kb` is the highest `ps -o rss=` sample during that profile. On macOS that value is kilobytes. Profiles share one server process in this order: read-heavy, balanced, write-heavy, mixed-latency. Later profiles run against a warm process. The mix is on the [testing](testing.md) page.

Host: `Darwin arm64`. Recorded output:

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
