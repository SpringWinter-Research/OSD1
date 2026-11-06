---
title: Benchmark
---

{% include nav.html %}

# Benchmark

Apple M4, 6 Nov 2026. The tables are the same runs as the blocks below.

A release build serving `SELECT 1` over keep-alive HTTP reached 25,467 requests/s with one client and 111,075 requests/s with 32 clients. Eight clients did 82,129/s and 16 clients did 102,234/s. The rate flattened, so the SQL thread was full.

SQLite on the same machine, with no HTTP: 2,516,325 in-memory transactions per second, 543,065 WAL `synchronous=FULL` transactions per second, and 3,447,361 indexed reads per second inside one transaction.

The four workloads below used one Python client against the release binary. They are latency for that client, not the 111k ceiling.

| Workload | p50 | p95 | p99 | Requests/s | Memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| Read-heavy | 0.17 ms | 0.49 ms | 0.78 ms | 2,335 | 5.8 MB |
| Balanced | 0.16 ms | 0.47 ms | 0.95 ms | 2,554 | 5.4 MB |
| Write-heavy | 0.14 ms | 0.48 ms | 1.64 ms | 2,568 | 4.8 MB |
| Mixed | 0.16 ms | 0.36 ms | 0.45 ms | 3,413 | 6.5 MB |

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
