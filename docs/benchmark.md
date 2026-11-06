# Benchmark

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
