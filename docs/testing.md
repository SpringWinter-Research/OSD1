---
title: Testing
---

{% include nav.html %}

# Testing

Three kinds of checks: the SQL and HTTP behavior, the four workloads, and the security controls. A row below is from a run on 6 Nov 2026.

| Check | What happened |
| --- | --- |
| `cargo test --workspace` | Passed. Engine 11, HTTP 18, server 1, suite 9, types 5. The suite took 0.81 s |
| SDK | `node spike/sdk-compat.mjs` printed `sdk-compat ok` |
| Profiles | `./scripts/profiles.sh` exited 0. Numbers are on the benchmark page. Peak memory was 6.5 MB |
| Restart | `./scripts/persist.sh` exited 0. The row was still there after restart and recreate |
| TLS | `./scripts/tls-check.sh` exited 0. Health was ok. A bad token was 401 |
| Missing volume and failed restore | `./scripts/recovery-cases.sh` exited 0 |
| Backup | `./scripts/recovery.sh` exited 0. Integrity check was `ok` |
| Scan | `cargo audit` was clean for 77 crates. Trivy found 225 Debian issues, one of them critical |

The sections under this table say which file or script covers each case.

## Functional categories

| Category | What is checked | Where |
| --- | --- | --- |
| SQL | CRUD, DDL, joins, indexes, CTEs | `sql_crud_ddl_joins_indexes_and_ctes` |
| Types | NULL, integers, floats, text, and rejection of blob results | `sql_types_cover_null_integer_float_text_and_reject_blobs` |
| Transactions | Commit, rollback, constraint failure. Client `BEGIN`, `COMMIT`, and `ROLLBACK` are denied | `transactions_commit_rollback_and_constraint_failures` |
| Concurrency | 10, 100, and 500 simultaneous clients | `simultaneous_clients_insert_every_row` |
| Queue | Full queue returns 429. Dropping a waiter does not block the next request. HTTP deadline returns 504 | `a_full_queue_rejects_overflow_and_keeps_accepted_rows`, `dropping_a_waiter_does_not_block_the_next_request`, `sql_request_timeout_returns_504` |
| Persistence | Restart, then recreate the container, on the same volume | `./scripts/persist.sh` |
| Backup | Upload, restore, `PRAGMA integrity_check` | `./scripts/recovery.sh` |
| Security | See the security table | cargo tests plus `./scripts/tls-check.sh` and `./scripts/scan.sh` |
| SDK | Official `cloudflare` TypeScript client, `query` and `raw` | `node spike/sdk-compat.mjs` |
| Performance | p50, p95, p99, throughput, max RSS | `./scripts/profiles.sh`, also `./scripts/bench.sh` |
| Recovery | Process kill, missing volume, failed restore | `process_kill_keeps_committed_rows_and_hides_the_token`, `./scripts/recovery-cases.sh` |
| Fuzzing | Malformed API bodies stay client errors (desirable) | `malformed_input_stays_a_client_error` |

Rust tests:

```bash
cargo test --workspace
```

The suite lives in `crates/osd1-server/tests/suite.rs`. Engine and HTTP tests live next to the code they check.

## Workload profiles

`./scripts/profiles.sh` builds `target/release/osd1`, loads 4,000 indexed rows, and times 2,000 HTTP calls for each profile. Latency is client-observed. Percentiles use nearest rank. `max_rss_kb` is the highest `ps -o rss=` sample during that profile (kilobytes on macOS).

| Profile | Mix |
| --- | --- |
| Read-heavy | 95% indexed `SELECT` on `items_n`, 5% `INSERT` |
| Balanced | 50% indexed `SELECT`, 50% `INSERT`/`UPDATE` |
| Write-heavy | 10% indexed `SELECT`, 90% `INSERT`/`UPDATE` |
| Mixed latency | Indexed point `SELECT`, plus a 2,000-row scan every 20th call |

The script checks `EXPLAIN QUERY PLAN` for `items_n` before timing. Measured numbers are in `docs/benchmark.md`.

## Security controls

| Control | Where it is checked |
| --- | --- |
| Bearer-token authentication | `bearer_token_is_required_when_configured`, HTTP token tests, and `./scripts/tls-check.sh` |
| TLS through a reverse proxy | `./scripts/tls-check.sh` with `deploy/nginx-tls.conf`. The OSD1 process stays on HTTP |
| Bounded request and response sizes | `oversized_body_is_rejected_before_sql` (HTTP 413), `oversized_response_is_rejected` (HTTP 400, code 7500) |
| No dynamic extension loading | `filesystem_functions_and_detach_are_denied`, `authorizer_blocks_extension_filesystem_and_attach` |
| Filesystem-accessing SQLite features | `load_extension`, `readfile`, and `writefile` are denied or absent in this build |
| No arbitrary attached databases | `attach_is_denied_and_creates_no_file` and the HTTP authorizer test |
| SQLite authorizer for unsafe operations | Authorizer denies `ATTACH`, `DETACH`, client transactions, `writable_schema`, and the functions above |
| Database directory permissions | New directories are `0700` and database files are `0600` (`new_database_directory_and_file_are_owner_only`). `./scripts/persist.sh` checks `/data` and the database file inside the image |
| Secret redaction in logs | `startup_line_redacts_the_api_token`, `process_kill_keeps_committed_rows_and_hides_the_token`. `./scripts/persist.sh` fails if the token appears in container logs |
| Dependency and container vulnerability scanning | `./scripts/scan.sh` runs `cargo audit` and Trivy. Findings are recorded in `docs/security-scan.md`. A finding does not by itself mean the scan failed to run |

## Commands

```bash
cargo test --workspace
node spike/sdk-compat.mjs
./scripts/profiles.sh
./scripts/persist.sh
./scripts/tls-check.sh
./scripts/recovery-cases.sh
./scripts/recovery.sh
./scripts/scan.sh
```

`persist.sh`, `tls-check.sh`, `recovery-cases.sh`, and `recovery.sh` all publish port 8080. Run them one at a time. `tls-check.sh` also publishes port 8443. Each of those scripts removes its compose project and volume on exit.

## Recorded runs

Host for the local runs: Darwin arm64. Dates are 2026-11-06.

`cargo test --workspace --offline` exited 0. Counts: engine 11, HTTP 18, server 1, suite 9, types 5, doc-tests 0. The suite finished in 0.81s. That covers SQL, types, transactions, concurrency at 10, 100, and 500 clients, a full queue, bearer tokens, the authorizer over HTTP, malformed bodies, process kill with log redaction, request size, response size, and the 504 deadline.

`node spike/sdk-compat.mjs` printed `sdk-compat ok` against `target/debug/osd1` after those server changes.

`./scripts/profiles.sh` exited 0. The numbers are copied in `docs/benchmark.md`. The release process reused one address space, in the order read-heavy, balanced, write-heavy, mixed-latency. Highest sampled RSS in that run was 6512 KB, during mixed-latency. The mixed-latency scan is `SELECT n FROM items WHERE n < 2000`.

`./scripts/persist.sh` exited 0. `/data` was mode 700. `osd1.db` and `osd1.db-wal` were mode 600. The token did not appear in the container logs. The row `persist-restart-ok` was readable after restart and after force-recreate. Bodies are in `docs/operations.md`.

`./scripts/tls-check.sh` exited 0. `https://127.0.0.1:8443/health` returned `{"ok":true}`. A query with no token and a query with the wrong token both returned HTTP 401. A query with the configured bearer token returned `"n":1`.

`./scripts/recovery-cases.sh` exited 0. Litestream restore against `http://127.0.0.1:9` exited 1 with `dial tcp 127.0.0.1:9: connect: connection refused`. The output file was absent. After `down -v` and a new volume, `SELECT note FROM gone` returned HTTP 400 and `sqlite error 1: no such table: gone`.

`./scripts/recovery.sh` exited 0. The restored row was `litestream-restore-ok`, and `PRAGMA integrity_check` returned `ok`. Bodies are in `docs/backup-restore.md`.

`./scripts/scan.sh` exited 0. `cargo audit` 0.22.2 loaded 1409 advisories, scanned 77 crate dependencies, and exited 0 with no advisory records. Trivy (`aquasec/trivy:latest`, documentation path v0.76) scanned `osd1-osd1:latest` (debian 12.15) and reported 225 vulnerabilities: 85 low, 103 medium, 36 high, 1 critical. That critical finding is `zlib1g` `CVE-2023-45853`, status `will_not_fix`. The image is not a clean scan. Details are in `docs/security-scan.md`.
