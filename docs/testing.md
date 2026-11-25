---
title: Testing
description: Recorded checks for SQL, HTTP, workloads, restore, and the image scan.
---

# Testing

Three kinds of checks cover the product. One kind checks SQL and HTTP behavior. One kind checks the four workloads. One kind checks the security controls. The rows below are from the recorded run.

| Check | What happened |
| --- | --- |
| `cargo test --workspace` | Passed. Engine 11, HTTP 18, server 1, suite 9, types 5. The suite took 0.81 s |
| SDK | `node spike/sdk-compat.mjs` printed `sdk-compat ok` |
| Profiles | `./scripts/profiles.sh` exited 0. Numbers are on the [benchmark](benchmark.md) page. Peak memory was 6.5 MB |
| Restart | `./scripts/persist.sh` exited 0. The row was still there after restart and recreate |
| TLS | `./scripts/tls-check.sh` exited 0. Health was ok. A bad token was HTTP 401 |
| Missing volume and failed restore | `./scripts/recovery-cases.sh` exited 0 |
| Backup | `./scripts/recovery.sh` exited 0. The integrity check was `ok` |
| Scan | `cargo audit` was clean for 77 crates. Trivy found 225 issues on the recorded Debian 12.15 image. One of them is critical. This row is not a scan of the distroless `latest` or `2026-11-26` image |

## Functional categories

| Category | What is checked | Where |
| --- | --- | --- |
| SQL | CRUD, DDL, joins, indexes, CTEs | `sql_crud_ddl_joins_indexes_and_ctes` |
| Types | NULL, integers, floats, text, and rejection of blob results | `sql_types_cover_null_integer_float_text_and_reject_blobs` |
| Transactions | Commit, rollback, and a constraint failure. Client `BEGIN`, `COMMIT`, and `ROLLBACK` are denied | `transactions_commit_rollback_and_constraint_failures` |
| Concurrency | 10, 100, and 500 clients at the same time | `simultaneous_clients_insert_every_row` |
| Queue | A full queue returns HTTP 429. Drop of a waiter does not block the next request. The HTTP deadline returns HTTP 504 | `a_full_queue_rejects_overflow_and_keeps_accepted_rows`, `dropping_a_waiter_does_not_block_the_next_request`, `sql_request_timeout_returns_504` |
| Persistence | Restart, then recreate the container, on the same volume | `./scripts/persist.sh` |
| Backup | Upload, restore, and `PRAGMA integrity_check` | `./scripts/recovery.sh` |
| Security | See the security table | Cargo tests, plus `./scripts/tls-check.sh` and `./scripts/scan.sh` |
| SDK | Official `cloudflare` TypeScript client, `query` and `raw` | `node spike/sdk-compat.mjs` |
| Performance | p50, p95, p99, throughput, and max RSS | `./scripts/profiles.sh` and `./scripts/bench.sh` |
| Recovery | Process kill, a missing volume, and a failed restore | `process_kill_keeps_committed_rows_and_hides_the_token`, `./scripts/recovery-cases.sh` |
| Fuzzing | A bad API body stays a client error. A seeded mix matches a reference SQLite | `malformed_input_stays_a_client_error`, `fuzzy_mix_matches_reference_sqlite` |

Run the Rust tests:

```bash
cargo test --workspace
```

A longer fuzz run uses one seed. The default test already runs seeds 1, 7, 13, and 42.

```bash
OSD1_FUZZ_SEED=1 OSD1_FUZZ_STEPS=2000 cargo test -p osd1-engine fuzzy_mix_matches_reference_sqlite
```

The suite is in `crates/osd1-server/tests/suite.rs`. Engine tests and HTTP tests sit next to the code they check.

## Workload profiles

| Profile | Mix |
| --- | --- |
| Read-heavy | 95% indexed `SELECT` on `items_n`, 5% `INSERT` |
| Balanced | 50% indexed `SELECT`, 50% `INSERT` or `UPDATE` |
| Write-heavy | 10% indexed `SELECT`, 90% `INSERT` or `UPDATE` |
| Mixed latency | Indexed point `SELECT`, plus a 2,000-row scan on every 20th call |

The script checks `EXPLAIN QUERY PLAN` for `items_n` before it times the calls. The recorded numbers are on the [benchmark](benchmark.md) page.

## Security controls

| Control | Where it is checked |
| --- | --- |
| Bearer-token authentication | `bearer_token_is_required_when_configured`, HTTP token tests, and `./scripts/tls-check.sh` |
| TLS through a reverse proxy | `./scripts/tls-check.sh` with `deploy/nginx-tls.conf`. The OSD1 process stays on HTTP |
| Bounded request and response sizes | `oversized_body_is_rejected_before_sql` (HTTP 413). `oversized_response_is_rejected` (HTTP 400, code 7500) |
| No dynamic extension loading | `filesystem_functions_and_detach_are_denied`, `authorizer_blocks_extension_filesystem_and_attach` |
| Filesystem functions in SQLite | `load_extension`, `readfile`, and `writefile` are denied or absent in this build |
| No extra attached databases | `attach_is_denied_and_creates_no_file` and the HTTP authorizer test |
| Authorizer for unsafe SQL | The authorizer denies `ATTACH`, `DETACH`, client transactions, `writable_schema`, and the functions above |
| Database directory permissions | New directories are mode `0700`. Database files are mode `0600` (`new_database_directory_and_file_are_owner_only`). `./scripts/persist.sh` checks `/data` and the database file in the image |
| Secret redaction in logs | `startup_line_redacts_the_api_token` and `process_kill_keeps_committed_rows_and_hides_the_token`. `./scripts/persist.sh` fails if the token appears in container logs |
| Dependency and image scan | `./scripts/scan.sh` runs `cargo audit` and Trivy. Findings are on the [security scan](security-scan.md) page. A finding does not by itself mean the scanner failed to run |

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

The host for the local runs is Darwin arm64.

`cargo test --workspace --offline` exited 0. Counts: engine 11, HTTP 18, server 1, suite 9, types 5, doc-tests 0. The suite finished in 0.81 s. That run covers SQL, types, and transactions. It covers 10, 100, and 500 clients at the same time. It covers a full queue, bearer tokens, and the authorizer over HTTP. It covers bad bodies and process kill with log redaction. It covers request size, response size, and the HTTP 504 deadline.

`node spike/sdk-compat.mjs` printed `sdk-compat ok` against `target/debug/osd1`.

`./scripts/profiles.sh` exited 0. The numbers are on the [benchmark](benchmark.md) page. The release process reused one address space, in the order read-heavy, balanced, write-heavy, mixed-latency. The highest sampled RSS in that run was 6512 KB, during mixed-latency. The mixed-latency scan is `SELECT n FROM items WHERE n < 2000`.

`./scripts/persist.sh` exited 0. `/data` was mode 700. `osd1.db` and `osd1.db-wal` were mode 600. The token did not appear in the container logs. The row `persist-restart-ok` was readable after restart and after force-recreate. Bodies are on the [operations](operations.md) page.

`./scripts/tls-check.sh` exited 0. `https://127.0.0.1:8443/health` returned `{"ok":true}`. The current body also includes `ready`. A query with no token and a query with the wrong token both returned HTTP 401. A query with the configured bearer token returned `"n":1`.

`./scripts/recovery-cases.sh` exited 0. Litestream restore against `http://127.0.0.1:9` exited 1 with `dial tcp 127.0.0.1:9: connect: connection refused`. The output file was absent. After `down -v` and a new volume, `SELECT note FROM gone` returned HTTP 400 and `sqlite error 1: no such table: gone`.

`./scripts/recovery.sh` exited 0. The restored row was `litestream-restore-ok`. `PRAGMA integrity_check` returned `ok`. Bodies are on the [backup and restore](backup-restore.md) page.

`./scripts/scan.sh` exited 0. `cargo audit` 0.22.2 loaded 1409 advisories, scanned 77 crate dependencies, and exited 0 with no advisory records. Trivy (`aquasec/trivy:latest`, documentation path v0.76) scanned `osd1-osd1:latest` (Debian 12.15) and reported 225 vulnerabilities: 85 low, 103 medium, 36 high, 1 critical. That critical finding is `zlib1g` `CVE-2023-45853`, status `will_not_fix`. That scan is the recorded Debian 12.15 image. This page does not quote a scan of the distroless `latest` or `2026-11-26` image. Details are on the [security scan](security-scan.md) page.
