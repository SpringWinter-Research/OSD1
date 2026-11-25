---
title: Backup and restore
---

# Backup and restore

Litestream runs beside OSD1. It is not linked into the server. The replica is an S3 bucket. The compose backup file points that at [Silo](https://silo.pgsty.com/), an S3-compatible server, so the recovery script can run without a cloud account. Docker Hub no longer serves `minio/minio` or `minio/mc`. The service name stays `minio`, and the endpoint stays `http://minio:9000`.

Required environment for a real bucket:

| Variable | Purpose |
| --- | --- |
| `LITESTREAM_BUCKET` | Bucket name |
| `LITESTREAM_ENDPOINT` | S3 endpoint. MinIO uses `http://minio:9000` |
| `LITESTREAM_ACCESS_KEY_ID` | Access key |
| `LITESTREAM_SECRET_ACCESS_KEY` | Secret key |

`deploy/litestream.yml` reads those variables with `${VAR}` expansion. Litestream does not expand the shell form `${VAR:-default}`. The backup profile in `deploy/compose.backup.yml` sets the bucket to `osd1`, the endpoint to `http://minio:9000`, and the keys to `minioadmin`, then creates the bucket with `mcli`.

Restore drill:

```bash
./scripts/recovery.sh
```

The script builds the image, starts Silo and OSD1, inserts `litestream-restore-ok`, waits until Litestream has LTX files, stops both processes, deletes the primary database, restores it, and selects the marker. It removes the compose project on exit. Durability is claimed only when the script exits 0.

`PRAGMA wal_checkpoint` is not sent through `/query`. Each request already runs inside `BEGIN IMMEDIATE`, and that pragma then returns sqlite error 6 (`database table is locked`). The SQL thread itself runs `PRAGMA wal_checkpoint(PASSIVE)` when `osd1.db-wal` is larger than 4 MiB and the connection is in autocommit. Litestream reads the WAL after the insert commits. Raft snapshots are a separate copy: the SQL thread uses SQLite’s backup API and writes `snapshot.bin` under `<database>.raft`.

`./scripts/recovery.sh` exited 0 on 2026-11-06. Litestream in that run was v0.5.18. After the insert, `litestream ltx` printed:

```
level  min_txid          max_txid          size  created
0      0000000000000001  0000000000000001  601   2026-11-06T08:56:01Z
```

The primary files were then deleted and restored. `SELECT note FROM markers` returned:

```json
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[{"note":"litestream-restore-ok"}],"meta":{"changed_db":false,"changes":0,"duration":0.010332999999999998,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":16384,"timings":{"sql_duration_ms":0.010332999999999998}}}]}
```

The script printed `recovery ok`.

The same script now also sends `PRAGMA integrity_check` and requires `"integrity_check":"ok"`. A later run on 2026-11-06 exited 0. `litestream ltx` printed:

```
level  min_txid          max_txid          size  created
0      0000000000000001  0000000000000001  601   2026-11-06T09:30:46Z
```

`SELECT note FROM markers` returned:

```json
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[{"note":"litestream-restore-ok"}],"meta":{"changed_db":false,"changes":0,"duration":0.056292,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":16384,"timings":{"sql_duration_ms":0.056292}}}]}
```

`PRAGMA integrity_check` returned:

```json
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[{"integrity_check":"ok"}],"meta":{"changed_db":false,"changes":0,"duration":0.02175,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":16384,"timings":{"sql_duration_ms":0.02175}}}]}
```

The script printed `recovery ok`.

## Recovery point

The recovery point is the age of the newest Litestream generation. The sidecar `scripts/litestream-leader.sh` touches `OSD1_LITESTREAM_HEARTBEAT` after `litestream ltx` lists a generation. With `OSD1_LITESTREAM_HEARTBEAT` set, `/metrics` exports `osd1_litestream_last_ok_seconds` as that file's mtime, or `0` when the file is missing. `deploy/prometheus-alerts.yml` pages when the gauge is greater than 0 and more than 60 seconds behind the scrape. A gauge of 0 does not page.

The restore procedure for one database is `scripts/recovery.sh`. The runs recorded above exited 0. This document does not publish a separate timed recovery target.

In simple mode the script replicates continuously. In raft mode it replicates only while `osd1_raft_role` is 1, so the bucket follows the leader's file. `deploy/compose.leader-backup.yml` is an optional overlay for that loop. `deploy/compose.backup.yml` is unchanged, and `scripts/recovery.sh` still uses it.

## Restoring a raft cluster from the replica

Stop every node. Restore the leader's database file with the same Litestream restore the script uses. Bootstrap one process on that file (`OSD1_RAFT_BOOTSTRAP=1`) with a new raft directory. Copy that same database file to each reader before any new write. Start those readers with `OSD1_RAFT_JOIN` pointed at the bootstrapped node. Leave `OSD1_RAFT_VOTER` unset on readers. Set it on the processes that should vote, and keep the voting set at three.
