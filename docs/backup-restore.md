---
title: Backup and restore
description: Litestream beside OSD1, the recorded simple-mode restore, and raft restore steps.
---

# Backup and restore

Litestream runs beside OSD1. It is not linked into the server. The replica is an S3 bucket. The compose backup file points that bucket at [Silo](https://silo.pgsty.com/), an S3-compatible server, so the recovery script can run without a cloud account. Docker Hub no longer serves `minio/minio` or `minio/mc`. The service name stays `minio`. The endpoint stays `http://minio:9000`.

Required environment for a real bucket:

| Variable | Purpose |
| --- | --- |
| `LITESTREAM_BUCKET` | Bucket name |
| `LITESTREAM_ENDPOINT` | S3 endpoint. The local store uses `http://minio:9000` |
| `LITESTREAM_ACCESS_KEY_ID` | Access key |
| `LITESTREAM_SECRET_ACCESS_KEY` | Secret key |

`deploy/litestream.yml` reads those variables with `${VAR}` expansion. Litestream does not expand the shell form `${VAR:-default}`. The backup profile in `deploy/compose.backup.yml` sets the bucket to `osd1`, the endpoint to `http://minio:9000`, and the keys to `minioadmin`. It then creates the bucket with `mcli`.

## Restore drill

Run the script:

```bash
./scripts/recovery.sh
```

The script builds the image. It starts Silo and OSD1. It inserts `litestream-restore-ok`. It waits until Litestream has LTX files. It stops both processes. It deletes the primary database. It restores that database. It selects the marker. It removes the compose project on exit. Treat durability as shown only when the script exits 0.

Do not send `PRAGMA wal_checkpoint` through `/query`. Each request already runs inside `BEGIN IMMEDIATE`. That pragma then returns SQLite error 6 (`database table is locked`). The SQL thread itself runs `PRAGMA wal_checkpoint(PASSIVE)` when the WAL file is larger than 4 MiB and the connection is in autocommit. Litestream reads the WAL after the insert commits. Raft snapshots are a separate copy. The SQL thread uses the SQLite backup API and writes `snapshot.bin` under `<database>.raft`.

The recorded run exited 0. Litestream in that run was v0.5.18. After the insert, `litestream ltx` printed:

```
level  min_txid          max_txid          size  created
0      0000000000000001  0000000000000001  601   2026-11-06T09:30:46Z
```

The primary files were then deleted and restored. `SELECT note FROM markers` returned:

```json
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[{"note":"litestream-restore-ok"}],"meta":{"changed_db":false,"changes":0,"duration":0.056292,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":16384,"timings":{"sql_duration_ms":0.056292}}}]}
```

`PRAGMA integrity_check` returned:

```json
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[{"integrity_check":"ok"}],"meta":{"changed_db":false,"changes":0,"duration":0.02175,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":16384,"timings":{"sql_duration_ms":0.02175}}}]}
```

The script printed `recovery ok`.

## Recovery point

The recovery point is the age of the newest Litestream generation. The sidecar `scripts/litestream-leader.sh` touches `OSD1_LITESTREAM_HEARTBEAT` after `litestream ltx` lists a generation. The loop sleeps 15 seconds. With `OSD1_LITESTREAM_HEARTBEAT` set, `/metrics` exports `osd1_litestream_last_ok_seconds` as the file modification time, or `0` when the file is missing. `deploy/prometheus-alerts.yml` pages when the gauge is greater than 0 and more than 60 seconds behind the scrape. A gauge of 0 does not page.

The restore procedure for one database is `scripts/recovery.sh`. The recorded run above exited 0. This page does not publish a separate timed recovery target.

In simple mode the script replicates continuously. In raft mode it replicates only while `osd1_raft_role` is 1, so the bucket follows the leader file. `deploy/compose.leader-backup.yml` is an optional overlay for that loop. `deploy/compose.backup.yml` is unchanged. `scripts/recovery.sh` still uses that file.

## Restore a raft cluster from the replica

1. Stop every node.
2. Restore the leader database file with the same Litestream restore the script uses.
3. Bootstrap one process on that file with `OSD1_RAFT_BOOTSTRAP=1` and a new raft directory.
4. Copy that same database file, plus `-wal` and `-shm`, to each other node before any new write.
5. Leave the old `.raft` directory behind.
6. Start the other nodes with `OSD1_RAFT_JOIN` pointed at the bootstrap node.
7. Leave `OSD1_RAFT_VOTER` unset on readers.
8. Set `OSD1_RAFT_VOTER=1` on the processes that should vote.
9. Keep the voting set at three.

Do not copy a `.raft` directory from a diverged node into a live cluster.
