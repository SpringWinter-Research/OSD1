# Operations

The image listens on `0.0.0.0:8080` and stores the only database at `/data/osd1.db`. Compose mounts a named volume there and sets `OSD1_API_TOKEN`. The default token is `dev-token` when the variable is unset.

```bash
docker compose -f deploy/compose.yml build
OSD1_API_TOKEN=dev-token docker compose -f deploy/compose.yml up -d
curl -sS -H "Authorization: Bearer dev-token" -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  http://127.0.0.1:8080/accounts/a/d1/database/b/query
docker compose -f deploy/compose.yml down
```

`down` without `-v` keeps the volume. A later `up` opens the same database file.

Recorded on this machine with image `osd1-osd1`. `GET /health` returned `{"ok":true}`. `SELECT 1 AS n` returned `result[0].results` of `[{"n":1}]`. This request created the table and inserted one row:

```json
{"sql":"CREATE TABLE kept (id INTEGER PRIMARY KEY, note TEXT); INSERT INTO kept (note) VALUES (?);","params":["survived-restart"]}
```

The response had two result elements. The insert element had `changes` 1 and `last_row_id` 1. After `docker compose -f deploy/compose.yml down` and `up -d` again, `SELECT note FROM kept` returned:

```json
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[{"note":"survived-restart"}],"meta":{"changed_db":false,"changes":0,"duration":0.010208,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":4096,"timings":{"sql_duration_ms":0.010208}}}]}
```

The named volume was `osd1_osd1-data`.

`./scripts/persist.sh` repeats that check on the current image: insert `persist-restart-ok`, restart the container, select the row, force-recreate the container, and select it again. It also checks that `/data` is mode `700`, `osd1.db` and `osd1.db-wal` are mode `600`, and that the API token does not appear in the container logs. The script removes the volume on exit.

Unedited result lines from 2026-11-06:

```
data_dir_mode 700
db_file_mode 600
wal_file_mode 600
token_absent_from_logs yes
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[{"note":"persist-restart-ok"}],"meta":{"changed_db":false,"changes":0,"duration":0.020208,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":4096,"timings":{"sql_duration_ms":0.020208}}}]}
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[{"note":"persist-restart-ok"}],"meta":{"changed_db":false,"changes":0,"duration":0.057666,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":4096,"timings":{"sql_duration_ms":0.057666}}}]}
persist ok
```

The first JSON line is after `compose restart`. The second is after `compose up -d --force-recreate`.

Litestream is not in this file. `deploy/compose.backup.yml` and `scripts/recovery.sh` add MinIO and Litestream. TLS for the same image is `./scripts/tls-check.sh`, which terminates TLS at nginx and forwards `Authorization` to OSD1.
