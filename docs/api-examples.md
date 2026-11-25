---
title: API examples
description: Recorded /health, /query, and /raw calls, plus the HTTP status codes.
---

# API examples

These requests ran against a local `osd1` process. It was started with `OSD1_API_TOKEN=test-token` and `OSD1_HTTP_ADDR=127.0.0.1:8091`.

Health does not use the token. Health does not touch SQLite. The sample body is the current shape. Status rules are on the [monitoring](monitoring.md) page.

```bash
curl -sS http://127.0.0.1:8091/health
```

```json
{"ok":true,"ready":true}
```

`/query` returns one JSON object for each row:

```bash
curl -sS -H "Authorization: Bearer test-token" -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  http://127.0.0.1:8091/accounts/a/d1/database/b/query
```

```json
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[{"n":1}],"meta":{"changed_db":false,"changes":0,"duration":0.0045000000000000005,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":4096,"timings":{"sql_duration_ms":0.0045000000000000005}}}]}
```

`/raw` returns column names and array rows:

```bash
curl -sS -H "Authorization: Bearer test-token" -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  http://127.0.0.1:8091/accounts/a/d1/database/b/raw
```

```json
{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":{"columns":["n"],"rows":[[1]]},"meta":{"changed_db":false,"changes":0,"duration":0.003792,"last_row_id":0,"rows_read":1,"rows_written":0,"served_by_colo":"LOC","served_by_primary":true,"served_by_region":"WNAM","size_after":4096,"timings":{"sql_duration_ms":0.003792}}}]}
```

## Status codes

| Status | Code | Meaning |
| --- | --- | --- |
| 200 | | The SQL ran, and the encoded envelope fits |
| 400 | 7500 | SQLite or the authorizer rejected the SQL, or the JSON answer is too large |
| 400 | 7400 | Bad JSON, empty `sql`, a bad parameter, or a bad `x-osd1-min-commit-index`. An absent header stays unset |
| 401 | 10000 | Missing or wrong bearer token |
| 404 | 7400 | `database_id` matches neither `OSD1_DATABASE_ID` nor `OSD1_ROUTES` |
| 413 | | The body is over the size limit. SQL does not run |
| 429 | 10001 | The queue is full. The header is `x-should-retry: false` |
| 500 | 7500 | The SQL thread has stopped |
| 502 | 7400 | The router could not reach the upstream |
| 503 | 7500 | The node is leaving the cluster. The message is `node is leaving the cluster`. The request is not queued |
| 504 | 7500 | Timed out. The message is `sql request timed out`. The header is `x-should-retry: false` |

A bad `x-osd1-min-commit-index` uses this message: `x-osd1-min-commit-index must be an integer of 1 or more`. Simple mode ignores a well-formed header. The read rule is on the [low-level design](low-level.md) page.

Simple mode and a raft read interrupt the statement at `OSD1_REQUEST_TIMEOUT_MS`. A raft read that waits on `x-osd1-min-commit-index` uses that same deadline. A raft write waits for raft. The SDK does not retry HTTP 504. A new send is a new command.

## Parameters and metadata

Parameters may be null, text, integers, or finite floats. Booleans, objects, and arrays are HTTP 400. OSD1 keeps the spaces around `sql`.

A success envelope has `success`, `errors`, `messages`, and `result`. One `result` element is one SQL statement. `/query` rows are objects. `/raw` rows are `{ columns, rows }`.

Each result can include these meta fields: `changed_db`, `changes`, `duration`, `last_row_id`, `rows_read`, `rows_written`, `served_by_colo`, `served_by_primary`, `served_by_region`, `size_after`, and `timings.sql_duration_ms`.

`duration` and `timings.sql_duration_ms` are both the milliseconds on the SQL thread. Queue wait is outside that number. `rows_read` is the number of rows returned. `changes` is the delta of `sqlite3_total_changes` for that statement. `changed_db` is true when that delta is not zero, or when the authorizer saw a schema change. `size_after` is the length of the main database file. The WAL length is not added. Blob result cells are rejected.

SDK differences are on the [compatibility](compatibility.md) page. `node spike/sdk-compat.mjs` runs the official client against a local binary. The last run printed `sdk-compat ok`.
