---
title: API examples
---

# API examples

These requests were run against a local `osd1` process started with `OSD1_API_TOKEN=test-token` and `OSD1_HTTP_ADDR=127.0.0.1:8091`.

Health does not use the token and does not touch SQLite:

```bash
curl -sS http://127.0.0.1:8091/health
```

```json
{"ok":true}
```

`/query` returns one JSON object per row:

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

The official `cloudflare@4.5.0` client is exercised by `node spike/sdk-compat.mjs`. That script creates a table, inserts a bound parameter, reads it back through `query` and `raw`, and checks that invalid SQL returns status 400 and a wrong token returns status 401. The last run printed `sdk-compat ok`.
