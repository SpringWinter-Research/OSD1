# Compatibility with the Cloudflare SDK

Observed with `cloudflare@4.5.0`. `node spike/run-spike.mjs` recorded the request shape. `node spike/sdk-compat.mjs` then called a running `osd1` binary and printed `sdk-compat ok`.

## What matches the SDK

- `POST /accounts/{account_id}/d1/database/{database_id}/query`
- `POST /accounts/{account_id}/d1/database/{database_id}/raw`
- `Authorization: Bearer <token>`
- JSON body `{"sql":"...","params":["..."]}` with `account_id` only in the path
- HTTP 200 envelope `{ success, errors, messages, result }`
- `/query` rows are objects. `/raw` rows are `{ columns, rows }`
- One `result` element per SQL statement. The SDK's `SinglePage` does not request a second page
- HTTP 400 for invalid SQL. The SDK throws with `error.status === 400`
- `served_by_region: "WNAM"` is one of the SDK's declared region literals

## Differences

- Path `account_id` and `database_id` do not select a database. OSD1 has one database file. Observed: the SDK compat script used a fixed id and the server ignored it.
- `X-Auth-Email` and `X-Auth-Key` are not accepted. The SDK compat script used a bearer token. A wrong bearer token returned HTTP 401.
- `served_by_colo` is not declared on `QueryResult.Meta` in `cloudflare@4.5.0`. The spike still returned the field to the caller. OSD1 sends `LOC` unless `OSD1_SERVED_BY_COLO` is set.
- `rows_read` is the number of rows returned to the client. The SDK type describes index reads as well. The SDK does not check the number. OSD1 does not invent an index counter.
- `changes` is the delta of `sqlite3_total_changes()` for that statement. The SDK text also says `sqlite3_total_changes()`, which is a lifetime counter. The insert in `sdk-compat.mjs` observed `meta.changes === 1`.
- `duration` and `timings.sql_duration_ms` are both milliseconds spent executing that statement on the SQL thread. Queue wait is not included. The SDK does not check the unit.
- Explicit `BEGIN`, `COMMIT`, `ROLLBACK`, and `SAVEPOINT` are denied by the SQLite authorizer. Each HTTP request is one transaction.
- `ATTACH`, `DETACH`, `PRAGMA writable_schema`, and the functions `load_extension`, `readfile`, and `writefile` are denied.
- JSON parameters may be null, strings, integers, or finite floats. The SDK sends strings. Booleans, objects, and arrays are HTTP 400.
- Blob result values are rejected. The SDK compat run did not return a blob.
- A full queue is HTTP 429 with `x-should-retry: false`, because this SDK retries 429 unless that header is false.
- `GET /health` is an OSD1 route. It is not part of the D1 API.

## Not exercised

These `cloudflare@4.5.0` methods exist and OSD1 does not implement them: database create, list, get, edit, update, delete, export, and import. The Workers binding (`prepare`, `batch`, `exec`, `dump`) is a different API and is not served here.
