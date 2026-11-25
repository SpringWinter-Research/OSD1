---
title: Compatibility
description: What matches cloudflare 4.5.0, and where OSD1 differs.
---

# Compatibility with the Cloudflare SDK

Observed with `cloudflare@4.5.0`. `node spike/run-spike.mjs` recorded the request shape. `node spike/sdk-compat.mjs` then called a running `osd1` binary and printed `sdk-compat ok`.

## What matches the SDK

- `POST /accounts/{account_id}/d1/database/{database_id}/query`
- `POST /accounts/{account_id}/d1/database/{database_id}/raw`
- `Authorization: Bearer <token>`
- JSON body `{"sql":"...","params":["..."]}`. `account_id` is only in the path
- HTTP 200 envelope `{ success, errors, messages, result }`
- `/query` rows are objects. `/raw` rows are `{ columns, rows }`
- One `result` element for each SQL statement. The SDK `SinglePage` does not request a second page
- HTTP 400 for invalid SQL. The SDK throws with `error.status === 400`
- `served_by_region: "WNAM"` is one of the region literals the SDK declares

## Differences

Path `account_id` and `database_id` do not select a database unless you set `OSD1_DATABASE_ID` or `OSD1_ROUTES`. One process has one database file. The SDK compat script used a fixed id. With those variables unset, the server ignored it.

`X-Auth-Email` and `X-Auth-Key` are not accepted. The compat script used a bearer token. A wrong bearer token returned HTTP 401.

`served_by_colo` is not declared on `QueryResult.Meta` in `cloudflare@4.5.0`. OSD1 still returns the field. The value is `LOC` unless you set `OSD1_SERVED_BY_COLO`.

`rows_read` is the number of rows returned to the client. The SDK type also describes index reads. The SDK does not check the number. OSD1 does not invent an index counter.

`changes` is the delta of `sqlite3_total_changes()` for that statement. The SDK text also says `sqlite3_total_changes()`, which is a lifetime counter. The insert in `sdk-compat.mjs` observed `meta.changes === 1`.

`duration` and `timings.sql_duration_ms` are both milliseconds for that statement on the SQL thread. Queue wait is not included. The SDK does not check the unit.

The authorizer denies client `BEGIN`, `COMMIT`, `ROLLBACK`, and `SAVEPOINT`. A simple-mode request, a leader read, and a raft apply are one transaction. A follower read does not open a transaction. The full deny list is on the [architecture](architecture.md) page.

JSON parameters may be null, strings, integers, or finite floats. The SDK sends strings. Booleans, objects, and arrays are HTTP 400. Blob result values are rejected. The compat run did not return a blob.

A full queue is HTTP 429 with `x-should-retry: false`. This SDK retries HTTP 429 unless that header is false.

A request body larger than `OSD1_MAX_BODY_BYTES` (default 1 MiB) is HTTP 413. SQL does not run. A success envelope larger than `OSD1_MAX_RESPONSE_BYTES` (default 1 MiB) is HTTP 400, code 7500, message `response exceeds the size limit`.

In simple mode, queue wait plus SQL longer than `OSD1_REQUEST_TIMEOUT_MS` (default 30000) is HTTP 504, code 7500, with `x-should-retry: false`. The statement is interrupted. `0` waits with no limit. A raft read uses that same deadline. A raft write waits until raft accepts or rejects it. The SDK does not retry HTTP 504. A new send is a new command. A client that gives up on its own timer can still see a committed raft write.

A raft write adds the response header `x-osd1-commit-index`. The value is the log index, and it starts at 1. A later request may send `x-osd1-min-commit-index`. In raft mode that read waits until this node applies that index. The same deadline returns HTTP 504 when it does not. Simple mode ignores a well-formed header. Zero or a non-integer value is HTTP 400, code 7400. An absent header stays unset. The header is not a D1 `meta` field.

`cloudflare@4.5.0` ignores response headers it does not know. It does not send `x-osd1-min-commit-index`. An SDK read can still be stale. `OSD1_ROUTER=1` rebuilds the success response and does not copy the header. A raft follower forwards the write on the raft port. That response still carries the header.

`GET /health` is an OSD1 route. It is outside the D1 API. While the process is up, the status is 200 and the body is `{"ok":true,"ready":true}`. `ready` is false when raft apply lags commit by more than 32. The status stays 200, so the image health check leaves a lagging follower running. A node that is leaving returns HTTP 503 and `{"ok":false,"ready":false}`. New SQL on that node is HTTP 503, code 7500, message `node is leaving the cluster`. `cloudflare@4.5.0` does not read `ready`.

The status table is on the [API examples](api-examples.md) page.

## Not exercised

These `cloudflare@4.5.0` methods exist. OSD1 does not implement them: database create, list, get, edit, update, delete, export, and import. The Workers binding (`prepare`, `batch`, `exec`, `dump`) is a different API. OSD1 does not serve it.
