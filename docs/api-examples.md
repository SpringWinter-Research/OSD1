---
title: API examples
description: Recorded /health, /query, and /raw calls, the MCP tools, and the HTTP status codes.
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

## Tables

Create a table, change a table, and delete a table with SQL on `POST /query`. The same statements work on `POST /raw`. One request is one transaction. There is no second table route.

Create a table:

```sql
CREATE TABLE items (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  note TEXT
);
```

`changed_db` is true after that statement.

Change a table with one of these statements:

```sql
ALTER TABLE items ADD COLUMN extra TEXT;
ALTER TABLE items RENAME COLUMN note TO body;
ALTER TABLE items RENAME TO notes;
ALTER TABLE notes DROP COLUMN extra;
```

Delete a table:

```sql
DROP TABLE notes;
```

`DROP TABLE osd1_applied` is denied. The message is `osd1_applied is reserved`.

An `fts5` table and a `vec0` table use the same `CREATE` and the same `DROP`. Change one of those tables by dropping it and creating it again.

Row changes use the same route. `INSERT` adds a row. `UPDATE` changes a row. `DELETE` removes a row.

## Model Context Protocol

`POST /mcp` speaks the Model Context Protocol. It uses the same bearer token as `/query`. Each tool call waits on the same SQL queue. One call is one transaction.

The server accepts `2025-03-26`, `2025-06-18`, `2025-11-25`, and `2026-07-28`. A legacy client sends `initialize`, then `notifications/initialized`, then `tools/list` and `tools/call`. A `2026-07-28` client calls `server/discover`. That client sends `MCP-Protocol-Version`, `Mcp-Method`, and `_meta["io.modelcontextprotocol/protocolVersion"]` on every request, and `Mcp-Name` on `tools/call`.

`GET /mcp` and `DELETE /mcp` return HTTP 405. The server keeps no session and sends no event stream.

Call `/mcp` on the process that owns the database file. `OSD1_ROUTER=1` does not execute these tools.

A missing token is HTTP 401. A node that is leaving returns HTTP 503. A SQL error, a full queue, and a timeout stay inside HTTP 200. The tool result sets `isError` to true. The text is the same message `/query` returns. A name that is not an identifier is rejected before SQL runs. The JSON-RPC code is `-32602`. `osd1_applied` is reserved.

### query

`query` runs one SQL request. `params` uses the same values as `/query`. The text is the success envelope. Text search and the vector index use this tool.

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "query",
    "arguments": { "sql": "SELECT 1 AS n", "params": [] }
  }
}
```

### list_tables

`list_tables` reads `sqlite_master`. It returns tables and views. It skips names that start with `sqlite_` and it skips `osd1_applied`.

### describe_table

`describe_table` takes one table name. The same call returns three results. The first result is `type`, `name`, and `sql` from `sqlite_master`. `sql` is the `CREATE` text. The second result is one row per column: `cid`, `name`, `type`, `notnull`, `dflt_value`, and `pk`. The third result is one row per index: `name`, `unique`, `origin`, and `partial`. An unknown name returns no rows. `osd1_applied` is reserved.

```json
{ "name": "items" }
```

### create_table

`create_table` builds `CREATE TABLE`. A name matches `^[A-Za-z_][A-Za-z0-9_]*$`. A type is `INTEGER`, `REAL`, `TEXT`, `BLOB`, or `NUMERIC`. At most one column is the primary key.

```json
{
  "name": "items",
  "columns": [
    { "name": "id", "type": "INTEGER", "primary_key": true },
    { "name": "name", "type": "TEXT", "not_null": true }
  ]
}
```

That call runs:

```sql
CREATE TABLE "items" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL)
```

### alter_table

`alter_table` takes `action` as `add_column`, `rename_column`, `rename_table`, or `drop_column`.

`drop_column` for table `notes` and column `extra` runs:

```sql
ALTER TABLE "notes" DROP COLUMN "extra"
```

### drop_table

`drop_table` runs `DROP TABLE "notes"`. A missing table is an error.

An `fts5` table and a `vec0` table still use `query` for `CREATE` and `DROP`.

## Text search

Text search is SQL in this database file. It is not a separate search server. The bundled SQLite library already includes FTS5. `load_extension` stays denied.

Send each statement with `POST /query`. One request is one transaction. In raft mode, supply the row id in the `INSERT`. The same `INSERT` and the same `MATCH` then run on each node. `datetime('now')` stays denied in raft mode.

```sql
CREATE VIRTUAL TABLE docs USING fts5(body);
INSERT INTO docs (rowid, body) VALUES (1, 'red apple');
SELECT body FROM docs WHERE docs MATCH 'apple';
```

The result column `body` is the text `red apple`.

## Vectors

Store each vector as a JSON text array. Score it with a SQL expression. Return the distance and the row text. The statement returns no blob.

The scan is the index. Use the scan while the row count is small. The row count where the scan is too slow is not measured. Those tests are on the [benchmark plans](benchmark-plans.md) page.

```sql
CREATE TABLE notes (
  id INTEGER PRIMARY KEY,
  body TEXT NOT NULL,
  embedding TEXT NOT NULL
);
INSERT INTO notes (id, body, embedding) VALUES (1, 'red apple', '[1,0,0]');
INSERT INTO notes (id, body, embedding) VALUES (2, 'green leaf', '[0,1,0]');
SELECT body, (
  SELECT sum((a.value - b.value) * (a.value - b.value))
  FROM json_each(notes.embedding) AS a
  JOIN json_each(?) AS b ON a.key = b.key
) AS distance
FROM notes
ORDER BY distance, id;
```

Bind the query vector as text. For the vector `[1,0,0]`, the first row has `body` `red apple` and `distance` 0. The second row has `body` `green leaf` and `distance` 2.

## Vector index

sqlite-vec 0.1.9 is compiled into this process. OSD1 registers it on the one connection. A client cannot call `load_extension`. The `vec0` table and its shadow tables stay in this database file.

The client sends each vector as JSON text. In raft mode, supply the row id in the `INSERT`. Raft stores that statement. The distance comes from those bytes.

```sql
CREATE TABLE notes (
  id INTEGER PRIMARY KEY,
  body TEXT NOT NULL
);
CREATE VIRTUAL TABLE notes_vec USING vec0(embedding float[3]);
INSERT INTO notes (id, body) VALUES (1, 'red apple');
INSERT INTO notes_vec (rowid, embedding) VALUES (1, '[1,0,0]');
INSERT INTO notes (id, body) VALUES (2, 'green leaf');
INSERT INTO notes_vec (rowid, embedding) VALUES (2, '[0,1,0]');
SELECT notes.id, notes.body, notes_vec.distance
FROM notes_vec
JOIN notes ON notes.id = notes_vec.rowid
WHERE notes_vec.embedding MATCH ?
  AND k = 2
ORDER BY notes_vec.distance;
```

Bind the query vector as text. For `[1,0,0]`, the first row has `id` 1, `body` `red apple`, and `distance` 0. The second row is `green leaf`.

The query does not return the embedding. An embedding cell is a blob. Blob result cells are rejected.

A `vec0` query that joins another table needs `k` in the `WHERE` clause. Without `k`, the module returns `A LIMIT or 'k = ?' constraint is required on vec0 knn queries.`
