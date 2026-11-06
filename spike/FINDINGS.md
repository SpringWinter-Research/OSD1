# Cloudflare SDK spike

Package: `cloudflare@4.5.0` (npm). Client constructed with `baseURL` pointed at a local recorder and `apiToken: "test-token"`.

`node spike/run-spike.mjs` exited 0. The SDK accepted the success fixture for `query` and `raw`. The SQL error fixture made the SDK throw status 400.

## What the SDK sends

`account_id` is a path parameter. It is not in the JSON body.

```
POST /accounts/023e105f4ecef8ad9ca31a8372d0c353/d1/database/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/query
Authorization: Bearer test-token
Content-Type: application/json

{
  "sql": "SELECT 1 AS n",
  "params": [
    "a"
  ]
}
```

`raw` uses the same body and the path suffix `/raw`. There is no query string. `query` and `raw` each made one HTTP request. `SinglePage.nextPageInfo()` returns null, so the SDK does not ask for a second page.

`params` is typed as `Array<string>`. Omitted params are omitted from the body (`sql: "NOPE"` sent no `params` field).

## What the SDK accepts

HTTP 200 JSON:

```json
{
  "success": true,
  "errors": [],
  "messages": [],
  "result": [
    {
      "success": true,
      "results": [{ "n": 1 }],
      "meta": {
        "changed_db": false,
        "changes": 0,
        "duration": 0.1,
        "last_row_id": 0,
        "rows_read": 1,
        "rows_written": 0,
        "served_by_colo": "LOC",
        "served_by_primary": true,
        "served_by_region": "WNAM",
        "size_after": 0,
        "timings": { "sql_duration_ms": 0.1 }
      }
    }
  ]
}
```

`for await` yielded that single `result[0]` object. For `/raw`, `results` is `{ "columns": ["n"], "rows": [[1]] }` and the SDK yielded that object too.

HTTP 400 with this body makes the SDK throw. `error.status` is 400. `error.error` is the parsed JSON:

```json
{
  "success": false,
  "errors": [{ "code": 7500, "message": "near \"NOPE\": syntax error" }],
  "messages": [],
  "result": []
}
```

A 200 response is success even if `success` is false, because the SDK checks `response.ok` before it reads the body. SQL failures must use a non-2xx status. This spike used 400.

## Contract for the server

- Routes: `POST /accounts/:account_id/d1/database/:database_id/query` and `.../raw`.
- Auth header: `Authorization: Bearer <OSD1_API_TOKEN>` when a token is configured.
- Body: `{ "sql": string, "params"?: string[] }`. Also accept JSON null, integer, and finite float so direct HTTP clients can bind native SQLite types. The SDK itself sends strings.
- Success: HTTP 200 and one `result` element per SQL statement.
- SQL and authorizer failures: HTTP 400 and the error envelope above. `code` 7500.
- Malformed JSON or a rejected parameter type: HTTP 400, `code` 7400.
- Missing bearer token: HTTP 401, `code` 10000.
- Full queue: HTTP 429, `code` 10001. `cloudflare@4.5.0` retries 429 up to 2 times unless `x-should-retry: false` is set. OSD1 sends `x-should-retry: false` on 429 so the SDK surfaces the queue failure instead of sleeping and retrying.

## Fields the SDK 4.5.0 types do not require

`served_by_colo` is not declared on `QueryResult.Meta` in this package. The runtime still returned it. Keep the field. `served_by_region: "WNAM"` matches the SDK union.

`rows_read` in the type means rows read including indexes. OSD1 reports rows returned to the client. The SDK does not check the number.
