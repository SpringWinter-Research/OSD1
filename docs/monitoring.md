---
title: Monitoring
---

# Monitoring

`GET /metrics` speaks the Prometheus text format (`text/plain; version=0.0.4`). Point a Prometheus scrape job at it, then use that data source in Grafana.

The route does not check the bearer token and does not run SQL. Labels are a fixed set. SQL text, parameters, account ids, and database ids are not labels.

A scrape shows rates, latency, queue depth, rows, and the main database file size. It does not show the API token.

## Scrape

```yaml
scrape_configs:
  - job_name: osd1
    metrics_path: /metrics
    static_configs:
      - targets: ["127.0.0.1:8080"]
```

Use the address OSD1 listens on. The image listens on port 8080 inside the container, so a Prometheus running on the host uses the published port. No bearer token is required.

`deploy/grafana/osd1.json` is a dashboard you can import. In Grafana open Dashboards, Import, upload that file, and choose the Prometheus data source.

## Series

| Name | Labels | Meaning |
| --- | --- | --- |
| `osd1_http_requests_total` | `route`, `status` | HTTP responses. `route` is `health`, `metrics`, `query`, `raw`, or `other`. `status` is `200`, `400`, `401`, `413`, `429`, `500`, `504`, or `other` |
| `osd1_http_request_duration_seconds` | `route` | Time in the handler, including queue wait and JSON encoding |
| `osd1_sql_requests_total` | `result`, `route` | Requests that reached the queue. `result` is `ok`, `sql_error`, `queue_full`, `timeout`, `shut_down`, or `response_too_large`. `route` is `query` or `raw` |
| `osd1_sql_duration_seconds` | `route` | Time on the SQL thread. Queue wait is not included. Recorded when the result is `ok` or `response_too_large` |
| `osd1_sql_statements_total` | | Statements inside those finished requests |
| `osd1_rows_read_total` | | Rows returned |
| `osd1_rows_written_total` | | Rows written, from `sqlite3_total_changes` |
| `osd1_database_size_bytes` | | Length of the main database file after the latest finished request. The WAL is not included |
| `osd1_sql_inflight` | | Accepted requests that have not finished, including the one running on the SQL thread |
| `osd1_sql_queue_capacity` | | How many requests may wait. The running request is outside this bound |
| `osd1_build_info` | `version` | Always `1` |
| `osd1_process_start_time_seconds` | | Unix time when this process started exporting metrics |

Histogram buckets run from 50 microseconds to 10 seconds.

The scrape request itself is counted on the next scrape. The handler encodes the text, then the middleware records the response.

A missing token, a bad body, and an empty `sql` field increment the HTTP counter only. They never reach the queue, so they do not increment `osd1_sql_requests_total`. A body over the size limit is HTTP 413 and also does not run SQL.

`osd1_sql_inflight` can be one higher than `osd1_sql_queue_capacity`, because the request running on the SQL thread is not sitting in the queue.

## Queries

Request rate:

```promql
sum by (route) (rate(osd1_http_requests_total[1m]))
```

HTTP latency, 99th percentile:

```promql
histogram_quantile(0.99, sum by (le, route) (rate(osd1_http_request_duration_seconds_bucket[5m])))
```

SQL-thread latency, 99th percentile. This excludes queue wait:

```promql
histogram_quantile(0.99, sum by (le, route) (rate(osd1_sql_duration_seconds_bucket[5m])))
```

Queue:

```promql
osd1_sql_inflight
osd1_sql_queue_capacity
sum(rate(osd1_sql_requests_total{result="queue_full"}[5m]))
```

Rows and file size:

```promql
rate(osd1_rows_read_total[1m])
rate(osd1_rows_written_total[1m])
osd1_database_size_bytes
```

## What a scrape can see

Anyone who can open `GET /metrics` can see those rates, latencies, the queue, row counts, and the main file size. Keep the listen address on a network that should have that view. `/query` and `/raw` still require the bearer token when `OSD1_API_TOKEN` is set.
