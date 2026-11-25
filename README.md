# OSD1

OSD1 is one SQLite database file behind an HTTP API. You send native SQL. The request shape matches the Cloudflare D1 client.

Simple mode is the default. One process owns one database file. Raft mode is optional. The process chooses the mode only at startup. Raft mode copies each committed write to other processes.

[Documentation](https://springwinter-research.github.io/OSD1/) · [Image](https://gallery.ecr.aws/c5l0h3s1/osd1) · [License: MIT](LICENSE)

## Quick start

**1. Start the server**

```bash
docker pull public.ecr.aws/c5l0h3s1/osd1:latest

docker run --rm -p 8080:8080 \
  -e OSD1_API_TOKEN=dev-token \
  -v osd1-data:/data \
  public.ecr.aws/c5l0h3s1/osd1:latest
```

The tags `latest` and `2026-11-06` include `linux/arm64` and `linux/amd64`. The database file is `/data/osd1.db`. To build from this repo, run `docker compose -f deploy/compose.yml up -d --build`.

**2. Check that the server is up**

```bash
curl -sS http://127.0.0.1:8080/health
```

```json
{"ok":true,"ready":true}
```

Open [http://127.0.0.1:8080/](http://127.0.0.1:8080/) for the table explorer. Sign in with the same bearer token. Leave the token blank when the server has no token.

**3. Run a query**

```bash
curl -sS -H "Authorization: Bearer dev-token" -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  http://127.0.0.1:8080/accounts/a/d1/database/b/query
```

`/query` returns one JSON object for each row. `/raw` returns column names and array rows. `POST /mcp` runs the same SQL for a Model Context Protocol client. Health does not use the token.

From source:

```bash
cargo run -p osd1-server
```

That command starts simple mode. `OSD1_DB_PATH` defaults to `./data/osd1.db`. `OSD1_HTTP_ADDR` defaults to `127.0.0.1:8080`.

The manual has the status codes, the configuration, raft startup, and the recorded rates: [OSD1 documentation](https://springwinter-research.github.io/OSD1/). The same pages are in `docs/`.

## License

[MIT](LICENSE)
