import http from "node:http";

const records = [];

const successBody = {
  success: true,
  errors: [],
  messages: [],
  result: [
    {
      success: true,
      results: [{ n: 1 }],
      meta: {
        changed_db: false,
        changes: 0,
        duration: 0.1,
        last_row_id: 0,
        rows_read: 1,
        rows_written: 0,
        served_by_colo: "LOC",
        served_by_primary: true,
        served_by_region: "WNAM",
        size_after: 0,
        timings: { sql_duration_ms: 0.1 },
      },
    },
  ],
};

const errorBody = {
  success: false,
  errors: [{ code: 7500, message: "near \"NOPE\": syntax error" }],
  messages: [],
  result: [],
};

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

export function startRecorder() {
  const server = http.createServer(async (req, res) => {
    const body = await readBody(req);
    const url = new URL(req.url, "http://127.0.0.1");
    records.push({
      method: req.method,
      path: url.pathname,
      search: url.search,
      headers: req.headers,
      body,
    });
    let parsed = {};
    try {
      parsed = body ? JSON.parse(body) : {};
    } catch {
      parsed = {};
    }
    const failing = typeof parsed.sql === "string" && parsed.sql.includes("NOPE");
    const raw = url.pathname.endsWith("/raw");
    const payload = failing
      ? errorBody
      : raw
        ? {
            ...successBody,
            result: [
              {
                ...successBody.result[0],
                results: { columns: ["n"], rows: [[1]] },
              },
            ],
          }
        : successBody;
    const status = failing ? 400 : 200;
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(payload));
  });

  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      resolve({
        server,
        port: address.port,
        records,
      });
    });
  });
}
