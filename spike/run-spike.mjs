import Cloudflare from "cloudflare";
import { writeFile } from "node:fs/promises";
import { startRecorder } from "./record-server.mjs";

const { server, port, records } = await startRecorder();
const baseURL = `http://127.0.0.1:${port}`;
const client = new Cloudflare({
  apiToken: "test-token",
  baseURL,
});

const accountId = "023e105f4ecef8ad9ca31a8372d0c353";
const databaseId = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx";

const outcomes = [];

async function exercise(name, fn) {
  const before = records.length;
  try {
    const yielded = [];
    for await (const item of fn()) {
      yielded.push(item);
    }
    outcomes.push({
      name,
      ok: true,
      yielded,
      requests: records.slice(before),
    });
  } catch (error) {
    outcomes.push({
      name,
      ok: false,
      error: {
        name: error?.name,
        message: error?.message,
        status: error?.status,
        error: error?.error,
      },
      requests: records.slice(before),
    });
  }
}

await exercise("query", () =>
  client.d1.database.query(databaseId, {
    account_id: accountId,
    sql: "SELECT 1 AS n",
    params: ["a"],
  }),
);

await exercise("raw", () =>
  client.d1.database.raw(databaseId, {
    account_id: accountId,
    sql: "SELECT 1 AS n",
    params: ["a"],
  }),
);

await exercise("query-error", () =>
  client.d1.database.query(databaseId, {
    account_id: accountId,
    sql: "NOPE",
  }),
);

server.close();

const summary = {
  baseURL,
  sdk: "cloudflare",
  outcomes: outcomes.map((outcome) => ({
    name: outcome.name,
    ok: outcome.ok,
    error: outcome.error ?? null,
    yielded: outcome.yielded ?? null,
    requests: (outcome.requests ?? []).map((request) => ({
      method: request.method,
      path: request.path,
      search: request.search,
      authorization: request.headers.authorization ?? null,
      contentType: request.headers["content-type"] ?? null,
      body: request.body,
    })),
  })),
};

await writeFile(new URL("./spike-output.json", import.meta.url), JSON.stringify(summary, null, 2));
console.log(JSON.stringify(summary, null, 2));

const queryError = outcomes.find((outcome) => outcome.name === "query-error");
const queryErrorOk = queryError && !queryError.ok && queryError.error?.status === 400;
if (!queryErrorOk || outcomes.some((outcome) => outcome.name !== "query-error" && !outcome.ok)) {
  process.exitCode = 1;
}
