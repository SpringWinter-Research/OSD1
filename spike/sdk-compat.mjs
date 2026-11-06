import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Cloudflare from "cloudflare";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const binary = path.join(root, "target", "debug", "osd1");
const token = "test-token";

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = address.port;
      server.close((error) => (error ? reject(error) : resolve(port)));
    });
  });
}

async function waitForHealth(port) {
  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/health`);
      if (response.ok) return;
    } catch {
      // The process is still binding the port.
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error("osd1 did not become healthy");
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

const port = await freePort();
const dir = await mkdtemp(path.join(os.tmpdir(), "osd1-sdk-"));
const dbPath = path.join(dir, "osd1.db");
const child = spawn(binary, [], {
  env: {
    ...process.env,
    OSD1_DB_PATH: dbPath,
    OSD1_HTTP_ADDR: `127.0.0.1:${port}`,
    OSD1_API_TOKEN: token,
  },
  stdio: ["ignore", "pipe", "pipe"],
});
let logs = "";
child.stdout.on("data", (chunk) => {
  logs += chunk.toString();
});
child.stderr.on("data", (chunk) => {
  logs += chunk.toString();
});

try {
  await waitForHealth(port);
  const client = new Cloudflare({
    apiToken: token,
    baseURL: `http://127.0.0.1:${port}`,
    maxRetries: 0,
  });
  const databaseId = "00000000-0000-0000-0000-000000000001";
  const accountId = "023e105f4ecef8ad9ca31a8372d0c353";

  const created = [];
  for await (const row of client.d1.database.query(databaseId, {
    account_id: accountId,
    sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)",
  })) {
    created.push(row);
  }
  assert(created.length === 1 && created[0].success === true, "create did not succeed");

  const inserted = [];
  for await (const row of client.d1.database.query(databaseId, {
    account_id: accountId,
    sql: "INSERT INTO items (name) VALUES (?)",
    params: ["sdk"],
  })) {
    inserted.push(row);
  }
  assert(inserted[0].meta.changes === 1, `insert changes ${inserted[0].meta.changes}`);

  const queried = [];
  for await (const row of client.d1.database.query(databaseId, {
    account_id: accountId,
    sql: "SELECT id, name FROM items WHERE name = ?",
    params: ["sdk"],
  })) {
    queried.push(row);
  }
  assert(queried[0].results[0].name === "sdk", JSON.stringify(queried[0].results));

  const raw = [];
  for await (const row of client.d1.database.raw(databaseId, {
    account_id: accountId,
    sql: "SELECT id, name FROM items WHERE name = ?",
    params: ["sdk"],
  })) {
    raw.push(row);
  }
  assert(
    Array.isArray(raw[0].results.columns) && raw[0].results.columns[1] === "name",
    JSON.stringify(raw[0].results.columns),
  );
  assert(raw[0].results.rows[0][1] === "sdk", JSON.stringify(raw[0].results.rows));

  let invalidFailed = false;
  try {
    for await (const _row of client.d1.database.query(databaseId, {
      account_id: accountId,
      sql: "NOPE",
    })) {
      // The SDK throws before yielding when the status is not 2xx.
    }
  } catch (error) {
    invalidFailed = error.status === 400;
  }
  assert(invalidFailed, "invalid SQL did not surface as status 400");

  const unauthenticated = new Cloudflare({
    apiToken: "wrong-token",
    baseURL: `http://127.0.0.1:${port}`,
    maxRetries: 0,
  });
  let authFailed = false;
  try {
    for await (const _row of unauthenticated.d1.database.query(databaseId, {
      account_id: accountId,
      sql: "SELECT 1",
    })) {
      // Expected to throw.
    }
  } catch (error) {
    authFailed = error.status === 401;
  }
  assert(authFailed, "wrong token did not surface as status 401");

  console.log("sdk-compat ok");
} catch (error) {
  console.error(error);
  console.error(logs);
  process.exitCode = 1;
} finally {
  child.kill("SIGTERM");
  await new Promise((resolve) => child.once("exit", resolve));
  await rm(dir, { recursive: true, force: true });
}
