require("fake-indexeddb/auto");
const test = require("node:test");
const assert = require("node:assert/strict");
const zlib = require("node:zlib");
const { load } = require("./helpers");
const { Outbox } = load("outbox.js");
const { ServerClient, flush } = load("client.js");

let n = 0;
async function outboxWith(count) {
  const ob = new Outbox({ dbName: `client-${++n}`, maxAttempts: 3 });
  await ob.observe(Array.from({ length: count }, (_, i) => ({
    type: "order", key: `K${i}`, source: "order_list", observed_at: "2025-01-01T00:00:00Z", data: { i },
  })));
  return ob;
}

function decodeBody(init) {
  const buf = Buffer.from(init.body instanceof ArrayBuffer ? new Uint8Array(init.body) : init.body);
  const text = init.headers["Content-Encoding"] === "gzip" ? zlib.gunzipSync(buf).toString() : buf.toString();
  return JSON.parse(text);
}

function fakeServer(handler) {
  const calls = [];
  const fetchImpl = async (url, init) => {
    const body = init.body ? decodeBody(init) : null;
    calls.push({ url, init, body });
    const { status = 200, json = { ok: true } } = (await handler({ url, init, body, calls })) || {};
    return new Response(JSON.stringify(json), { status });
  };
  return { calls, fetchImpl };
}

test("flush delivers in batches with gzip, auth header and stable batch ids", async () => {
  const ob = await outboxWith(5);
  const srv = fakeServer(() => ({}));
  const client = new ServerClient({ baseUrl: "http://100.64.0.1:8765/", token: "s3cret", fetchImpl: srv.fetchImpl });
  const res = await flush(ob, client, { maxRecords: 2, installId: "abc" });
  assert.deepEqual({ sent: res.sent, remaining: res.remaining, error: res.error }, { sent: 5, remaining: 0, error: null });
  assert.equal(srv.calls.length, 3);
  const first = srv.calls[0];
  assert.equal(first.url, "http://100.64.0.1:8765/v1/ingest");
  assert.equal(first.init.headers.Authorization, "Bearer s3cret");
  assert.equal(first.init.headers["Content-Encoding"], "gzip");
  assert.equal(first.body.schema_version, 1);
  assert.equal(first.body.source, "amazon");
  assert.match(first.body.batch_id, /^abc-1-2-/);
  assert.deepEqual(first.body.records.map((r) => r.key), ["K0", "K1"]);
});

test("offline: records stay cached and error is reported", async () => {
  const ob = await outboxWith(3);
  const client = new ServerClient({
    baseUrl: "http://100.64.0.1:8765",
    fetchImpl: async () => {
      throw new TypeError("Failed to fetch");
    },
  });
  const res = await flush(ob, client);
  assert.equal(res.sent, 0);
  assert.equal(res.remaining, 3);
  assert.equal(res.error.kind, "offline");
  assert.deepEqual(await ob.counts(), { outbox: 3, deadletter: 0 });
});

test("auth failure keeps records and does not count attempts", async () => {
  const ob = await outboxWith(1);
  const srv = fakeServer(() => ({ status: 401, json: { error: "unauthorized" } }));
  const res = await flush(ob, new ServerClient({ baseUrl: "http://h", fetchImpl: srv.fetchImpl }));
  assert.equal(res.error.kind, "auth");
  const [entry] = await ob.peekBatch();
  assert.equal(entry.attempts, 0);
});

test("413 halves the batch size", async () => {
  const ob = await outboxWith(4);
  const srv = fakeServer(({ body }) => (body.records.length > 1 ? { status: 413 } : {}));
  const res = await flush(ob, new ServerClient({ baseUrl: "http://h", fetchImpl: srv.fetchImpl }), { maxRecords: 4 });
  assert.equal(res.sent, 4);
  assert.equal(res.remaining, 0);
});

test("a poison record is isolated and dead-lettered; the rest are delivered", async () => {
  const ob = await outboxWith(3);
  const srv = fakeServer(({ body }) => (body.records.some((r) => r.key === "K1") ? { status: 400, json: { error: "bad" } } : {}));
  const res = await flush(ob, new ServerClient({ baseUrl: "http://h", fetchImpl: srv.fetchImpl }));
  assert.equal(res.sent, 2);
  assert.deepEqual(await ob.counts(), { outbox: 0, deadletter: 1 });
});

test("unconfigured client fails fast", async () => {
  const ob = await outboxWith(1);
  const res = await flush(ob, new ServerClient({ baseUrl: "" }));
  assert.equal(res.error.kind, "unconfigured");
  assert.equal(res.remaining, 1);
});
