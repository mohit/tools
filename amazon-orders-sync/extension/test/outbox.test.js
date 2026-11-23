require("fake-indexeddb/auto");
const test = require("node:test");
const assert = require("node:assert/strict");
const { load } = require("./helpers");
const { Outbox } = load("outbox.js");

let n = 0;
function fresh(opts = {}) {
  return new Outbox({ dbName: `test-${++n}`, ...opts });
}

function rec(key, data, extra = {}) {
  return { type: "order", key, source: "order_list", observed_at: new Date().toISOString(), data, ...extra };
}

test("observe queues new records and skips unchanged ones", async () => {
  const ob = fresh();
  assert.deepEqual(await ob.observe([rec("A", { s: 1 }), rec("B", { s: 1 })]), { queued: 2, unchanged: 0 });
  // Same content, different observed_at and raw html → unchanged.
  assert.deepEqual(await ob.observe([rec("A", { s: 1 }, { raw_html: "<div>" })]), { queued: 0, unchanged: 1 });
  // Changed content → queued again.
  assert.deepEqual(await ob.observe([rec("A", { s: 2 })]), { queued: 1, unchanged: 0 });
  // Same key from a different source is tracked separately.
  assert.deepEqual(await ob.observe([rec("A", { s: 2 }, { source: "order_detail" })]), { queued: 1, unchanged: 0 });
  assert.equal((await ob.counts()).outbox, 4);
});

test("peekBatch is FIFO and bounded; ack removes", async () => {
  const ob = fresh();
  await ob.observe(Array.from({ length: 5 }, (_, i) => rec(`K${i}`, { i })));
  const batch = await ob.peekBatch({ maxRecords: 3 });
  assert.deepEqual(batch.map((e) => e.record.key), ["K0", "K1", "K2"]);
  await ob.ack(batch.map((e) => e.id));
  const rest = await ob.peekBatch({ maxRecords: 10 });
  assert.deepEqual(rest.map((e) => e.record.key), ["K3", "K4"]);
  const tiny = await ob.peekBatch({ maxBytes: 10 });
  assert.equal(tiny.length, 1, "always returns at least one record");
});

test("markFailed dead-letters after maxAttempts and requeue restores", async () => {
  const ob = fresh({ maxAttempts: 2 });
  await ob.observe([rec("X", { bad: true })]);
  const [entry] = await ob.peekBatch();
  assert.deepEqual(await ob.markFailed([entry.id], "400"), { dead_lettered: 0 });
  assert.deepEqual(await ob.markFailed([entry.id], "400"), { dead_lettered: 1 });
  assert.deepEqual(await ob.counts(), { outbox: 0, deadletter: 1 });
  const exported = await ob.exportAll();
  assert.equal(exported.deadletter[0].last_error, "400");
  assert.equal(await ob.requeueDeadLetters(), 1);
  assert.deepEqual(await ob.counts(), { outbox: 1, deadletter: 0 });
});

test("order states merge and reset", async () => {
  const ob = fresh();
  await ob.updateOrderStates([{ order_id: "O1", last_list_status: "Shipped" }]);
  await ob.updateOrderStates([{ order_id: "O1", detail_fetched_at: "2025-01-01T00:00:00Z" }]);
  assert.deepEqual(await ob.getOrderStates(["O1", "O2"]), {
    O1: { order_id: "O1", last_list_status: "Shipped", detail_fetched_at: "2025-01-01T00:00:00Z" },
  });
  await ob.observe([rec("A", { s: 1 })]);
  await ob.resetSyncState();
  assert.deepEqual(await ob.getOrderStates(["O1"]), {});
  assert.deepEqual(await ob.observe([rec("A", { s: 1 })]), { queued: 1, unchanged: 0 });
});

test("fingerprint ignores item order but not item content", () => {
  const { fingerprint } = require("../src/shared/outbox.js");
  const items = [{ item_key: "A", title: "a" }, { item_key: "B", title: "b" }];
  const rec = (its) => ({ type: "order", key: "1", data: { order_id: "1", items: its } });
  assert.equal(fingerprint(rec(items)), fingerprint(rec([...items].reverse())));
  assert.notEqual(fingerprint(rec(items)), fingerprint(rec([{ item_key: "A", title: "a" }, { item_key: "B", title: "c" }])));
});
