const test = require("node:test");
const assert = require("node:assert/strict");
const { load } = require("./helpers");
const plan = load("plan.js");
const { DEFAULT_SETTINGS } = load("constants.js");

const NOW = Date.parse("2025-11-20T00:00:00Z");
const DAY = 86400000;
const iso = (ms) => new Date(ms).toISOString();

test("needsDetail: never fetched → yes", () => {
  assert.equal(plan.needsDetail({ order_id: "A" }, undefined, DEFAULT_SETTINGS, NOW), true);
});

test("needsDetail: young order refreshed every few days, old order never", () => {
  const young = { order_id: "A", order_date: "2025-11-01", status: "Delivered" };
  const old = { order_id: "B", order_date: "2023-01-01", status: "Delivered" };
  const fresh = { detail_fetched_at: iso(NOW - DAY), list_status_at_detail: "Delivered" };
  const stale = { detail_fetched_at: iso(NOW - 5 * DAY), list_status_at_detail: "Delivered" };
  assert.equal(plan.needsDetail(young, fresh, DEFAULT_SETTINGS, NOW), false);
  assert.equal(plan.needsDetail(young, stale, DEFAULT_SETTINGS, NOW), true);
  assert.equal(plan.needsDetail(old, stale, DEFAULT_SETTINGS, NOW), false);
});

test("needsDetail: status change forces refetch even for old orders", () => {
  const old = { order_id: "B", order_date: "2023-01-01", status: "Return complete" };
  assert.equal(plan.needsDetail(old, { detail_fetched_at: iso(NOW - DAY), list_status_at_detail: "Delivered" }, DEFAULT_SETTINGS, NOW), true);
});

test("autoCrawlDue respects interval, running crawls and retry throttle", () => {
  const s = { ...DEFAULT_SETTINGS, autoCrawlHours: 24 };
  assert.equal(plan.autoCrawlDue({}, s, NOW), true);
  assert.equal(plan.autoCrawlDue({ lastCrawlFinishedAt: iso(NOW - 3600000) }, s, NOW), false);
  assert.equal(plan.autoCrawlDue({ lastCrawlFinishedAt: iso(NOW - 25 * 3600000) }, s, NOW), true);
  assert.equal(plan.autoCrawlDue({ running: true, heartbeatAt: iso(NOW - 10000) }, s, NOW), false);
  assert.equal(plan.autoCrawlDue({ running: true, heartbeatAt: iso(NOW - 600000) }, s, NOW), true, "stale heartbeat");
  assert.equal(plan.autoCrawlDue({ lastAutoAttemptAt: iso(NOW - 60000) }, s, NOW), false);
  assert.equal(plan.autoCrawlDue({}, { ...s, autoCrawlHours: 0 }, NOW), false);
});
