const test = require("node:test");
const assert = require("node:assert/strict");
const { load } = require("./helpers");
const util = load("util.js");

test("parseMoney handles common formats", () => {
  assert.deepEqual(util.parseMoney("$1,042.17"), { amount: 1042.17, currency: "USD" });
  assert.deepEqual(util.parseMoney("-$5.99"), { amount: -5.99, currency: "USD" });
  assert.deepEqual(util.parseMoney("£3.50"), { amount: 3.5, currency: "GBP" });
  assert.deepEqual(util.parseMoney("€1.234,56"), { amount: 1234.56, currency: "EUR" });
  assert.deepEqual(util.parseMoney("USD 12.00"), { amount: 12, currency: "USD" });
  assert.equal(util.parseMoney(""), null);
  assert.equal(util.parseMoney("free"), null);
});

test("parseDate handles Amazon date styles", () => {
  assert.equal(util.parseDate("Ordered on November 3, 2025"), "2025-11-03");
  assert.equal(util.parseDate("Return window closed on Dec 4, 2025"), "2025-12-04");
  assert.equal(util.parseDate("3 November 2025"), "2025-11-03");
  assert.equal(util.parseDate("2025-01-09"), "2025-01-09");
  assert.equal(util.parseDate("Delivered November 5", "2025-11-20T00:00:00Z"), "2025-11-05");
  // A year-less date far in the future relative to the reference means last year.
  assert.equal(util.parseDate("Delivered December 28", "2026-01-03T00:00:00Z"), "2025-12-28");
  assert.equal(util.parseDate("no date here"), null);
});

test("order id and ASIN extraction", () => {
  assert.equal(util.extractOrderId("Order # 112-1234567-7654321"), "112-1234567-7654321");
  assert.equal(util.extractOrderId("D01-5555555-2222222"), "D01-5555555-2222222");
  assert.equal(util.extractOrderId("nothing"), null);
  assert.equal(util.extractAsin("/dp/B0ABCDEF12?ref=x"), "B0ABCDEF12");
  assert.equal(util.extractAsin("https://www.amazon.com/gp/product/B09ZZZZZZ9/"), "B09ZZZZZZ9");
  assert.equal(util.extractAsin("/gp/help"), null);
});

test("hashObject is key-order independent", () => {
  assert.equal(util.hashObject({ a: 1, b: [1, 2] }), util.hashObject({ b: [1, 2], a: 1 }));
  assert.notEqual(util.hashObject({ a: 1 }), util.hashObject({ a: 2 }));
});
