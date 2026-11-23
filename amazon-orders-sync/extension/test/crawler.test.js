const test = require("node:test");
const assert = require("node:assert/strict");
const { fixture, parseHtml, load } = require("./helpers");
const { Crawler, CrawlBlocked } = load("crawler.js");
const { DEFAULT_SETTINGS } = load("constants.js");

const ORIGIN = "https://www.amazon.com";

function site(routes) {
  const requested = [];
  const fetchImpl = async (url) => {
    requested.push(url);
    const u = new URL(url);
    const key = `${u.pathname}?${u.searchParams.toString()}`;
    const hit = Object.entries(routes).find(([pattern]) => key.startsWith(pattern) || u.pathname === pattern);
    if (!hit) return Object.assign(new Response("<html><body>Not found</body></html>", { status: 404 }), {});
    const [, spec] = hit;
    const { body, finalUrl } = typeof spec === "string" ? { body: spec } : spec;
    const res = new Response(body, { status: 200 });
    Object.defineProperty(res, "url", { value: finalUrl ? `${ORIGIN}${finalUrl}` : url });
    return res;
  };
  return { requested, fetchImpl };
}

function makeCrawler(fetchImpl, overrides = {}) {
  const sent = [];
  const planned = [];
  const crawler = new Crawler({
    origin: ORIGIN,
    fetchImpl,
    parseHtml: (html) => parseHtml(html, ORIGIN),
    send: async (records) => sent.push(...records),
    plan: async (orders) => {
      planned.push(...orders.map((o) => o.order_id));
      return overrides.wantDetail ? orders.map((o) => o.order_id).filter(overrides.wantDetail) : [];
    },
    settings: { ...DEFAULT_SETTINGS, requestDelayMs: 0, captureRawHtml: false },
  });
  return { crawler, sent, planned };
}

test("recent crawl paginates, fetches wanted details, then returns", async () => {
  const s = site({
    "/your-orders/orders?timeFilter=months-3&startIndex=10": fixture("order-list-last-page.html"),
    "/your-orders/orders?timeFilter=months-3": fixture("order-list.html"),
    "/your-orders/order-details?orderID=113-7777777-1111111": fixture("order-detail-legacy-refund.html"),
    "/spr/returns/history": fixture("returns.html"),
  });
  const { crawler, sent, planned } = makeCrawler(s.fetchImpl, { wantDetail: (id) => id === "113-7777777-1111111" });
  const result = await crawler.run({ mode: "recent" });
  assert.equal(result.pages, 2);
  assert.equal(result.orders, 4);
  assert.equal(result.details, 1);
  assert.deepEqual(planned, ["112-1234567-7654321", "113-7777777-1111111", "D01-5555555-2222222", "114-0000000-0000001"]);
  assert.ok(s.requested.some((u) => u.includes("order-details?orderID=113-7777777-1111111")));
  const sources = new Set(sent.map((r) => r.source));
  assert.deepEqual([...sources].sort(), ["order_detail", "order_list", "returns_page"]);
});

test("encrypted list page falls back to disableCsd=no-js", async () => {
  const s = site({
    "/your-orders/orders?timeFilter=months-3&disableCsd=no-js": fixture("order-list-last-page.html"),
    "/your-orders/orders?timeFilter=months-3": fixture("order-list-encrypted.html"),
  });
  const { crawler, sent } = makeCrawler(s.fetchImpl);
  await crawler.run({ mode: "recent" });
  assert.ok(s.requested.some((u) => u.includes("disableCsd=no-js")));
  assert.deepEqual(sent.filter((r) => r.type === "order").map((r) => r.key), ["114-0000000-0000001"]);
});

test("full crawl walks the year filters from the dropdown", async () => {
  const s = site({
    "/your-orders/orders?timeFilter=year-2025": fixture("order-list-last-page.html"),
    "/your-orders/orders?timeFilter=year-2024": "<html><body></body></html>",
    "/your-orders/orders?": fixture("order-list.html"),
  });
  const { crawler } = makeCrawler(s.fetchImpl);
  await crawler.run({ mode: "full" });
  const filters = s.requested.map((u) => new URL(u).searchParams.get("timeFilter")).filter(Boolean);
  assert.deepEqual(filters, ["year-2025", "year-2024"]);
});

test("sign-in redirect aborts the crawl with CrawlBlocked", async () => {
  const s = site({
    "/your-orders/orders": { body: fixture("signin.html"), finalUrl: "/ap/signin?openid.return_to=x" },
  });
  const { crawler } = makeCrawler(s.fetchImpl);
  await assert.rejects(crawler.run({ mode: "recent" }), (err) => err instanceof CrawlBlocked && err.reason === "signin");
});
