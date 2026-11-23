const test = require("node:test");
const assert = require("node:assert/strict");
const { fixture, parseHtml, load } = require("./helpers");
const parsers = load("parsers.js");

const OBSERVED = "2025-11-20T12:00:00.000Z";

function listPage(name, url = "https://www.amazon.com/your-orders/orders?timeFilter=months-3") {
  return parsers.parseOrderList(parseHtml(fixture(name), url), { url, observedAt: OBSERVED });
}

test("order list: orders, header fields and items", () => {
  const res = listPage("order-list.html");
  assert.equal(res.cardCount, 3);
  assert.equal(res.orders.length, 3);
  assert.equal(res.hasNextPage, true);

  const [first, second, digital] = res.orders;
  assert.equal(first.order_id, "112-1234567-7654321");
  assert.equal(first.order_date, "2025-11-03");
  assert.equal(first.total_amount, 1042.17);
  assert.equal(first.currency, "USD");
  assert.equal("ship_to_name" in first, false, "recipient comes from order details only");
  assert.equal(first.status, "Delivered November 5");
  assert.match(first.detail_url, /^https:\/\/www\.amazon\.com\/your-orders\/order-details\?orderID=112-1234567-7654321/);
  assert.match(first.invoice_url, /invoice/);
  assert.equal(first.items.length, 2);

  const [cable, desk] = first.items;
  assert.equal(cable.asin, "B0ABCDEF12");
  assert.equal(cable.item_key, "B0ABCDEF12");
  assert.equal(cable.title, "USB-C Charging Cable, 6 ft");
  assert.equal(cable.quantity, 2);
  assert.equal(cable.return_eligible_until, "2025-12-05");
  assert.equal(cable.delivery_status, "Delivered November 5");
  assert.equal(desk.asin, "B09ZZZZZZ9");
  assert.equal(desk.quantity, 1);
  assert.equal(desk.return_window_closed_on, "2025-12-04");

  assert.equal(second.order_id, "113-7777777-1111111");
  assert.equal(second.total_amount, 24.99);
  assert.equal(second.items[0].return_status, "return_complete");

  assert.equal(digital.order_id, "D01-5555555-2222222");
  assert.equal(digital.is_digital, true);
  assert.equal(digital.items[0].asin, "B0EBOOK001");
});

test("order list: emits order + return records with raw html", () => {
  const res = listPage("order-list.html");
  const types = res.records.map((r) => `${r.type}:${r.key}`);
  assert.deepEqual(types, [
    "order:112-1234567-7654321",
    "order:113-7777777-1111111",
    "return:113-7777777-1111111:B07RETURN1",
    "order:D01-5555555-2222222",
  ]);
  const ret = res.records[2];
  assert.equal(ret.data.status, "return_complete");
  assert.equal(ret.source, "order_list");
  assert.equal(ret.observed_at, OBSERVED);
  assert.equal(ret.marketplace, "www.amazon.com");
  assert.ok(res.records[0].raw_html.startsWith("<div class=\"order-card"));
  assert.ok(!res.records[0].raw_html.includes("<script"));
});

test("order list: raw html can be disabled", () => {
  const url = "https://www.amazon.com/your-orders/orders";
  const res = parsers.parseOrderList(parseHtml(fixture("order-list.html"), url), { url, captureRawHtml: false });
  assert.ok(res.records.every((r) => r.raw_html === undefined));
});

test("order list: last page has no next link", () => {
  const res = listPage("order-list-last-page.html");
  assert.equal(res.orders.length, 1);
  assert.equal(res.hasNextPage, false);
  assert.equal(res.orders[0].order_date, "2025-08-01");
  assert.equal(res.orders[0].total_amount, 15);
});

test("order list: encrypted cards are counted, not parsed", () => {
  const res = listPage("order-list-encrypted.html");
  assert.equal(res.cardCount, 2);
  assert.equal(res.encryptedCards, 2);
  assert.equal(res.orders.length, 0);
});

test("time filters discovered from dropdown", () => {
  const doc = parseHtml(fixture("order-list.html"));
  assert.deepEqual(parsers.discoverTimeFilters(doc), ["last30", "months-3", "year-2025", "year-2024", "archived"]);
});

test("order detail (current layout): totals, payment, address, items", () => {
  const url = "https://www.amazon.com/your-orders/order-details?orderID=112-1234567-7654321";
  const { order, records } = parsers.parseOrderDetail(parseHtml(fixture("order-detail.html"), url), { url, observedAt: OBSERVED });
  assert.equal(order.order_id, "112-1234567-7654321");
  assert.equal(order.order_date, "2025-11-03");
  assert.equal(order.subtotal, 1020);
  assert.equal(order.shipping, 5.99);
  assert.equal(order.promotion, -5.99);
  assert.equal(order.total_before_tax, 1020);
  assert.equal(order.tax, 22.17);
  assert.equal(order.grand_total, 1042.17);
  assert.equal(order.total_amount, 1042.17);
  assert.equal(order.refund_total, null);
  assert.equal(order.payment_method, "Visa");
  assert.equal(order.payment_last4, "4242");
  assert.equal(order.ship_to_name, "Alex Example");
  assert.equal(order.ship_to_address, "123 Main St, SPRINGFIELD, IL 62701, United States");
  assert.equal(order.has_detail, true);
  assert.equal(order.status, "Delivered November 5, 2025");

  assert.equal(order.items.length, 2);
  const [cable, desk] = order.items;
  assert.equal(cable.title, "USB-C Charging Cable, 6 ft (2-Pack)");
  assert.equal(cable.quantity, 2);
  assert.equal(cable.unit_price, 10);
  assert.equal(cable.seller, "Cable Co");
  assert.equal(cable.condition, "New");
  assert.equal(cable.return_eligible_until, "2025-12-05");
  assert.equal(desk.unit_price, 1000);
  assert.equal(desk.seller, "Amazon.com Services, Inc");
  assert.equal(desk.return_window_closed_on, "2025-12-04");
  assert.deepEqual(records.map((r) => r.type), ["order"]);
  assert.equal(records[0].source, "order_detail");
});

test("order detail (legacy layout): refund produces a return record", () => {
  const url = "https://www.amazon.com/gp/your-account/order-details?orderID=113-7777777-1111111";
  const { order, records } = parsers.parseOrderDetail(parseHtml(fixture("order-detail-legacy-refund.html"), url), { url, observedAt: OBSERVED });
  assert.equal(order.order_id, "113-7777777-1111111");
  assert.equal(order.order_date, "2025-10-12");
  assert.equal(order.grand_total, 24.99);
  assert.equal(order.refund_total, 24.99);
  assert.equal(order.tax, 2);
  assert.equal(order.payment_method, "Mastercard");
  assert.equal(order.payment_last4, "1111");
  assert.equal(order.ship_to_name, "Alex Example");
  assert.equal(order.items.length, 1);
  const item = order.items[0];
  assert.equal(item.asin, "B07RETURN1");
  assert.equal(item.title, "Wireless Mouse");
  assert.equal(item.unit_price, 22.99);
  assert.equal(item.seller, "Gadget Store");

  const returns = records.filter((r) => r.type === "return");
  assert.equal(returns.length, 1);
  assert.equal(returns[0].key, "113-7777777-1111111:B07RETURN1");
  // "Return complete" and "refund ... issued" both appear: the more advanced state wins.
  assert.equal(returns[0].data.status, "refunded");
  assert.equal(returns[0].data.refund_amount, 24.99);
  assert.equal(returns[0].data.event_date, "2025-10-20");
});

test("returns page heuristic", () => {
  const url = "https://www.amazon.com/spr/returns/history";
  const { records } = parsers.parseReturnsPage(parseHtml(fixture("returns.html"), url), { url, observedAt: OBSERVED });
  assert.equal(records.length, 2);
  const [refund, started] = records.map((r) => r.data);
  assert.equal(refund.order_id, "113-7777777-1111111");
  assert.equal(refund.asin, "B07RETURN1");
  assert.equal(refund.status, "refunded");
  assert.equal(refund.refund_amount, 24.99);
  assert.equal(refund.event_date, "2025-10-20");
  assert.equal(started.order_id, "112-1234567-7654321");
  assert.equal(started.status, "return_started");
  assert.equal(records[0].key, "113-7777777-1111111:B07RETURN1");
});

test("blocked page detection", () => {
  assert.equal(parsers.detectBlocked(parseHtml(fixture("signin.html")), "https://www.amazon.com/ap/signin?x"), "signin");
  assert.equal(parsers.detectBlocked(parseHtml(fixture("order-list.html")), "https://www.amazon.com/your-orders/orders"), null);
  const captcha = parseHtml('<form action="/errors/validateCaptcha"><input id="captchacharacters"></form>');
  assert.equal(parsers.detectBlocked(captcha, "https://www.amazon.com/your-orders/orders"), "captcha");
});

test("year-less return deadline is resolved forward from the observation date", () => {
  const url = "https://www.amazon.com/your-orders/orders";
  const html = fixture("order-list-last-page.html").replace(
    '<div class="item-box"><div class="yohtmlc-product-title"><a href="/dp/B000000001">Notebook</a></div></div>',
    '<div class="item-box"><div class="yohtmlc-product-title"><a href="/dp/B000000001">Notebook</a></div>' +
      '<span>Return or replace items: Eligible through January 31</span>' +
      '<span>Return window closed on Nov 20</span></div>',
  );
  const { orders } = parsers.parseOrderList(parseHtml(html, url), { url, observedAt: "2025-12-01T10:00:00Z" });
  assert.equal(orders[0].items[0].return_eligible_until, "2026-01-31");
});

test("order list: live page and crawled copy of the same card fingerprint identically", () => {
  // Live DOM: Amazon's JS fills the recipient popover and swaps in 2x images under /images/W/.
  // Crawled HTML (fetch + DOMParser, no JS): empty #shipToInsertionNode, 1x images.
  const { fingerprint } = load("outbox.js");
  const live = fixture("order-list.html").replace(
    /src="https:\/\/m\.media-amazon\.com\/images\/I\/([^."]+)\.jpg"/g,
    'src="https://m.media-amazon.com/images/W/MEDIAX_1234-T4/images/I/$1._SS284_.jpg"',
  );
  const crawled = fixture("order-list.html").replace(
    /<div class="a-row yohtmlc-recipient">[\s\S]*?<\/div>/g,
    '<div class="a-row yohtmlc-recipient"><div id="shipToInsertionNode-shippingAddress-abc"></div></div>',
  );
  assert.ok(live.includes("._SS284_.jpg") && crawled.includes("shipToInsertionNode"), "variants applied");
  const url = "https://www.amazon.com/your-orders/orders?timeFilter=months-3";
  const parse = (html) => parsers.parseOrderList(parseHtml(html, url), { url, observedAt: OBSERVED }).records;
  const a = parse(live);
  const b = parse(crawled);
  assert.equal(a.length, b.length);
  a.forEach((rec, i) => assert.equal(fingerprint(rec), fingerprint(b[i]), `${rec.type} ${rec.key} differs`));
});

test("labeledValue ignores ancestor text that doesn't start with the label", () => {
  const { labeledValue } = parsers._internal;
  const doc = parseHtml(`<ul id="h"><div><span>Order placed</span><span>May 5, 2026</span></div>
    <div><div><span>Ship to</span></div><div><div id="shipToInsertionNode-x"></div></div></div></ul>`);
  assert.equal(labeledValue(doc.getElementById("h"), ["ship to"]), null);
  assert.equal(labeledValue(doc.getElementById("h"), ["order placed"]), "May 5, 2026");
});

test("imageUrl canonicalises Amazon image renditions and leaves other hosts alone", () => {
  const { imageUrl } = parsers._internal;
  const base = "https://www.amazon.com/your-orders/orders";
  const want = "https://m.media-amazon.com/images/I/00TEST+IMG1L.jpg";
  assert.equal(imageUrl("https://m.media-amazon.com/images/I/00TEST+IMG1L._SS142_.jpg", base), want);
  assert.equal(imageUrl("https://m.media-amazon.com/images/W/MEDIAX_1/images/I/00TEST+IMG1L._SS284_.jpg", base), want);
  assert.equal(imageUrl("https://m.media-amazon.com/images/I/00TEST+IMG2L._AC_SX90_SY90_.gif", base), "https://m.media-amazon.com/images/I/00TEST+IMG2L.gif");
  assert.equal(imageUrl("https://example.com/a._SS142_.jpg", base), "https://example.com/a._SS142_.jpg");
});
