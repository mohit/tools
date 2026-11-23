/*
 * DOM parsers for Amazon order pages. Pure functions over a Document/Element so they run
 * unchanged in content scripts (live DOM or DOMParser output) and in node tests (jsdom).
 *
 * Amazon has no stable markup contract, so every field is located with an ordered list of
 * selectors (newest layout first) and then a text heuristic as a last resort. Selector lists
 * were cross-checked against the MIT-licensed `amazon-orders` Python project (alexdlaird).
 * When Amazon changes layout: capture the page (raw_html is kept server-side), add it as a
 * fixture under test/fixtures, and extend the selector list rather than replacing it.
 *
 * Avoid :has()/:is() — not every DOM implementation we run on supports them.
 */
(function (root, factory) {
  const util = (root.AOS && root.AOS.util) || (typeof require === "function" ? require("./util.js") : null);
  const mod = factory(util);
  root.AOS = Object.assign(root.AOS || {}, { parsers: mod });
  if (typeof module === "object" && module.exports) module.exports = mod;
})(typeof globalThis !== "undefined" ? globalThis : self, function (util) {
  "use strict";

  const { cleanText, textOf, parseMoney, parseDate, extractOrderId, extractAsin, parseIntSafe, hashString } = util;

  const SEL = {
    orderCard: ["div.order-card", ".js-order-card", "div.order"],
    csdEncrypted: "div.csd-encrypted-sensitive",
    orderId: ["[data-component='orderId']", ".yohtmlc-order-id bdi[dir='ltr']", ".yohtmlc-order-id span[dir='ltr']",
      ".order-date-invoice-item bdi[dir='ltr']", ".order-date-invoice-item span[dir='ltr']", "bdi[dir='ltr']", "span[dir='ltr']"],
    orderTotal: ["div.yohtmlc-order-total span.value", ".yohtmlc-order-total .a-color-secondary.value"],
    detailLink: ["a.yohtmlc-order-details-link", "a[href*='/your-orders/order-details']", "a[href*='order-details']"],
    invoiceLink: ["a[href*='/your-orders/invoice']", "a[href*='print.html']", "a[href*='invoice']"],
    shipment: ["[data-component='shipments'] .a-box", "div.shipment", "div.delivery-box", ".js-shipment-info-container"],
    shipmentStatus: [".delivery-box__primary-text", ".yohtmlc-shipment-status-primaryText", ".od-status-message",
      "[data-component='shipmentStatus']", ".js-shipment-info-container .a-row"],
    itemContainer: ["[data-component='purchasedItems'] .a-fixed-left-grid", ".item-box", ".yo-enhanced-flex-card",
      ".yo-enhanced-card", ".yohtmlc-item"],
    itemTitle: ["[data-component='itemTitle']", ".yohtmlc-product-title", ".yo-enhanced-title", ".yohtmlc-item a"],
    itemLink: ["[data-component='itemTitle'] a", ".yohtmlc-product-title a", ".yo-enhanced-title a", ".yohtmlc-item a"],
    itemQty: [".od-item-view-qty", ".item-view-qty", ".product-image__qty", ".hzsearch-item-view-qty"],
    itemPrice: ["[data-component='unitPrice'] .a-offscreen", "[data-component='unitPrice'] .a-text-price",
      "[data-component='unitPrice']", ".yohtmlc-item .a-color-price", ".a-color-price"],
    itemSeller: ["[data-component='orderedMerchant']"],
    itemReturn: ["[data-component='itemReturnEligibility']", ".yo-enhanced-return"],
    productLink: "a[href*='/dp/'], a[href*='/gp/product/'], a[href*='/gp/aw/d/']",
    detailRoot: ["div#orderDetails", "div#ordersContainer", "div#odp-main-section", "[data-component='orderDetails']"],
    detailOrderDate: ["[data-component='orderDate']", ".order-date-invoice-item"],
    subtotalRows: ["[data-component='orderSubtotals'] div.a-row", "div#od-subtotals div.a-row",
      "[data-component='chargeSummary'] div.od-line-item-row"],
    subtotalValue: ["div.a-span-last", ".od-line-item-row-content", ".a-text-right"],
    paymentName: ["[data-testid='payment-instrument-name']", "img.pmts-payment-credit-card-instrument-logo"],
    paymentNumber: ["[data-testid='payment-instrument-number']"],
    address: ["div.displayAddressDiv", "[data-component='shippingAddress']"],
    addressName: ["li.displayAddressFullName"],
    nextPage: ["ul.a-pagination li.a-last:not(.a-disabled) a", ".a-pagination .a-last a"],
    timeFilterOptions: ["select[name='timeFilter'] option", "select#time-filter option", "select[name='orderFilter'] option"],
    captcha: ["form[action*='validateCaptcha']", "#captchacharacters"],
    signIn: ["form[name='signIn']", "#ap_email", "#ap_password"],
  };

  // Subtotal row label → field. First match wins, so more specific labels come first.
  const SUBTOTAL_LABELS = [
    ["grand_total", [/grand total/, /total for this order/, /^order total/]],
    ["refund_total", [/refund total/, /^refund/]],
    ["total_before_tax", [/before tax/, /before vat/]],
    ["tax", [/estimated tax/, /tax collected/, /estimated vat/, /^vat\b/, /^tax\b/]],
    ["subtotal", [/subtotal/]],
    ["promotion", [/promotion/, /free shipping/, /discount/, /coupon/, /savings/]],
    ["shipping", [/shipping/, /postage/, /delivery/]],
    ["gift_card", [/gift card/]],
  ];

  // Item/shipment status text → normalised return status. Return *window* text
  // ("Return window closed", "Eligible through") is not a return and is excluded first.
  const RETURN_WINDOW_RE = /(return window|eligible (?:for return )?(?:through|until)|return or replace items?|return items?:)/i;
  const RETURN_STATUS_PATTERNS = [
    ["return_cancelled", /return (?:was )?cancel/i],
    ["refunded", /\brefund(?:ed| issued| has been issued| processed| completed)\b|\byour refund\b/i],
    ["return_complete", /\breturn (?:is )?complete/i],
    ["return_received", /\breturn (?:was )?received|\bwe(?:'ve| have) received your return/i],
    ["return_in_transit", /\breturn (?:is )?(?:in transit|on (?:its|the) way|dropped off|shipped)/i],
    ["return_started", /\breturn (?:started|requested|initiated|created|in progress)|\breturn label\b/i],
    ["replacement", /\breplacement (?:ordered|requested|shipped|sent|issued)/i],
  ];

  // --- small DOM helpers ---------------------------------------------------------------

  function qsa(rootEl, selector) {
    try {
      return Array.from(rootEl.querySelectorAll(selector));
    } catch {
      return [];
    }
  }

  function first(rootEl, selectors) {
    for (const sel of [].concat(selectors)) {
      for (const el of qsa(rootEl, sel)) {
        if (textOf(el) || el.tagName === "IMG" || el.tagName === "A") return el;
      }
    }
    return null;
  }

  function exists(rootEl, selectors) {
    return [].concat(selectors).some((sel) => qsa(rootEl, sel).length > 0);
  }

  function allOfFirst(rootEl, selectors) {
    for (const sel of [].concat(selectors)) {
      const found = qsa(rootEl, sel);
      if (found.length) return { selector: sel, elements: found };
    }
    return { selector: null, elements: [] };
  }

  /** Drop elements nested inside other elements of the same list (keeps outermost). */
  function outermost(elements) {
    return elements.filter((el) => !elements.some((other) => other !== el && other.contains(el)));
  }

  /** Elements that directly own a non-empty text node, in document order. */
  function textBearers(rootEl) {
    const out = [];
    for (const el of [rootEl, ...qsa(rootEl, "*")]) {
      if (el.tagName === "SCRIPT" || el.tagName === "STYLE") continue;
      for (const node of el.childNodes || []) {
        if (node.nodeType === 3 && node.nodeValue.trim()) {
          out.push(el);
          break;
        }
      }
    }
    return out;
  }

  function findLine(rootEl, re) {
    for (const el of textBearers(rootEl)) {
      const t = textOf(el);
      if (t && t.length < 300 && re.test(t)) return t;
    }
    return null;
  }

  /**
   * Header-style label/value lookup: find an element whose whole text is one of `labels`
   * (e.g. "Order placed") and return the text of what follows it, walking up a couple of
   * levels to handle `<div><span>Label</span></div><div><span>Value</span></div>`.
   */
  function labeledValue(rootEl, labels) {
    const wanted = labels.map((l) => l.toLowerCase());
    for (const el of qsa(rootEl, "span, div, li, dt, h5, label")) {
      const t = textOf(el).toLowerCase().replace(/:$/, "");
      if (!wanted.includes(t)) continue;
      let node = el;
      for (let depth = 0; depth < 3 && node; depth++) {
        let sib = node.nextElementSibling;
        while (sib && !textOf(sib)) sib = sib.nextElementSibling;
        if (sib) return textOf(sib);
        node = node.parentElement;
        if (node && node !== rootEl) {
          const rest = cleanText(textOf(node).slice(textOf(el).length));
          if (rest) return rest;
        }
      }
    }
    return null;
  }

  function absUrl(href, base) {
    if (!href) return null;
    try {
      return new URL(href, base).toString();
    } catch {
      return null;
    }
  }

  function sanitizedHtml(el, maxChars) {
    if (!el) return null;
    const clone = el.cloneNode(true);
    for (const junk of qsa(clone, "script, style, noscript, iframe, svg, link")) junk.remove();
    let html = clone.outerHTML || "";
    html = html.replace(/\s{2,}/g, " ");
    return html.length > maxChars ? html.slice(0, maxChars) : html;
  }

  // --- field extractors -----------------------------------------------------------------

  // When several lines match, the most advanced state wins (a page can say both
  // "Return started" and "Refund issued" for the same item).
  const RETURN_STATUS_RANK = {
    refunded: 7, return_complete: 6, return_cancelled: 5, return_received: 4,
    return_in_transit: 3, replacement: 2, return_started: 1,
  };

  function returnInfo(texts) {
    let best = null;
    for (const t of texts) {
      if (!t || RETURN_WINDOW_RE.test(t)) continue;
      for (const [code, re] of RETURN_STATUS_PATTERNS) {
        if (re.test(t)) {
          if (!best || RETURN_STATUS_RANK[code] > RETURN_STATUS_RANK[best.status]) best = { status: code, text: t };
          break;
        }
      }
    }
    if (!best) return null;
    const refundLine = /refund/i.test(best.text) && parseMoney(best.text)
      ? best.text
      : texts.find((t) => t && /refund/i.test(t) && parseMoney(t));
    const refund = refundLine ? parseMoney(refundLine) : null;
    return {
      status: best.status,
      status_text: best.text,
      event_date: parseDate(best.text) || (refundLine ? parseDate(refundLine) : null),
      refund_amount: refund ? refund.amount : null,
      currency: refund ? refund.currency : null,
    };
  }

  function itemReturnWindow(texts) {
    const out = { return_policy_text: null, return_eligible_until: null, return_window_closed_on: null };
    for (const t of texts) {
      if (!t || !/return/i.test(t)) continue;
      if (/window closed|no longer eligible/i.test(t)) {
        out.return_policy_text = t;
        out.return_window_closed_on = parseDate(t);
        break;
      }
      if (/eligible|return or replace/i.test(t)) {
        out.return_policy_text = t;
        out.return_eligible_until = parseDate(t);
        break;
      }
    }
    return out;
  }

  /** Group product links by ASIN and climb to the largest ancestor holding only that ASIN. */
  function itemContainersFromLinks(rootEl) {
    const links = qsa(rootEl, SEL.productLink).filter((a) => extractAsin(a.getAttribute("href")));
    const byAsin = new Map();
    for (const a of links) {
      const asin = extractAsin(a.getAttribute("href"));
      if (!byAsin.has(asin)) byAsin.set(asin, a);
    }
    const containers = [];
    for (const [asin, link] of byAsin) {
      let node = link;
      while (node.parentElement && node.parentElement !== rootEl) {
        const parent = node.parentElement;
        const foreign = qsa(parent, SEL.productLink).some((a) => {
          const other = extractAsin(a.getAttribute("href"));
          return other && other !== asin;
        });
        if (foreign) break;
        node = parent;
      }
      containers.push(node);
    }
    return outermost(containers);
  }

  function findItemContainers(rootEl) {
    const { selector, elements } = allOfFirst(rootEl, SEL.itemContainer);
    if (elements.length) {
      const els = selector === ".yohtmlc-item" ? elements.map((e) => e.parentElement || e) : elements;
      const withProduct = outermost(els).filter((el) => qsa(el, SEL.productLink).length || textOf(el));
      if (withProduct.length) return withProduct;
    }
    return itemContainersFromLinks(rootEl);
  }

  function parseItem(itemEl, ctx, extra) {
    const productLinks = qsa(itemEl, SEL.productLink);
    const linkEl = first(itemEl, SEL.itemLink) ||
      productLinks.slice().sort((a, b) => textOf(b).length - textOf(a).length)[0] || null;
    const href = linkEl ? linkEl.getAttribute("href") : null;
    let asin = extractAsin(href);
    if (!asin) {
      for (const a of productLinks) {
        asin = extractAsin(a.getAttribute("href"));
        if (asin) break;
      }
    }
    const img = itemEl.querySelector("img");
    const titleEl = first(itemEl, SEL.itemTitle);
    let title = textOf(titleEl);
    if (!title) {
      title = productLinks.map(textOf).sort((a, b) => b.length - a.length)[0] || "";
    }
    if (!title && img) title = cleanText(img.getAttribute("alt"));
    if (!title && !asin) return null;

    const lines = textBearers(itemEl).map(textOf).filter(Boolean);
    let quantity = parseIntSafe(textOf(first(itemEl, SEL.itemQty)));
    if (quantity == null) {
      const q = lines.find((t) => /^(?:qty|quantity)\s*:?\s*\d+/i.test(t));
      quantity = q ? parseIntSafe(q) : 1;
    }
    const priceEl = first(itemEl, SEL.itemPrice);
    const price = priceEl ? parseMoney(textOf(priceEl), ctx.currency) : null;

    const sellerEl = first(itemEl, SEL.itemSeller);
    const sellerLine = sellerEl ? textOf(sellerEl) : findLine(itemEl, /^sold by\s*:/i);
    const seller = sellerLine ? cleanText(sellerLine.replace(/^sold by\s*:?/i, "")) || null : null;
    const conditionLine = findLine(itemEl, /^condition\s*:/i);
    const subscriptionLine = findLine(itemEl, /^auto-delivered\s*:/i);

    const returnEls = qsa(itemEl, SEL.itemReturn.join(", ")).map(textOf);
    const retWindow = itemReturnWindow(returnEls.concat(lines));
    const ret = returnInfo(lines.concat(extra.statusText ? [extra.statusText] : []));

    return {
      asin: asin || null,
      title: title || null,
      item_url: absUrl(href, ctx.url),
      image_url: img ? absUrl(img.getAttribute("src") || img.getAttribute("data-src"), ctx.url) : null,
      quantity: quantity || 1,
      unit_price: price ? price.amount : null,
      currency: (price && price.currency) || ctx.currency || null,
      seller,
      condition: conditionLine ? cleanText(conditionLine.replace(/^condition\s*:?/i, "")) : null,
      subscription: subscriptionLine ? cleanText(subscriptionLine.replace(/^auto-delivered\s*:?/i, "")) : null,
      delivery_status: extra.statusText || null,
      ...retWindow,
      _return: ret,
    };
  }

  /** Parse items, grouped by shipment where the page shows shipments. Assigns stable item_keys. */
  function parseItemsWithShipments(rootEl, ctx, fallbackStatus) {
    const shipments = outermost(allOfFirst(rootEl, SEL.shipment).elements);
    const groups = shipments.length ? shipments : [rootEl];
    const items = [];
    const seen = new Set();
    for (const group of groups) {
      const statusText = textOf(first(group, SEL.shipmentStatus)) || fallbackStatus || null;
      for (const el of findItemContainers(group)) {
        if (seen.has(el)) continue;
        seen.add(el);
        const item = parseItem(el, ctx, { statusText });
        if (item) items.push(item);
      }
    }
    if (!items.length && shipments.length) {
      // Shipment boxes matched but contained no items: fall back to whole root.
      for (const el of findItemContainers(rootEl)) {
        const item = parseItem(el, ctx, { statusText: fallbackStatus });
        if (item) items.push(item);
      }
    }
    const counts = new Map();
    for (const item of items) {
      const base = item.asin || `t-${hashString(item.title || "")}`;
      const n = counts.get(base) || 0;
      counts.set(base, n + 1);
      item.item_key = n ? `${base}#${n + 1}` : base;
    }
    return items;
  }

  function splitReturns(order, ctx) {
    const returns = [];
    for (const item of order.items) {
      const ret = item._return;
      delete item._return;
      if (!ret) continue;
      item.return_status = ret.status;
      returns.push({
        return_key: `${order.order_id}:${item.item_key}`,
        order_id: order.order_id,
        item_key: item.item_key,
        asin: item.asin,
        title: item.title,
        ...ret,
        currency: ret.currency || item.currency || ctx.currency || null,
      });
    }
    return returns;
  }

  function record(type, key, data, ctx, rawEl) {
    const rec = {
      type,
      key,
      source: ctx.source,
      source_url: ctx.url,
      marketplace: ctx.host,
      observed_at: ctx.observedAt,
      data,
    };
    if (ctx.captureRawHtml && rawEl) rec.raw_html = sanitizedHtml(rawEl, ctx.rawHtmlMaxChars || 200 * 1024);
    return rec;
  }

  function makeCtx(opts, source) {
    const url = opts.url || "https://www.amazon.com/";
    let host = "www.amazon.com";
    try {
      host = new URL(url).host;
    } catch {
      /* keep default */
    }
    return {
      url,
      host,
      source,
      observedAt: opts.observedAt || new Date().toISOString(),
      captureRawHtml: opts.captureRawHtml !== false,
      rawHtmlMaxChars: opts.rawHtmlMaxChars,
      currency: opts.currency || (host.endsWith(".com") ? "USD" : null),
    };
  }

  // --- page-level parsers ---------------------------------------------------------------

  function detectBlocked(doc, url) {
    const path = (() => {
      try {
        return new URL(url).pathname;
      } catch {
        return "";
      }
    })();
    if (/^\/ap\/(signin|mfa|cvf)/.test(path) || exists(doc, SEL.signIn)) return "signin";
    if (exists(doc, SEL.captcha) || /\/errors\/validateCaptcha/.test(path)) return "captcha";
    return null;
  }

  function isEncryptedCard(card) {
    if (!card.querySelector(SEL.csdEncrypted)) return false;
    return !extractOrderId(textOf(first(card, SEL.orderId)) || textOf(card));
  }

  function parseOrderCard(card, ctx) {
    const header = card.querySelector(".order-header, .a-box-inner, .a-box") || card;
    let orderId = null;
    for (const sel of SEL.orderId) {
      for (const el of qsa(card, sel)) {
        orderId = extractOrderId(textOf(el));
        if (orderId) break;
      }
      if (orderId) break;
    }
    if (!orderId) {
      const link = first(card, SEL.detailLink);
      const href = link ? link.getAttribute("href") || "" : "";
      orderId = extractOrderId(decodeURIComponent(href)) || extractOrderId(textOf(card));
    }
    if (!orderId) return null;

    const dateText = labeledValue(header, ["order placed", "ordered on", "date ordered"]) ||
      findLine(header, /order placed|ordered on/i);
    const totalText = textOf(first(card, SEL.orderTotal)) || labeledValue(header, ["total", "order total"]);
    const total = parseMoney(totalText, ctx.currency);
    const shipTo = labeledValue(header, ["ship to", "deliver to", "sent to"]);
    const detailEl = first(card, SEL.detailLink);
    const invoiceEl = first(card, SEL.invoiceLink);
    const status = textOf(first(card, SEL.shipmentStatus)) || null;
    const cardText = textOf(card);

    const order = {
      order_id: orderId,
      order_date: parseDate(dateText),
      total_amount: total ? total.amount : null,
      currency: (total && total.currency) || ctx.currency || null,
      status,
      is_cancelled: /\bcancell?ed\b/i.test(status || "") || null,
      is_digital: orderId.startsWith("D") || /digital order/i.test(cardText) || null,
      ship_to_name: shipTo ? shipTo.split(/\s{2,}|\n/)[0] : null,
      detail_url: detailEl ? absUrl(detailEl.getAttribute("href"), ctx.url) : null,
      invoice_url: invoiceEl ? absUrl(invoiceEl.getAttribute("href"), ctx.url) : null,
      items: parseItemsWithShipments(card, ctx, status),
    };
    order.item_count = order.items.length;
    return order;
  }

  /**
   * Parse an order history page.
   * @returns {{records: object[], orders: object[], encryptedCards: number, cardCount: number, hasNextPage: boolean}}
   */
  function parseOrderList(doc, opts = {}) {
    const ctx = makeCtx(opts, "order_list");
    const cards = outermost(allOfFirst(doc, SEL.orderCard).elements);
    const records = [];
    const orders = [];
    let encryptedCards = 0;
    for (const card of cards) {
      if (isEncryptedCard(card)) {
        encryptedCards++;
        continue;
      }
      const order = parseOrderCard(card, ctx);
      if (!order) continue;
      const returns = splitReturns(order, ctx);
      orders.push(order);
      records.push(record("order", order.order_id, order, ctx, card));
      for (const r of returns) records.push(record("return", r.return_key, { ...r, source: ctx.source }, ctx, null));
    }
    return { records, orders, encryptedCards, cardCount: cards.length, hasNextPage: hasNextPage(doc) };
  }

  function parseSubtotals(rootEl, ctx) {
    const out = {};
    const rows = allOfFirst(rootEl, SEL.subtotalRows).elements;
    for (const row of rows) {
      const valueEl = first(row, SEL.subtotalValue) || row.lastElementChild;
      const valueText = textOf(valueEl);
      const money = parseMoney(valueText, ctx.currency);
      if (!money) continue;
      const label = cleanText(textOf(row).replace(valueText, "")).toLowerCase().replace(/:$/, "");
      if (!label) continue;
      for (const [field, patterns] of SUBTOTAL_LABELS) {
        if (patterns.some((re) => re.test(label))) {
          if (field === "promotion" && out.promotion != null) {
            out.promotion = Math.round((out.promotion + money.amount) * 100) / 100;
          } else if (out[field] == null) {
            out[field] = money.amount;
          }
          out.currency = out.currency || money.currency;
          break;
        }
      }
    }
    return out;
  }

  function parseAddress(rootEl) {
    const el = first(rootEl, SEL.address);
    if (!el) return { ship_to_name: null, ship_to_address: null };
    const nameEl = first(el, SEL.addressName);
    let lines = qsa(el, "li").map(textOf).filter(Boolean);
    if (!lines.length) lines = textBearers(el).map(textOf).filter(Boolean);
    lines = lines.filter((l) => !/^(ship(ping)? to|deliver(y|ing)? to|shipping address)\s*:?$/i.test(l));
    const name = nameEl ? textOf(nameEl) : lines[0] || null;
    const rest = lines.filter((l) => l !== name);
    return { ship_to_name: name || null, ship_to_address: rest.length ? rest.join(", ") : null };
  }

  function parsePayment(rootEl) {
    const nameEl = first(rootEl, SEL.paymentName);
    let method = null;
    if (nameEl) method = nameEl.tagName === "IMG" ? cleanText(nameEl.getAttribute("alt")) : textOf(nameEl);
    let last4 = null;
    // Look for the card digits next to the payment method, not anywhere on the page.
    const area = (nameEl && nameEl.closest("[data-component='paymentMethod'], [data-testid='payment-instrument'], .a-box, .a-row")) || rootEl;
    const numText = textOf(first(rootEl, SEL.paymentNumber)) ||
      findLine(area, /(?:ending in|\*{2,}|•{2,})\s*\d{4}\b/i) || "";
    const m = numText.match(/(?:ending in\s*|•+\s*|\*+\s*)?(\d{4})\b/i);
    if (m) last4 = m[1];
    return { payment_method: method || null, payment_last4: last4 };
  }

  function parseOrderDetail(doc, opts = {}) {
    const ctx = makeCtx(opts, "order_detail");
    const rootEl = first(doc, SEL.detailRoot) || doc.body || doc;
    let orderId = null;
    try {
      orderId = extractOrderId(new URL(ctx.url).searchParams.get("orderID") || "");
    } catch {
      /* ignore */
    }
    orderId = orderId || extractOrderId(textOf(first(rootEl, SEL.orderId))) || extractOrderId(textOf(rootEl));
    if (!orderId) return { records: [], order: null };

    const dateText = textOf(first(rootEl, SEL.detailOrderDate)) || findLine(rootEl, /ordered on|order placed/i);
    const subtotals = parseSubtotals(rootEl, ctx);
    const orderStatus = textOf(first(rootEl, [".od-status-message", "[data-component='orderStatus']"])) || null;
    const items = parseItemsWithShipments(rootEl, ctx, orderStatus);
    const firstStatus = orderStatus || (items.find((i) => i.delivery_status) || {}).delivery_status || null;

    const order = {
      order_id: orderId,
      order_date: parseDate(dateText),
      status: firstStatus,
      is_cancelled: /\bcancell?ed\b/i.test(firstStatus || "") || null,
      is_digital: orderId.startsWith("D") || null,
      subtotal: subtotals.subtotal ?? null,
      shipping: subtotals.shipping ?? null,
      promotion: subtotals.promotion ?? null,
      total_before_tax: subtotals.total_before_tax ?? null,
      tax: subtotals.tax ?? null,
      grand_total: subtotals.grand_total ?? null,
      refund_total: subtotals.refund_total ?? null,
      gift_card: subtotals.gift_card ?? null,
      total_amount: subtotals.grand_total ?? null,
      currency: subtotals.currency || ctx.currency || null,
      ...parsePayment(rootEl),
      ...parseAddress(rootEl),
      detail_url: ctx.url,
      invoice_url: (() => {
        const el = first(rootEl, SEL.invoiceLink);
        return el ? absUrl(el.getAttribute("href"), ctx.url) : null;
      })(),
      items,
      item_count: items.length,
      has_detail: true,
    };
    const returns = splitReturns(order, ctx);
    if (order.refund_total && !returns.length) {
      returns.push({
        return_key: `${orderId}:order`,
        order_id: orderId,
        item_key: null,
        asin: null,
        title: null,
        status: "refunded",
        status_text: "Refund total on order summary",
        event_date: null,
        refund_amount: order.refund_total,
        currency: order.currency,
      });
    }
    const records = [record("order", orderId, order, ctx, rootEl)];
    for (const r of returns) records.push(record("return", r.return_key, { ...r, source: ctx.source }, ctx, null));
    return { records, order };
  }

  /**
   * Heuristic parser for the returns centre: any card mentioning an order ID plus return/refund
   * wording becomes a return record. Layout of this page is the least stable of the three.
   */
  function parseReturnsPage(doc, opts = {}) {
    const ctx = makeCtx(opts, "returns_page");
    const body = doc.body || doc;
    const cards = [];
    for (const el of textBearers(body)) {
      if (!extractOrderId(textOf(el))) continue;
      let node = el;
      for (let depth = 0; depth < 10 && node.parentElement && node.parentElement !== body; depth++) {
        if (/return|refund/i.test(textOf(node)) && qsa(node, SEL.productLink).length) break;
        node = node.parentElement;
      }
      if (/return|refund/i.test(textOf(node)) && !cards.includes(node)) cards.push(node);
    }
    const records = [];
    for (const card of outermost(cards)) {
      const orderId = extractOrderId(textOf(card));
      const lines = textBearers(card).map(textOf).filter(Boolean);
      const info = returnInfo(lines) || { status: "unknown", status_text: lines.find((l) => /return|refund/i.test(l)) || null };
      const refundLine = lines.find((l) => /refund/i.test(l) && parseMoney(l));
      const refund = refundLine ? parseMoney(refundLine, ctx.currency) : null;
      const containers = itemContainersFromLinks(card);
      const items = containers.length ? containers.map((c) => parseItem(c, ctx, {})).filter(Boolean) : [null];
      for (const item of items) {
        const itemKey = item ? item.asin || `t-${hashString(item.title || "")}` : null;
        const data = {
          return_key: `${orderId}:${itemKey || "order"}`,
          order_id: orderId,
          item_key: itemKey,
          asin: item ? item.asin : null,
          title: item ? item.title : null,
          status: info.status,
          status_text: info.status_text,
          event_date: info.event_date || parseDate(lines.find((l) => parseDate(l)) || ""),
          refund_amount: info.refund_amount ?? (refund ? refund.amount : null),
          currency: info.currency || (refund && refund.currency) || ctx.currency || null,
          source: ctx.source,
        };
        records.push(record("return", data.return_key, data, ctx, card));
      }
    }
    return { records };
  }

  function hasNextPage(doc) {
    return Boolean(first(doc, SEL.nextPage[0]));
  }

  /** Time filter values offered by the history page's dropdown, e.g. ["months-3", "year-2025", ...]. */
  function discoverTimeFilters(doc) {
    const values = [];
    for (const opt of allOfFirst(doc, SEL.timeFilterOptions).elements) {
      const v = opt.getAttribute("value");
      if (v && !values.includes(v)) values.push(v);
    }
    return values;
  }

  return {
    SEL, parseOrderList, parseOrderDetail, parseReturnsPage, detectBlocked, hasNextPage, discoverTimeFilters,
    _internal: { labeledValue, returnInfo, itemReturnWindow, parseSubtotals, findItemContainers, textBearers },
  };
});
