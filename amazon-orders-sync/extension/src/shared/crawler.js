/*
 * Order-history crawler. Runs inside an amazon.com content script so requests are same-origin
 * and carry the user's session cookies in both Chrome and Safari (an MV3 service worker has
 * no DOMParser, and Safari's background context does not reliably send Amazon cookies).
 *
 * All I/O is injected (fetch, HTML parsing, delivery to background, detail planning), which
 * keeps this file browser-agnostic and testable in node.
 */
(function (root, factory) {
  const g = root.AOS || {};
  const req = (p) => (typeof require === "function" ? require(p) : null);
  const mod = factory(g.util || req("./util.js"), g.constants || req("./constants.js"), g.parsers || req("./parsers.js"));
  root.AOS = Object.assign(root.AOS || {}, { crawler: mod });
  if (typeof module === "object" && module.exports) module.exports = mod;
})(typeof globalThis !== "undefined" ? globalThis : self, function (util, constants, parsers) {
  "use strict";

  const PAGE_SIZE = 10;
  const MAX_PAGES_PER_FILTER = 200;

  class CrawlBlocked extends Error {
    constructor(reason) {
      super(reason === "signin" ? "Amazon asked to sign in" : "Amazon showed a CAPTCHA");
      this.reason = reason;
    }
  }

  class Crawler {
    /**
     * @param {object} deps
     * @param {string} deps.origin            e.g. "https://www.amazon.com"
     * @param {Function} deps.fetchImpl       fetch(url, init) → Response
     * @param {Function} deps.parseHtml       (html) → Document
     * @param {Function} deps.send            async (records) → void      deliver to background
     * @param {Function} deps.plan            async (orders) → string[]   order ids needing detail
     * @param {Function} [deps.onProgress]    (progress) → void
     * @param {object} deps.settings          DEFAULT_SETTINGS-shaped
     * @param {AbortSignal} [deps.signal]
     */
    constructor(deps) {
      this.d = deps;
      this.requests = 0;
      this.progress = { pages: 0, orders: 0, details: 0, returns: 0, filter: null };
    }

    async _pause() {
      if (this.requests === 0) return;
      const base = this.d.settings.requestDelayMs ?? 2000;
      await util.sleep(util.jitter(base, Math.floor(base * 0.75)), this.d.signal);
    }

    async getDoc(url) {
      if (this.d.signal && this.d.signal.aborted) throw new DOMException("Aborted", "AbortError");
      await this._pause();
      this.requests++;
      const res = await this.d.fetchImpl(url, { credentials: "include", redirect: "follow" });
      const finalUrl = res.url || url;
      const html = await res.text();
      const doc = this.d.parseHtml(html);
      const blocked = parsers.detectBlocked(doc, finalUrl);
      if (blocked) throw new CrawlBlocked(blocked);
      return { doc, status: res.status, ok: res.ok, url: finalUrl };
    }

    _opts(url) {
      return {
        url,
        observedAt: util.nowIso(),
        captureRawHtml: this.d.settings.captureRawHtml,
        rawHtmlMaxChars: constants.LIMITS.rawHtmlMaxChars,
      };
    }

    _report() {
      if (this.d.onProgress) this.d.onProgress({ ...this.progress, requests: this.requests });
    }

    historyUrl(filter, startIndex, noJs) {
      const u = new URL(constants.ROUTES.orderHistory, this.d.origin);
      if (filter) u.searchParams.set("timeFilter", filter);
      if (startIndex) u.searchParams.set("startIndex", String(startIndex));
      if (noJs) u.searchParams.set("disableCsd", "no-js");
      return u.toString();
    }

    async fetchListPage(filter, startIndex) {
      let url = this.historyUrl(filter, startIndex, false);
      let page = await this.getDoc(url);
      let parsed = parsers.parseOrderList(page.doc, this._opts(page.url));
      if (parsed.encryptedCards && parsed.orders.length < parsed.cardCount) {
        // Cards encrypted for client-side decryption: Amazon's no-JS variant is readable.
        url = this.historyUrl(filter, startIndex, true);
        page = await this.getDoc(url);
        parsed = parsers.parseOrderList(page.doc, this._opts(page.url));
      }
      return { ...parsed, doc: page.doc };
    }

    async crawlFilter(filter) {
      this.progress.filter = filter;
      let seen = 0;
      for (let pageNo = 0; pageNo < MAX_PAGES_PER_FILTER; pageNo++) {
        const page = await this.fetchListPage(filter, pageNo * PAGE_SIZE);
        if (!page.cardCount) break;
        await this.d.send(page.records);
        this.progress.pages++;
        this.progress.orders += page.orders.length;
        seen += page.orders.length;
        this._report();

        const wanted = await this.d.plan(
          page.orders.map((o) => ({ order_id: o.order_id, order_date: o.order_date, status: o.status })),
        );
        for (const orderId of wanted) {
          const order = page.orders.find((o) => o.order_id === orderId);
          await this.crawlDetail(orderId, order && order.detail_url);
        }
        if (!page.hasNextPage) break;
      }
      return seen;
    }

    async crawlDetail(orderId, detailUrl) {
      const u = detailUrl && !/wholefoods|fopo/.test(detailUrl)
        ? detailUrl
        : `${this.d.origin}${constants.ROUTES.orderDetails}?orderID=${encodeURIComponent(orderId)}`;
      const page = await this.getDoc(u);
      if (!page.ok) return;
      const parsed = parsers.parseOrderDetail(page.doc, this._opts(page.url));
      if (!parsed.order) return;
      await this.d.send(parsed.records);
      this.progress.details++;
      this.progress.returns += parsed.records.filter((r) => r.type === "return").length;
      this._report();
    }

    async crawlReturns() {
      for (const route of constants.ROUTES.returnsHistory) {
        let page;
        try {
          page = await this.getDoc(new URL(route, this.d.origin).toString());
        } catch (err) {
          if (err instanceof CrawlBlocked || (err && err.name === "AbortError")) throw err;
          continue;
        }
        if (!page.ok || constants.pageType(new URL(page.url).pathname) !== "returns") continue;
        const parsed = parsers.parseReturnsPage(page.doc, this._opts(page.url));
        if (parsed.records.length) await this.d.send(parsed.records);
        this.progress.returns += parsed.records.length;
        this._report();
        return parsed.records.length;
      }
      return 0;
    }

    /** Time filters for a full backfill: the dropdown's year-* options, or a descending fallback. */
    async discoverYearFilters() {
      const page = await this.getDoc(this.historyUrl(null, 0, false));
      const years = parsers.discoverTimeFilters(page.doc).filter((v) => /^year-\d{4}$/.test(v));
      return years.length ? years : null;
    }

    /**
     * @param {{mode: "recent"|"full"|"year", year?: number}} job
     */
    async run(job) {
      const startedAt = util.nowIso();
      if (job.mode === "full") {
        const years = await this.discoverYearFilters();
        if (years) {
          for (const f of years) await this.crawlFilter(f);
        } else {
          // Dropdown not found: walk back year by year until 3 consecutive empty years.
          let empty = 0;
          for (let y = new Date().getFullYear(); y >= 1995 && empty < 3; y--) {
            const n = await this.crawlFilter(`year-${y}`);
            empty = n ? 0 : empty + 1;
          }
        }
      } else if (job.mode === "year") {
        await this.crawlFilter(`year-${job.year}`);
      } else {
        await this.crawlFilter(this.d.settings.recentTimeFilter || "months-3");
      }
      await this.crawlReturns();
      return { ...this.progress, requests: this.requests, startedAt, finishedAt: util.nowIso() };
    }
  }

  return { Crawler, CrawlBlocked, PAGE_SIZE };
});
