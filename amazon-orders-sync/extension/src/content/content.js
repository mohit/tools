/*
 * Content script on www.amazon.com. Two jobs:
 *   1. Passive capture: when the user is looking at an order list / order detail / returns
 *      page, parse the live (already decrypted) DOM and hand records to the background.
 *   2. Crawl host: when the background asks (button in the popup, or auto-crawl being due),
 *      run the Crawler from this page's origin so requests carry the user's Amazon session.
 */
/* global AOS */
(function () {
  "use strict";
  if (window.top !== window) return;

  const { api, send, util, constants, parsers } = AOS;
  const { Crawler, CrawlBlocked } = AOS.crawler;

  const pageType = constants.pageType(location.pathname);
  let settings = { ...constants.DEFAULT_SETTINGS };
  let activeCrawl = null;

  // --- passive capture -----------------------------------------------------------------

  let lastFingerprint = null;

  function parseCurrentPage() {
    const opts = {
      url: location.href,
      observedAt: util.nowIso(),
      captureRawHtml: settings.captureRawHtml,
      rawHtmlMaxChars: constants.LIMITS.rawHtmlMaxChars,
    };
    if (pageType === "orderList") return parsers.parseOrderList(document, opts).records;
    if (pageType === "orderDetail") return parsers.parseOrderDetail(document, opts).records;
    if (pageType === "returns") return parsers.parseReturnsPage(document, opts).records;
    return [];
  }

  async function capture() {
    const records = parseCurrentPage();
    if (!records.length) return;
    const fp = util.hashObject(records.map((r) => [r.type, r.key, r.data]));
    if (fp === lastFingerprint) return;
    lastFingerprint = fp;
    await send({ type: "observations", records, passive: true });
  }

  function watchForLateRender() {
    // Order cards can be decrypted/rendered after document_idle. Re-parse on DOM changes,
    // debounced, a bounded number of times.
    let runs = 0;
    let timer = null;
    const observer = new MutationObserver(() => {
      clearTimeout(timer);
      timer = setTimeout(() => {
        runs++;
        capture().catch(() => {});
        if (runs >= 5) observer.disconnect();
      }, 1500);
    });
    observer.observe(document.body, { childList: true, subtree: true });
    setTimeout(() => observer.disconnect(), 60000);
  }

  // --- crawl host ------------------------------------------------------------------------

  function toast() {
    let el = document.getElementById("aos-toast");
    if (el) return el;
    el = document.createElement("div");
    el.id = "aos-toast";
    el.style.cssText = [
      "position:fixed", "right:16px", "bottom:16px", "z-index:2147483647", "max-width:320px",
      "background:#232f3e", "color:#fff", "font:13px/1.4 -apple-system,system-ui,sans-serif",
      "padding:10px 12px", "border-radius:8px", "box-shadow:0 4px 16px rgba(0,0,0,.3)",
    ].join(";");
    const text = document.createElement("span");
    text.className = "aos-text";
    const stop = document.createElement("button");
    stop.textContent = "Stop";
    stop.style.cssText = "margin-left:10px;background:#febd69;border:0;border-radius:4px;padding:2px 8px;cursor:pointer";
    stop.addEventListener("click", () => activeCrawl && activeCrawl.abort());
    el.append(text, stop);
    document.documentElement.appendChild(el);
    return el;
  }

  function setToast(message, done) {
    const el = toast();
    el.querySelector(".aos-text").textContent = message;
    el.querySelector("button").style.display = done ? "none" : "";
    if (done) setTimeout(() => el.remove(), 6000);
  }

  async function runCrawl(job) {
    if (activeCrawl) return;
    activeCrawl = new AbortController();
    const label = job.mode === "full" ? "full history" : job.mode === "year" ? String(job.year) : "recent orders";
    if (!job.auto) setToast(`Amazon Orders Sync: syncing ${label}…`);
    const crawler = new Crawler({
      origin: location.origin,
      fetchImpl: (url, init) => fetch(url, init),
      parseHtml: (html) => new DOMParser().parseFromString(html, "text/html"),
      send: async (records) => {
        if (records.length) await send({ type: "observations", records });
      },
      plan: async (orders) => {
        const res = await send({ type: "crawl:plan", orders });
        return (res && res.orderIds) || [];
      },
      onProgress: (progress) => {
        send({ type: "crawl:progress", progress });
        if (!job.auto) setToast(`Syncing ${label}: ${progress.orders} orders, ${progress.details} details, ${progress.returns} returns…`);
      },
      settings,
      signal: activeCrawl.signal,
    });
    try {
      const result = await crawler.run(job);
      await send({ type: "crawl:finished", result });
      if (!job.auto) setToast(`Synced ${result.orders} orders, ${result.details} details, ${result.returns} returns.`, true);
    } catch (err) {
      const aborted = err && err.name === "AbortError";
      const reason = err instanceof CrawlBlocked ? err.reason : aborted ? "aborted" : "error";
      await send({ type: "crawl:finished", error: aborted ? "Stopped" : String(err.message || err), reason });
      if (!job.auto || reason === "signin" || reason === "captcha") {
        setToast(aborted ? "Sync stopped." : `Sync failed: ${err.message || err}`, true);
      }
    } finally {
      activeCrawl = null;
    }
  }

  api.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    if (msg && msg.type === "crawl:run") {
      if (activeCrawl) {
        sendResponse({ ok: false, error: "busy" });
      } else {
        runCrawl(msg.job);
        sendResponse({ ok: true });
      }
    } else if (msg && msg.type === "crawl:stop") {
      if (activeCrawl) activeCrawl.abort();
      sendResponse({ ok: true });
    }
    return false;
  });

  // --- boot ------------------------------------------------------------------------------

  (async () => {
    const hello = await send({ type: "content:hello", pageType, url: location.href });
    if (hello && hello.settings) settings = { ...settings, ...hello.settings };
    if (["orderList", "orderDetail", "returns"].includes(pageType)) {
      await capture().catch(() => {});
      watchForLateRender();
    }
    if (hello && hello.job && pageType !== "signIn") runCrawl(hello.job);
  })();
})();
