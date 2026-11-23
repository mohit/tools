/*
 * Crawl planning decisions, kept pure so they are unit-testable:
 *   - which orders need their detail page (re)fetched
 *   - whether an automatic background crawl is due
 */
(function (root, factory) {
  const mod = factory();
  root.AOS = Object.assign(root.AOS || {}, { plan: mod });
  if (typeof module === "object" && module.exports) module.exports = mod;
})(typeof globalThis !== "undefined" ? globalThis : self, function () {
  "use strict";

  const DAY = 86400000;

  /**
   * @param {{order_id: string, order_date?: string|null, status?: string|null}} order  as seen on the list page
   * @param {object|undefined} state   bookkeeping from Outbox.orders (detail_fetched_at, detail_status)
   * @param {{refreshDetailDays: number, refreshDetailEveryDays: number}} settings
   * @param {number} now  epoch ms
   */
  function needsDetail(order, state, settings, now = Date.now()) {
    if (!state || !state.detail_fetched_at) return true;
    const fetchedAt = Date.parse(state.detail_fetched_at);
    // Status line changed since we last read the detail page (shipped → delivered → returned).
    if (order.status && state.list_status_at_detail && order.status !== state.list_status_at_detail) return true;
    const orderTime = order.order_date ? Date.parse(order.order_date) : NaN;
    const young = Number.isNaN(orderTime) || now - orderTime < settings.refreshDetailDays * DAY;
    return young && now - fetchedAt > settings.refreshDetailEveryDays * DAY;
  }

  /**
   * @param {{lastCrawlFinishedAt?: string|null, running?: boolean, heartbeatAt?: string|null}} crawlState
   */
  function autoCrawlDue(crawlState, settings, now = Date.now()) {
    if (!settings.autoCrawlHours || settings.autoCrawlHours <= 0) return false;
    if (crawlInProgress(crawlState, now)) return false;
    const last = crawlState.lastCrawlFinishedAt ? Date.parse(crawlState.lastCrawlFinishedAt) : 0;
    const lastAttempt = crawlState.lastAutoAttemptAt ? Date.parse(crawlState.lastAutoAttemptAt) : 0;
    // Don't hammer Amazon if a crawl keeps failing (e.g. signed out): at most one attempt/hour.
    if (now - lastAttempt < 3600000) return false;
    return now - last > settings.autoCrawlHours * 3600000;
  }

  function crawlInProgress(crawlState, now = Date.now()) {
    if (!crawlState || !crawlState.running) return false;
    const hb = crawlState.heartbeatAt ? Date.parse(crawlState.heartbeatAt) : 0;
    return now - hb < 120000;
  }

  return { needsDetail, autoCrawlDue, crawlInProgress };
});
