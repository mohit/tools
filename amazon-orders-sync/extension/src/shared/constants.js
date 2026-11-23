/*
 * Constants shared by background, content scripts and UI: URL routes, page detection,
 * default settings, schema version of the wire format.
 */
(function (root, factory) {
  const mod = factory(root);
  root.AOS = Object.assign(root.AOS || {}, { constants: mod });
  if (typeof module === "object" && module.exports) module.exports = mod;
})(typeof globalThis !== "undefined" ? globalThis : self, function () {
  "use strict";

  const SCHEMA_VERSION = 1;
  // Which website the records come from. The wire format is source-tagged so the same
  // extension/server pair can grow to other sites without a protocol break.
  const SOURCE = "amazon";

  const ROUTES = {
    orderHistory: "/your-orders/orders",
    orderDetails: "/your-orders/order-details",
    legacyOrderDetails: "/gp/your-account/order-details",
    // Best-effort: Amazon has moved the returns centre several times. The crawler tries each
    // and ignores the ones that 404 or redirect elsewhere.
    returnsHistory: ["/spr/returns/history", "/gp/css/returns/homepage.html"],
  };

  const PAGE_PATTERNS = {
    orderList: [/^\/your-orders\/orders/, /^\/gp\/your-account\/order-history/, /^\/gp\/css\/order-history/],
    orderDetail: [/^\/your-orders\/order-details/, /^\/gp\/your-account\/order-details/, /^\/gp\/css\/summary\/edit/],
    returns: [/^\/spr\/returns/, /^\/returns\//, /^\/gp\/css\/returns/, /^\/your-orders\/returns/],
    signIn: [/^\/ap\/signin/, /^\/ap\/mfa/, /^\/ap\/cvf/],
  };

  const DEFAULT_SETTINGS = {
    serverUrl: "",
    token: "",
    // Run an incremental crawl in the background of any open Amazon tab when the last
    // successful crawl is older than this. 0 disables auto-crawl (passive capture still works).
    autoCrawlHours: 24,
    // Window for routine crawls. Amazon's history filter supports "last30" and "months-3".
    recentTimeFilter: "months-3",
    captureRawHtml: true,
    requestDelayMs: 2000,
    // Re-fetch order details for orders younger than this many days so that late returns,
    // refunds and delivery updates are picked up.
    refreshDetailDays: 120,
    refreshDetailEveryDays: 3,
  };

  const LIMITS = {
    batchMaxRecords: 200,
    batchMaxBytes: 2 * 1024 * 1024,
    rawHtmlMaxChars: 200 * 1024,
    healthTimeoutMs: 4000,
    ingestTimeoutMs: 30000,
    maxAttemptsBeforeDeadLetter: 8,
    flushAlarmMinutes: 5,
  };

  function pageType(pathname) {
    for (const [type, patterns] of Object.entries(PAGE_PATTERNS)) {
      if (patterns.some((re) => re.test(pathname))) return type;
    }
    return "other";
  }

  return { SCHEMA_VERSION, SOURCE, ROUTES, PAGE_PATTERNS, DEFAULT_SETTINGS, LIMITS, pageType };
});
