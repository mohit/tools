/*
 * Cross-browser WebExtension API handle. Safari exposes both `browser` (promise-based) and
 * `chrome`; Chrome MV3 exposes promise-returning `chrome`. Everything else in the codebase
 * goes through AOS.api so there is exactly one place that knows about the difference.
 */
(function (root) {
  const api = root.browser && root.browser.runtime ? root.browser : root.chrome;

  /** Promise wrapper around runtime.sendMessage that never rejects on "no receiver". */
  async function send(message) {
    try {
      return await api.runtime.sendMessage(message);
    } catch (err) {
      return { ok: false, error: String(err && err.message ? err.message : err) };
    }
  }

  /**
   * Register an async onMessage handler. Uses the sendResponse + `return true` form because
   * Chrome does not (reliably) accept a returned Promise from onMessage listeners.
   */
  function onMessage(handler) {
    api.runtime.onMessage.addListener((message, sender, sendResponse) => {
      Promise.resolve()
        .then(() => handler(message, sender))
        .then(
          (result) => sendResponse(result === undefined ? { ok: true } : result),
          (err) => sendResponse({ ok: false, error: String(err && err.message ? err.message : err) }),
        );
      return true;
    });
  }

  const browserName = (() => {
    const ua = (root.navigator && root.navigator.userAgent) || "";
    if (/Safari\//.test(ua) && !/Chrom(e|ium)\//.test(ua)) return "safari";
    if (/Chrom(e|ium)\//.test(ua)) return "chrome";
    return "unknown";
  })();

  root.AOS = Object.assign(root.AOS || {}, { api, send, onMessage, browserName });
})(typeof globalThis !== "undefined" ? globalThis : self);
