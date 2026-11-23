/*
 * Shared helpers: text/money/date parsing, hashing, ids.
 *
 * Every file in src/shared uses the same tiny module pattern so it can be:
 *   - listed as a classic script in a manifest (content scripts, Safari background page),
 *   - pulled in with importScripts() (Chrome service worker),
 *   - require()d by node tests.
 * Each module registers itself on globalThis.AOS.<name>.
 */
(function (root, factory) {
  const mod = factory(root);
  root.AOS = Object.assign(root.AOS || {}, { util: mod });
  if (typeof module === "object" && module.exports) module.exports = mod;
})(typeof globalThis !== "undefined" ? globalThis : self, function () {
  "use strict";

  const MONTHS = {
    jan: 1, january: 1, feb: 2, february: 2, mar: 3, march: 3, apr: 4, april: 4,
    may: 5, jun: 6, june: 6, jul: 7, july: 7, aug: 8, august: 8,
    sep: 9, sept: 9, september: 9, oct: 10, october: 10, nov: 11, november: 11,
    dec: 12, december: 12,
  };

  const CURRENCY_SYMBOLS = [
    ["US$", "USD"], ["CA$", "CAD"], ["CDN$", "CAD"], ["A$", "AUD"], ["AU$", "AUD"],
    ["£", "GBP"], ["€", "EUR"], ["₹", "INR"], ["¥", "JPY"], ["$", "USD"],
  ];

  const ORDER_ID_RE = /\b([0-9A-Z]{3}-\d{7}-\d{7})\b/;
  const ASIN_RE = /\/(?:dp|gp\/product|product|gp\/aw\/d)\/([A-Z0-9]{10})(?:[/?#]|$)/;

  function cleanText(value) {
    if (value == null) return "";
    return String(value).replace(/\s+/g, " ").trim();
  }

  function textOf(el) {
    return el ? cleanText(el.textContent) : "";
  }

  /** Parse "$1,234.56", "-$5.00", "USD 12.00", "£3.50", "€1.234,56" → {amount, currency} or null. */
  function parseMoney(value, defaultCurrency) {
    const text = cleanText(value);
    if (!text) return null;
    let currency = null;
    for (const [sym, code] of CURRENCY_SYMBOLS) {
      if (text.includes(sym)) { currency = code; break; }
    }
    const iso = text.match(/\b(USD|CAD|GBP|EUR|AUD|INR|JPY|MXN)\b/);
    if (iso) currency = iso[1];
    const m = text.match(/(-|−)?\s*(?:[A-Z]{2,3}\$?|[$£€₹¥])?\s*(-|−)?\s*(\d[\d.,]*)/);
    if (!m) return null;
    let num = m[3];
    // European style "1.234,56" or "12,50": comma is the decimal separator.
    if (/,\d{2}$/.test(num) && !/\.\d{2}$/.test(num)) {
      num = num.replace(/\./g, "").replace(",", ".");
    } else {
      num = num.replace(/,/g, "");
    }
    let amount = Number.parseFloat(num);
    if (!Number.isFinite(amount)) return null;
    if (m[1] || m[2]) amount = -amount;
    amount = Math.round(amount * 100) / 100;
    return { amount, currency: currency || defaultCurrency || null };
  }

  function pad2(n) {
    return String(n).padStart(2, "0");
  }

  function isoDate(y, m, d) {
    if (!y || !m || !d || m < 1 || m > 12 || d < 1 || d > 31) return null;
    return `${y}-${pad2(m)}-${pad2(d)}`;
  }

  /**
   * Find the first calendar date in free text. Handles "November 3, 2025", "Nov 3 2025",
   * "3 November 2025", "2025-11-03". Returns "YYYY-MM-DD" or null. Year-less dates
   * ("Delivered Nov 3") are resolved against `referenceDate` (defaults to today) and
   * assumed to be in the past.
   */
  function parseDate(value, referenceDate) {
    const text = cleanText(value);
    if (!text) return null;
    let m = text.match(/\b(\d{4})-(\d{2})-(\d{2})\b/);
    if (m) return isoDate(+m[1], +m[2], +m[3]);
    m = text.match(/\b([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b/);
    if (m && MONTHS[m[1].toLowerCase()]) return isoDate(+m[3], MONTHS[m[1].toLowerCase()], +m[2]);
    m = text.match(/\b(\d{1,2})\s+([A-Za-z]{3,9})\.?,?\s+(\d{4})\b/);
    if (m && MONTHS[m[2].toLowerCase()]) return isoDate(+m[3], MONTHS[m[2].toLowerCase()], +m[1]);
    m = text.match(/\b([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:st|nd|rd|th)?\b/);
    if (m && MONTHS[m[1].toLowerCase()]) {
      const ref = referenceDate ? new Date(referenceDate) : new Date();
      const month = MONTHS[m[1].toLowerCase()];
      let year = ref.getUTCFullYear();
      const candidate = Date.UTC(year, month - 1, +m[2]);
      if (candidate > ref.getTime() + 7 * 86400000) year -= 1;
      return isoDate(year, month, +m[2]);
    }
    return null;
  }

  function extractOrderId(value) {
    const m = cleanText(value).match(ORDER_ID_RE);
    return m ? m[1] : null;
  }

  function extractAsin(href) {
    if (!href) return null;
    const m = String(href).match(ASIN_RE);
    return m ? m[1] : null;
  }

  function parseIntSafe(value) {
    const m = cleanText(value).match(/\d+/);
    return m ? Number.parseInt(m[0], 10) : null;
  }

  /** JSON.stringify with sorted keys so equal objects always hash equally. */
  function stableStringify(value) {
    if (value === null || typeof value !== "object") return JSON.stringify(value);
    if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
    const keys = Object.keys(value).filter((k) => value[k] !== undefined).sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(",")}}`;
  }

  /** cyrb53: fast, sync, 53-bit string hash. Used for change detection, not security. */
  function hashString(str, seed = 0) {
    let h1 = 0xdeadbeef ^ seed;
    let h2 = 0x41c6ce57 ^ seed;
    for (let i = 0; i < str.length; i++) {
      const ch = str.charCodeAt(i);
      h1 = Math.imul(h1 ^ ch, 2654435761);
      h2 = Math.imul(h2 ^ ch, 1597334677);
    }
    h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
    h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
    return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(36);
  }

  function hashObject(obj) {
    return hashString(stableStringify(obj));
  }

  function uuid() {
    if (globalThis.crypto && typeof globalThis.crypto.randomUUID === "function") {
      return globalThis.crypto.randomUUID();
    }
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
      const r = (Math.random() * 16) | 0;
      return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
    });
  }

  function sleep(ms, signal) {
    return new Promise((resolve, reject) => {
      const t = setTimeout(resolve, ms);
      if (signal) {
        signal.addEventListener("abort", () => {
          clearTimeout(t);
          reject(new DOMException("Aborted", "AbortError"));
        }, { once: true });
      }
    });
  }

  function jitter(baseMs, spreadMs) {
    return baseMs + Math.floor(Math.random() * spreadMs);
  }

  function nowIso() {
    return new Date().toISOString();
  }

  function daysBetween(isoA, isoB) {
    return Math.abs(Date.parse(isoA) - Date.parse(isoB)) / 86400000;
  }

  return {
    cleanText, textOf, parseMoney, parseDate, extractOrderId, extractAsin, parseIntSafe,
    stableStringify, hashString, hashObject, uuid, sleep, jitter, nowIso, daysBetween,
    ORDER_ID_RE, ASIN_RE,
  };
});
