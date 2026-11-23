#!/usr/bin/env node
// End-to-end check of the real Chrome build against the real ingest server.
//
//   npm run build:chrome && node test/e2e/chrome-e2e.mjs
//
// - starts ../server (uv run) on a free port with throwaway data dirs
// - loads dist/chrome into Playwright's Chromium
// - serves test/fixtures as https://www.amazon.com via request routing (no real Amazon traffic)
// - phase 1: server URL points at a dead port → records must be cached, not lost
// - phase 2: crawl while still offline → more records cached
// - phase 3: point at the live server, flush → server tables populated, cache empty
//
// Needs: node, uv, and the pre-installed Playwright Chromium. Not part of `npm test`.

import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const HERE = path.dirname(fileURLToPath(import.meta.url));
const EXT = path.join(HERE, "..", "..", "dist", "chrome");
const FIXTURES = path.join(HERE, "..", "fixtures");
const SERVER_DIR = path.join(HERE, "..", "..", "..", "server");
const TOKEN = "e2e-token";

const fixture = (name) => readFileSync(path.join(FIXTURES, name), "utf8");
const ROUTES = [
  [/^\/your-orders\/orders\?.*startIndex=10/, "order-list-last-page.html"],
  [/^\/your-orders\/orders/, "order-list.html"],
  [/^\/your-orders\/order-details\?orderID=112-1234567-7654321/, "order-detail.html"],
  [/^\/your-orders\/order-details\?orderID=113-7777777-1111111/, "order-detail-legacy-refund.html"],
  [/^\/spr\/returns\/history/, "returns.html"],
];

function freePort() {
  return new Promise((resolve) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

async function waitFor(fn, label, timeoutMs = 20000) {
  const start = Date.now();
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${label}`);
    await new Promise((r) => setTimeout(r, 300));
  }
}

function check(cond, message) {
  if (!cond) throw new Error(`FAIL: ${message}`);
  console.log(`  ok - ${message}`);
}

const tmp = mkdtempSync(path.join(os.tmpdir(), "aos-e2e-"));
const port = await freePort();
const deadPort = await freePort();
const server = spawn(
  "uv",
  ["run", "--quiet", "python", "amazon_orders_server.py", "serve", "--port", String(port), "--export-delay", "0",
    "--db", path.join(tmp, "amazon.duckdb"), "--raw-dir", path.join(tmp, "raw"), "--curated-dir", path.join(tmp, "curated")],
  { cwd: SERVER_DIR, env: { ...process.env, AMAZON_SYNC_TOKEN: TOKEN }, stdio: ["ignore", "inherit", "inherit"] },
);
const base = `http://127.0.0.1:${port}`;
const api = async (p) => (await fetch(`${base}${p}`, { headers: { Authorization: `Bearer ${TOKEN}` } })).json();

let context;
try {
  await waitFor(async () => fetch(`${base}/v1/health`).then((r) => r.ok).catch(() => false), "server");

  context = await chromium.launchPersistentContext(path.join(tmp, "profile"), {
    channel: "chromium",
    headless: true,
    args: [`--disable-extensions-except=${EXT}`, `--load-extension=${EXT}`],
  });
  await context.route("https://www.amazon.com/**", async (route) => {
    const u = new URL(route.request().url());
    const key = u.pathname + u.search;
    const hit = ROUTES.find(([re]) => re.test(key));
    if (!hit) return route.fulfill({ status: 404, body: "<html><body>nope</body></html>", contentType: "text/html" });
    return route.fulfill({ status: 200, body: fixture(hit[1]), contentType: "text/html" });
  });

  const sw = context.serviceWorkers()[0] || (await context.waitForEvent("serviceworker"));
  const extId = new URL(sw.url()).host;
  const ui = await context.newPage();
  await ui.goto(`chrome-extension://${extId}/ui/popup.html`);
  const send = (msg) => ui.evaluate((m) => chrome.runtime.sendMessage(m), msg);
  const setSettings = (s) => ui.evaluate((v) => chrome.storage.local.set({ settings: v }), s);
  const settings = { token: TOKEN, autoCrawlHours: 0, requestDelayMs: 50, captureRawHtml: true };

  console.log("phase 1: passive capture while the server is unreachable");
  await setSettings({ ...settings, serverUrl: `http://127.0.0.1:${deadPort}` });
  const shop = await context.newPage();
  await shop.goto("https://www.amazon.com/your-orders/orders?timeFilter=months-3");
  const s1 = await waitFor(async () => {
    const s = await send({ type: "ui:status" });
    return s.counts.outbox >= 4 ? s : null;
  }, "passive capture");
  check(s1.counts.outbox === 4, `4 records cached from the visible page (got ${s1.counts.outbox})`);
  const f1 = await send({ type: "ui:flush" });
  check(f1.error && f1.error.kind === "offline", "flush reports offline and keeps the cache");
  check((await send({ type: "ui:status" })).counts.outbox === 4, "cache intact after failed flush");

  console.log("phase 2: crawl (still offline)");
  const started = await send({ type: "ui:crawl", mode: "recent" });
  check(started.ok, "crawl started in the open Amazon tab");
  const s2 = await waitFor(async () => {
    const s = await send({ type: "ui:status" });
    return !s.crawl.inProgress && s.crawl.lastCrawlFinishedAt ? s : null;
  }, "crawl to finish", 60000);
  check(!s2.crawl.lastError, `crawl finished without error (${JSON.stringify(s2.crawl.lastResult)})`);
  // 4 new orders → 4 detail requests; only 2 have detail fixtures (the others 404 and are skipped).
  check(s2.crawl.lastResult.details === 2 && s2.crawl.lastResult.requests === 7, "detail pages fetched for every new order");
  check(s2.counts.outbox > 4, `more records cached after crawl (${s2.counts.outbox})`);

  console.log("phase 3: tailnet back — flush to the live server");
  await setSettings({ ...settings, serverUrl: base });
  const f3 = await send({ type: "ui:flush" });
  check(!f3.error && f3.remaining === 0, `flush delivered ${f3.sent} records, nothing left cached`);
  const stats = await api("/v1/stats");
  console.log("  server tables:", JSON.stringify(stats.tables));
  check(stats.tables.orders === 4, "4 orders on the server");
  check(stats.tables.returns >= 2, "returns on the server");
  const order = await api("/v1/orders/112-1234567-7654321");
  check(order.has_detail === true && order.tax === 22.17 && order.items.length === 2, "detail fields merged into the order");
  check(stats.last_batch.client_browser === "chrome", "batch tagged with browser");

  console.log("phase 4: revisit page → unchanged records are not re-sent");
  await shop.reload();
  await new Promise((r) => setTimeout(r, 2500));
  check((await send({ type: "ui:status" })).counts.outbox === 0, "no duplicate records queued");
  console.log("E2E PASSED");
} finally {
  if (context) await context.close();
  server.kill();
  rmSync(tmp, { recursive: true, force: true });
}
