/*
 * Background context (Chrome: MV3 service worker via sw.js; Safari: non-persistent background
 * page). Owns the outbox cache, server delivery, crawl coordination and the toolbar badge.
 * Shared modules (AOS.util, AOS.constants, AOS.outbox, AOS.client, AOS.plan) are loaded first.
 */
/* global AOS */
(function () {
  "use strict";

  const { api, onMessage, browserName, util, constants, plan } = AOS;
  const { Outbox } = AOS.outbox;
  const { ServerClient, flush } = AOS.client;

  const outbox = new Outbox({ maxAttempts: constants.LIMITS.maxAttemptsBeforeDeadLetter });
  const FLUSH_ALARM = "aos-flush";
  const VERSION = api.runtime.getManifest().version;

  // --- persisted state -------------------------------------------------------------------

  async function getLocal(key, fallback) {
    const v = await api.storage.local.get(key);
    return v[key] === undefined ? fallback : v[key];
  }

  async function setLocal(key, value) {
    await api.storage.local.set({ [key]: value });
  }

  async function patchLocal(key, patch) {
    const prev = await getLocal(key, {});
    const next = { ...prev, ...patch };
    await setLocal(key, next);
    return next;
  }

  async function getSettings() {
    return { ...constants.DEFAULT_SETTINGS, ...(await getLocal("settings", {})) };
  }

  async function getInstallId() {
    let id = await getLocal("installId", null);
    if (!id) {
      id = util.uuid().slice(0, 8);
      await setLocal("installId", id);
    }
    return id;
  }

  async function makeClient(overrides) {
    const s = { ...(await getSettings()), ...(overrides || {}) };
    return new ServerClient({
      baseUrl: s.serverUrl,
      token: s.token,
      clientInfo: { browser: browserName, extension_version: VERSION, install_id: await getInstallId() },
    });
  }

  // --- badge -----------------------------------------------------------------------------

  async function updateBadge() {
    try {
      const { outbox: queued, deadletter } = await outbox.counts();
      const status = await getLocal("status", {});
      const text = queued > 0 ? (queued > 999 ? "999+" : String(queued)) : deadletter > 0 ? "!" : "";
      await api.action.setBadgeText({ text });
      const color = deadletter > 0 || status.lastErrorKind === "auth" ? "#b3261e"
        : status.serverReachable === false ? "#c77700" : "#2e7d32";
      await api.action.setBadgeBackgroundColor({ color });
    } catch {
      /* badge is cosmetic */
    }
  }

  // --- delivery --------------------------------------------------------------------------

  let flushing = null;
  let flushTimer = null;

  function scheduleFlush(delayMs = 3000) {
    clearTimeout(flushTimer);
    flushTimer = setTimeout(() => flushNow().catch(() => {}), delayMs);
  }

  async function flushNow() {
    if (flushing) return flushing;
    flushing = (async () => {
      const client = await makeClient();
      if (!client.configured) {
        await patchLocal("status", { lastErrorKind: "unconfigured", lastError: "Set the server URL in Settings" });
        return { sent: 0, remaining: (await outbox.counts()).outbox, error: { kind: "unconfigured" } };
      }
      const result = await flush(outbox, client, { installId: await getInstallId() });
      const now = util.nowIso();
      const patch = { lastFlushAttemptAt: now, remaining: result.remaining };
      if (result.error) {
        Object.assign(patch, {
          lastError: result.error.message,
          lastErrorKind: result.error.kind,
          serverReachable: result.error.kind === "offline" ? false : true,
        });
      } else {
        Object.assign(patch, { lastError: null, lastErrorKind: null, serverReachable: true, lastFlushOkAt: now });
        if (result.sent) patch.lastSentCount = result.sent;
      }
      await patchLocal("status", patch);
      await updateBadge();
      return { sent: result.sent, remaining: result.remaining, error: result.error && { kind: result.error.kind, message: result.error.message } };
    })();
    try {
      return await flushing;
    } finally {
      flushing = null;
    }
  }

  // --- observations from content scripts -------------------------------------------------

  async function handleObservations(records) {
    if (!Array.isArray(records) || !records.length) return { queued: 0, unchanged: 0 };
    const result = await outbox.observe(records);
    const now = util.nowIso();
    const orderRecords = records.filter((r) => r.type === "order");
    if (orderRecords.length) {
      const states = await outbox.getOrderStates(orderRecords.map((r) => r.key));
      await outbox.updateOrderStates(orderRecords.map((r) => {
        const prev = states[r.key] || {};
        if (r.source === "order_detail") {
          return { order_id: r.key, detail_fetched_at: now, list_status_at_detail: prev.last_list_status ?? r.data.status ?? null };
        }
        return { order_id: r.key, last_list_status: r.data.status ?? null, order_date: r.data.order_date ?? null, list_seen_at: now };
      }));
    }
    if (result.queued) scheduleFlush();
    await updateBadge();
    return result;
  }

  async function planDetails(orders) {
    const settings = await getSettings();
    const states = await outbox.getOrderStates(orders.map((o) => o.order_id));
    return orders.filter((o) => plan.needsDetail(o, states[o.order_id], settings)).map((o) => o.order_id);
  }

  /** Content scripts only need crawl knobs — never hand them the server token. */
  function contentSettings(s) {
    const { serverUrl, token, ...rest } = s;
    return rest;
  }

  // --- crawl coordination ----------------------------------------------------------------

  async function startCrawl(job) {
    const crawl = await getLocal("crawl", {});
    if (plan.crawlInProgress(crawl)) return { ok: false, error: "A crawl is already running" };
    const amazonTabs = await api.tabs.query({ url: "https://www.amazon.com/*" });
    for (const tab of amazonTabs) {
      try {
        const res = await api.tabs.sendMessage(tab.id, { type: "crawl:run", job });
        if (res && res.ok) {
          await patchLocal("crawl", { running: true, tabId: tab.id, job, heartbeatAt: util.nowIso(), progress: null, lastError: null });
          return { ok: true, tabId: tab.id };
        }
      } catch {
        /* content script not present in that tab (opened before install) — try the next */
      }
    }
    const tab = await api.tabs.create({ url: `https://www.amazon.com${constants.ROUTES.orderHistory}`, active: false });
    await patchLocal("crawl", { pending: { tabId: tab.id, job }, lastError: null });
    return { ok: true, tabId: tab.id, opened: true };
  }

  async function onContentHello(msg, sender) {
    const tabId = sender.tab && sender.tab.id;
    if (msg.pageType === "signIn") return { job: null };
    const crawl = await getLocal("crawl", {});
    if (crawl.pending && crawl.pending.tabId === tabId) {
      await patchLocal("crawl", { pending: null, running: true, tabId, job: crawl.pending.job, heartbeatAt: util.nowIso() });
      return { job: crawl.pending.job };
    }
    const settings = await getSettings();
    if (plan.autoCrawlDue(crawl, settings)) {
      const job = { mode: "recent", auto: true };
      await patchLocal("crawl", { running: true, tabId, job, heartbeatAt: util.nowIso(), lastAutoAttemptAt: util.nowIso() });
      return { job };
    }
    return { job: null };
  }

  async function onCrawlFinished(msg) {
    const patch = { running: false, heartbeatAt: util.nowIso() };
    if (msg.error) {
      patch.lastError = msg.error;
      patch.lastErrorReason = msg.reason || null;
    } else {
      patch.lastError = null;
      patch.lastCrawlFinishedAt = util.nowIso();
      patch.lastResult = msg.result;
    }
    await patchLocal("crawl", patch);
    scheduleFlush(500);
  }

  // --- status for popup/options ----------------------------------------------------------

  async function statusSnapshot() {
    const [counts, status, crawl, settings] = await Promise.all([
      outbox.counts(), getLocal("status", {}), getLocal("crawl", {}), getSettings(),
    ]);
    return {
      ok: true,
      version: VERSION,
      browser: browserName,
      configured: Boolean(settings.serverUrl),
      serverUrl: settings.serverUrl,
      counts,
      status,
      crawl: { ...crawl, inProgress: plan.crawlInProgress(crawl) },
    };
  }

  // --- message router --------------------------------------------------------------------

  onMessage(async (msg, sender) => {
    switch (msg && msg.type) {
      case "observations":
        return { ok: true, ...(await handleObservations(msg.records)) };
      case "crawl:plan":
        return { ok: true, orderIds: await planDetails(msg.orders || []) };
      case "crawl:progress":
        await patchLocal("crawl", { running: true, progress: msg.progress, heartbeatAt: util.nowIso() });
        return { ok: true };
      case "crawl:finished":
        await onCrawlFinished(msg);
        return { ok: true };
      case "content:hello":
        return { ok: true, ...(await onContentHello(msg, sender)), settings: contentSettings(await getSettings()) };
      case "ui:status":
        return statusSnapshot();
      case "ui:flush":
        return { ok: true, ...(await flushNow()) };
      case "ui:crawl":
        return startCrawl({ mode: msg.mode || "recent", year: msg.year });
      case "ui:stopCrawl": {
        const crawl = await getLocal("crawl", {});
        if (crawl.tabId != null) {
          try {
            await api.tabs.sendMessage(crawl.tabId, { type: "crawl:stop" });
          } catch {
            /* tab gone */
          }
        }
        await patchLocal("crawl", { running: false, pending: null });
        return { ok: true };
      }
      case "ui:export":
        return { ok: true, data: await outbox.exportAll() };
      case "ui:requeueDead": {
        const n = await outbox.requeueDeadLetters();
        scheduleFlush(200);
        return { ok: true, requeued: n };
      }
      case "ui:resetSyncState":
        await outbox.resetSyncState();
        return { ok: true };
      case "ui:testConnection": {
        const client = await makeClient({ serverUrl: msg.serverUrl, token: msg.token });
        try {
          const health = await client.health();
          const stats = await client.stats();
          return { ok: true, health, stats };
        } catch (err) {
          return { ok: false, kind: err.kind, error: err.message };
        }
      }
      case "ui:settingsChanged":
        scheduleFlush(200);
        return { ok: true };
      default:
        return { ok: false, error: `unknown message ${msg && msg.type}` };
    }
  });

  // --- lifecycle -------------------------------------------------------------------------

  async function ensureAlarm() {
    // Re-creating an existing alarm resets its schedule, and the worker wakes often.
    const existing = await api.alarms.get(FLUSH_ALARM);
    if (!existing) api.alarms.create(FLUSH_ALARM, { periodInMinutes: constants.LIMITS.flushAlarmMinutes });
  }

  api.runtime.onInstalled.addListener(() => {
    ensureAlarm();
    getInstallId();
    updateBadge();
  });
  if (api.runtime.onStartup) api.runtime.onStartup.addListener(ensureAlarm);

  api.alarms.onAlarm.addListener(async (alarm) => {
    if (alarm.name !== FLUSH_ALARM) return;
    const { outbox: queued } = await outbox.counts();
    if (queued > 0) await flushNow();
  });

  api.tabs.onRemoved.addListener(async (tabId) => {
    const crawl = await getLocal("crawl", {});
    if (crawl.running && crawl.tabId === tabId) {
      await patchLocal("crawl", { running: false, lastError: "Crawl tab was closed" });
    }
  });

  // Network came back (laptop rejoined the tailnet): try delivering right away.
  if (typeof self !== "undefined" && self.addEventListener) {
    self.addEventListener("online", () => scheduleFlush(1000));
  }

  ensureAlarm();
  updateBadge();
})();
