/* global AOS */
(function () {
  "use strict";
  const { api, send } = AOS;
  const $ = (id) => document.getElementById(id);

  function ago(iso) {
    if (!iso) return "never";
    const s = Math.round((Date.now() - Date.parse(iso)) / 1000);
    if (s < 60) return "just now";
    if (s < 3600) return `${Math.round(s / 60)} min ago`;
    if (s < 86400) return `${Math.round(s / 3600)} h ago`;
    return `${Math.round(s / 86400)} d ago`;
  }

  function setMsg(text, isErr) {
    $("msg").textContent = text || "";
    $("msg").className = isErr ? "msg err" : "msg";
  }

  function serverLabel(snap) {
    const st = snap.status || {};
    if (!snap.configured) return ["err", "not configured"];
    if (st.lastErrorKind === "auth") return ["err", "token rejected"];
    if (st.serverReachable === false) return ["warn", "unreachable — caching"];
    if (st.serverReachable === true) return ["ok", "connected"];
    return ["", "not contacted yet"];
  }

  async function refresh() {
    const snap = await send({ type: "ui:status" });
    if (!snap || !snap.ok) {
      setMsg("Background not responding", true);
      return;
    }
    const [cls, label] = serverLabel(snap);
    $("server").innerHTML = "";
    const dot = document.createElement("span");
    dot.className = `dot ${cls}`;
    $("server").append(dot, document.createTextNode(label));
    $("server").title = snap.serverUrl || "";
    $("queued").textContent = String(snap.counts.outbox);
    $("dead").textContent = String(snap.counts.deadletter);
    $("dead-row").hidden = !snap.counts.deadletter;
    $("requeue").hidden = !snap.counts.deadletter;
    $("last-flush").textContent = ago(snap.status.lastFlushOkAt);
    const c = snap.crawl || {};
    $("last-crawl").textContent = c.lastError ? `failed: ${c.lastError}` : ago(c.lastCrawlFinishedAt);
    $("progress-row").hidden = !c.inProgress;
    if (c.inProgress && c.progress) {
      $("progress").textContent = `${c.progress.orders} orders · ${c.progress.details} details`;
    }
    $("stop").hidden = !c.inProgress;
    $("crawl-recent").disabled = c.inProgress;
    $("crawl-full").disabled = c.inProgress;
    if (snap.status.lastError && snap.status.lastErrorKind !== "offline") setMsg(snap.status.lastError, true);
  }

  async function act(fn, busyText) {
    setMsg(busyText);
    try {
      await fn();
    } catch (err) {
      setMsg(String(err), true);
    }
    await refresh();
  }

  $("crawl-recent").addEventListener("click", () => act(async () => {
    const r = await send({ type: "ui:crawl", mode: "recent" });
    setMsg(r.ok ? (r.opened ? "Opened an Amazon tab to sync in" : "Sync started") : r.error, !r.ok);
  }, "Starting…"));

  $("crawl-full").addEventListener("click", () => act(async () => {
    const r = await send({ type: "ui:crawl", mode: "full" });
    setMsg(r.ok ? "Backfill started — leave the Amazon tab open" : r.error, !r.ok);
  }, "Starting…"));

  $("stop").addEventListener("click", () => act(() => send({ type: "ui:stopCrawl" }), "Stopping…"));

  $("flush").addEventListener("click", () => act(async () => {
    const r = await send({ type: "ui:flush" });
    if (r.error) setMsg(`${r.sent || 0} sent; ${r.error.message || r.error.kind}`, r.error.kind !== "offline");
    else setMsg(`${r.sent} sent, ${r.remaining} left`);
  }, "Sending…"));

  $("requeue").addEventListener("click", () => act(async () => {
    const r = await send({ type: "ui:requeueDead" });
    setMsg(`${r.requeued} records re-queued`);
  }, "Re-queueing…"));

  $("export").addEventListener("click", () => act(async () => {
    const r = await send({ type: "ui:export" });
    const blob = new Blob([JSON.stringify(r.data, null, 2)], { type: "application/json" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `amazon-orders-cache-${new Date().toISOString().slice(0, 10)}.json`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 10000);
    setMsg(`${r.data.outbox.length} cached records downloaded`);
  }, "Exporting…"));

  $("open-options").addEventListener("click", (e) => {
    e.preventDefault();
    api.runtime.openOptionsPage();
  });

  refresh();
  setInterval(refresh, 2000);
})();
