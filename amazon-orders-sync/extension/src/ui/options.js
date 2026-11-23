/* global AOS */
(function () {
  "use strict";
  const { api, send, constants } = AOS;
  const $ = (id) => document.getElementById(id);
  const NUMERIC = ["autoCrawlHours", "requestDelayMs", "refreshDetailDays"];

  function setMsg(id, text, isErr) {
    $(id).textContent = text || "";
    $(id).className = isErr ? "msg err" : "msg";
  }

  async function load() {
    const { settings = {} } = await api.storage.local.get("settings");
    const s = { ...constants.DEFAULT_SETTINGS, ...settings };
    $("serverUrl").value = s.serverUrl;
    $("token").value = s.token;
    for (const k of NUMERIC) $(k).value = s[k];
    $("captureRawHtml").checked = Boolean(s.captureRawHtml);
  }

  function readForm() {
    const s = {
      serverUrl: $("serverUrl").value.trim().replace(/\/+$/, ""),
      token: $("token").value.trim(),
      captureRawHtml: $("captureRawHtml").checked,
    };
    for (const k of NUMERIC) {
      const n = Number($(k).value);
      if (Number.isFinite(n)) s[k] = n;
    }
    return s;
  }

  /**
   * Ask for host permission on the server origin. The server also answers CORS, so this is
   * belt-and-braces for browsers that gate private-network requests from extensions.
   */
  async function requestOriginPermission(serverUrl) {
    if (!serverUrl || !api.permissions || !api.permissions.request) return true;
    try {
      // Must be the first await in the click handler: request() needs the user gesture.
      // It resolves true straight away when the origin is already granted.
      return await api.permissions.request({ origins: [`${new URL(serverUrl).origin}/*`] });
    } catch {
      return false;
    }
  }

  $("save").addEventListener("click", async () => {
    const next = readForm();
    if (next.serverUrl && !/^https?:\/\//.test(next.serverUrl)) {
      setMsg("msg", "Server URL must start with http:// or https://", true);
      return;
    }
    await requestOriginPermission(next.serverUrl);
    const { settings = {} } = await api.storage.local.get("settings");
    await api.storage.local.set({ settings: { ...settings, ...next } });
    await send({ type: "ui:settingsChanged" });
    setMsg("msg", "Saved.");
  });

  $("test").addEventListener("click", async () => {
    const { serverUrl, token } = readForm();
    await requestOriginPermission(serverUrl);
    setMsg("msg", "Testing…");
    const r = await send({ type: "ui:testConnection", serverUrl, token });
    if (r.ok) {
      const t = r.stats && r.stats.tables ? r.stats.tables : {};
      setMsg("msg", `Connected to ${r.health.service} ${r.health.version}. Server has ${t.orders ?? "?"} orders, ${t.order_items ?? "?"} items, ${t.returns ?? "?"} returns.`);
    } else {
      setMsg("msg", r.kind === "offline"
        ? `Can't reach ${serverUrl}. Is Tailscale connected and the server running?`
        : r.error, true);
    }
  });

  $("reset").addEventListener("click", async () => {
    await send({ type: "ui:resetSyncState" });
    setMsg("msg2", "Done. The next sync re-reads order details and re-sends every record.");
  });

  load();
})();
