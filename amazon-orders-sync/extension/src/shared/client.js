/*
 * HTTP client for the ingest server, plus the flush loop that drains the outbox.
 *
 * Failure classes decide what happens to queued records:
 *   offline     network error / timeout (Tailscale down, server asleep) → keep, retry later
 *   auth        401/403 → keep, surface "check token" in the UI
 *   too_large   413 → halve the batch and retry immediately
 *   rejected    other 4xx → count an attempt; dead-letter after LIMITS.maxAttemptsBeforeDeadLetter
 *   server      5xx → keep, retry later
 */
(function (root, factory) {
  const g = root.AOS || {};
  const util = g.util || (typeof require === "function" ? require("./util.js") : null);
  const constants = g.constants || (typeof require === "function" ? require("./constants.js") : null);
  const mod = factory(util, constants);
  root.AOS = Object.assign(root.AOS || {}, { client: mod });
  if (typeof module === "object" && module.exports) module.exports = mod;
})(typeof globalThis !== "undefined" ? globalThis : self, function (util, constants) {
  "use strict";

  class SyncError extends Error {
    constructor(kind, message, status) {
      super(message);
      this.kind = kind;
      this.status = status || null;
    }
  }

  function normaliseBaseUrl(url) {
    return String(url || "").trim().replace(/\/+$/, "");
  }

  async function gzipBody(text) {
    if (typeof CompressionStream === "undefined") return null;
    try {
      const stream = new Blob([text]).stream().pipeThrough(new CompressionStream("gzip"));
      return await new Response(stream).arrayBuffer();
    } catch {
      return null;
    }
  }

  class ServerClient {
    constructor({ baseUrl, token, fetchImpl, clientInfo } = {}) {
      this.baseUrl = normaliseBaseUrl(baseUrl);
      this.token = token || "";
      this.fetch = fetchImpl || globalThis.fetch.bind(globalThis);
      this.clientInfo = clientInfo || {};
    }

    get configured() {
      return Boolean(this.baseUrl);
    }

    async _request(path, { method = "GET", body, headers = {}, timeoutMs }) {
      if (!this.configured) throw new SyncError("unconfigured", "Server URL is not set");
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      const h = { ...headers };
      if (this.token) h.Authorization = `Bearer ${this.token}`;
      let res;
      try {
        res = await this.fetch(`${this.baseUrl}${path}`, { method, body, headers: h, signal: controller.signal });
      } catch (err) {
        throw new SyncError("offline", `Server unreachable: ${err && err.name === "AbortError" ? "timeout" : err}`);
      } finally {
        clearTimeout(timer);
      }
      if (res.ok) return res.json().catch(() => ({}));
      const text = await res.text().catch(() => "");
      if (res.status === 401 || res.status === 403) throw new SyncError("auth", `Unauthorized (${res.status})`, res.status);
      if (res.status === 413) throw new SyncError("too_large", "Batch too large", 413);
      if (res.status >= 500) throw new SyncError("server", `Server error ${res.status}: ${text.slice(0, 200)}`, res.status);
      throw new SyncError("rejected", `Rejected ${res.status}: ${text.slice(0, 200)}`, res.status);
    }

    health() {
      return this._request("/v1/health", { timeoutMs: constants.LIMITS.healthTimeoutMs });
    }

    stats() {
      return this._request("/v1/stats", { timeoutMs: constants.LIMITS.healthTimeoutMs });
    }

    async ingest(records, batchId) {
      const payload = JSON.stringify({
        schema_version: constants.SCHEMA_VERSION,
        source: constants.SOURCE,
        batch_id: batchId || util.uuid(),
        sent_at: util.nowIso(),
        client: this.clientInfo,
        records,
      });
      const gz = await gzipBody(payload);
      const headers = { "Content-Type": "application/json" };
      if (gz) headers["Content-Encoding"] = "gzip";
      return this._request("/v1/ingest", {
        method: "POST",
        body: gz || payload,
        headers,
        timeoutMs: constants.LIMITS.ingestTimeoutMs,
      });
    }
  }

  /**
   * Drain the outbox into the server. Each batch gets an id derived from the outbox entry ids
   * it contains, so a batch retried after a lost response is deduplicated server-side.
   * @returns {Promise<{sent: number, remaining: number, error: SyncError|null}>}
   */
  async function flush(outbox, client, opts = {}) {
    const limits = constants.LIMITS;
    let maxRecords = opts.maxRecords || limits.batchMaxRecords;
    let sent = 0;
    let error = null;
    for (let guard = 0; guard < 10000; guard++) {
      const batch = await outbox.peekBatch({ maxRecords, maxBytes: limits.batchMaxBytes });
      if (!batch.length) break;
      const ids = batch.map((e) => e.id);
      const batchId = `${opts.installId || "x"}-${ids[0]}-${ids[ids.length - 1]}-${util.hashString(ids.join(","))}`;
      try {
        await client.ingest(batch.map((e) => e.record), batchId);
        await outbox.ack(ids);
        sent += ids.length;
        maxRecords = opts.maxRecords || limits.batchMaxRecords;
      } catch (err) {
        const e = err instanceof SyncError ? err : new SyncError("server", String(err));
        if (e.kind === "too_large" && maxRecords > 1) {
          maxRecords = Math.max(1, Math.floor(maxRecords / 2));
          continue;
        }
        if (e.kind === "rejected" || (e.kind === "too_large" && maxRecords === 1)) {
          await outbox.markFailed(ids, e.message);
          if (ids.length > 1) {
            // Isolate the bad record(s) by retrying one at a time.
            maxRecords = 1;
            continue;
          }
          continue;
        }
        error = e;
        break;
      }
    }
    const { outbox: remaining } = await outbox.counts();
    return { sent, remaining, error };
  }

  return { ServerClient, SyncError, flush, normaliseBaseUrl };
});
