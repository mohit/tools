/*
 * Offline cache ("outbox") backed by IndexedDB, owned by the background context.
 *
 * Stores:
 *   outbox        records waiting to be delivered to the server (FIFO by auto-increment id)
 *   deadletter    records the server rejected repeatedly; kept for inspection / manual retry
 *   fingerprints  hash of the last *queued* version of each (type, source, key), so unchanged
 *                 observations are not re-sent every crawl
 *   orders        per-order crawl bookkeeping (when detail was last fetched, last status seen)
 *
 * Records stay in the outbox until the server acknowledges them, so nothing is lost while the
 * Tailscale network (or the server) is unreachable.
 */
(function (root, factory) {
  const util = (root.AOS && root.AOS.util) || (typeof require === "function" ? require("./util.js") : null);
  const mod = factory(util);
  root.AOS = Object.assign(root.AOS || {}, { outbox: mod });
  if (typeof module === "object" && module.exports) module.exports = mod;
})(typeof globalThis !== "undefined" ? globalThis : self, function (util) {
  "use strict";

  const DB_VERSION = 1;

  function req(request) {
    return new Promise((resolve, reject) => {
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  }

  function txDone(tx) {
    return new Promise((resolve, reject) => {
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error || new Error("transaction aborted"));
    });
  }

  /** Fingerprint of a record's content: ignores when/where it was observed and the raw HTML. */
  // Item order is not data (the server keys items by order_id + item_key), and Amazon doesn't
  // list multi-item orders in a stable order, so sort before hashing.
  function fingerprint(record) {
    let data = record.data;
    if (data && Array.isArray(data.items)) {
      const items = [...data.items].sort((a, b) => String(a && a.item_key).localeCompare(String(b && b.item_key)));
      data = { ...data, items };
    }
    return util.hashObject({ type: record.type, key: record.key, data });
  }

  function fpKey(record) {
    return `${record.type}|${record.source}|${record.key}`;
  }

  function approxSize(obj) {
    return JSON.stringify(obj).length;
  }

  class Outbox {
    constructor(opts = {}) {
      this.dbName = opts.dbName || "amazon-orders-sync";
      this.idb = opts.indexedDB || globalThis.indexedDB;
      this.maxAttempts = opts.maxAttempts || 8;
      this._db = null;
    }

    async open() {
      if (this._db) return this._db;
      const request = this.idb.open(this.dbName, DB_VERSION);
      request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains("outbox")) db.createObjectStore("outbox", { keyPath: "id", autoIncrement: true });
        if (!db.objectStoreNames.contains("deadletter")) db.createObjectStore("deadletter", { keyPath: "id", autoIncrement: true });
        if (!db.objectStoreNames.contains("fingerprints")) db.createObjectStore("fingerprints", { keyPath: "k" });
        if (!db.objectStoreNames.contains("orders")) db.createObjectStore("orders", { keyPath: "order_id" });
      };
      this._db = await req(request);
      return this._db;
    }

    /**
     * Queue observed records unless an identical version was already queued.
     * @returns {Promise<{queued: number, unchanged: number}>}
     */
    async observe(records) {
      const db = await this.open();
      const tx = db.transaction(["outbox", "fingerprints"], "readwrite");
      const out = tx.objectStore("outbox");
      const fps = tx.objectStore("fingerprints");
      let queued = 0;
      let unchanged = 0;
      for (const record of records) {
        const k = fpKey(record);
        const hash = fingerprint(record);
        const prev = await req(fps.get(k));
        if (prev && prev.hash === hash) {
          unchanged++;
          continue;
        }
        out.add({ record, enqueued_at: util.nowIso(), attempts: 0 });
        fps.put({ k, hash, at: record.observed_at });
        queued++;
      }
      await txDone(tx);
      return { queued, unchanged };
    }

    /** Oldest-first batch bounded by record count and approximate JSON size. */
    async peekBatch({ maxRecords = 200, maxBytes = 2 * 1024 * 1024 } = {}) {
      const db = await this.open();
      const tx = db.transaction("outbox", "readonly");
      const store = tx.objectStore("outbox");
      const batch = [];
      let bytes = 0;
      await new Promise((resolve, reject) => {
        const cursorReq = store.openCursor();
        cursorReq.onerror = () => reject(cursorReq.error);
        cursorReq.onsuccess = () => {
          const cursor = cursorReq.result;
          if (!cursor) return resolve();
          const size = approxSize(cursor.value.record);
          if (batch.length && (batch.length >= maxRecords || bytes + size > maxBytes)) return resolve();
          batch.push(cursor.value);
          bytes += size;
          cursor.continue();
        };
      });
      return batch;
    }

    async ack(ids) {
      const db = await this.open();
      const tx = db.transaction("outbox", "readwrite");
      const store = tx.objectStore("outbox");
      for (const id of ids) store.delete(id);
      await txDone(tx);
    }

    /** Count a delivery failure; entries past maxAttempts move to the dead-letter store. */
    async markFailed(ids, error) {
      const db = await this.open();
      const tx = db.transaction(["outbox", "deadletter"], "readwrite");
      const out = tx.objectStore("outbox");
      const dead = tx.objectStore("deadletter");
      let dead_lettered = 0;
      for (const id of ids) {
        const entry = await req(out.get(id));
        if (!entry) continue;
        entry.attempts = (entry.attempts || 0) + 1;
        entry.last_error = String(error || "");
        if (entry.attempts >= this.maxAttempts) {
          out.delete(id);
          delete entry.id;
          dead.add(entry);
          dead_lettered++;
        } else {
          out.put(entry);
        }
      }
      await txDone(tx);
      return { dead_lettered };
    }

    async counts() {
      const db = await this.open();
      const tx = db.transaction(["outbox", "deadletter"], "readonly");
      const [outbox, deadletter] = await Promise.all([
        req(tx.objectStore("outbox").count()),
        req(tx.objectStore("deadletter").count()),
      ]);
      return { outbox, deadletter };
    }

    async requeueDeadLetters() {
      const db = await this.open();
      const tx = db.transaction(["outbox", "deadletter"], "readwrite");
      const dead = tx.objectStore("deadletter");
      const out = tx.objectStore("outbox");
      const entries = await req(dead.getAll());
      for (const entry of entries) {
        dead.delete(entry.id);
        delete entry.id;
        entry.attempts = 0;
        out.add(entry);
      }
      await txDone(tx);
      return entries.length;
    }

    /** Everything still cached (outbox + dead letters), for a manual JSON export. */
    async exportAll() {
      const db = await this.open();
      const tx = db.transaction(["outbox", "deadletter"], "readonly");
      const [outbox, deadletter] = await Promise.all([
        req(tx.objectStore("outbox").getAll()),
        req(tx.objectStore("deadletter").getAll()),
      ]);
      return { outbox, deadletter };
    }

    async getOrderStates(orderIds) {
      const db = await this.open();
      const tx = db.transaction("orders", "readonly");
      const store = tx.objectStore("orders");
      const out = {};
      for (const id of orderIds) {
        const v = await req(store.get(id));
        if (v) out[id] = v;
      }
      return out;
    }

    /** Merge partial per-order bookkeeping (e.g. {order_id, detail_fetched_at}). */
    async updateOrderStates(partials) {
      const db = await this.open();
      const tx = db.transaction("orders", "readwrite");
      const store = tx.objectStore("orders");
      for (const p of partials) {
        const prev = (await req(store.get(p.order_id))) || {};
        store.put({ ...prev, ...p });
      }
      await txDone(tx);
    }

    /** Forget fingerprints and crawl bookkeeping so the next crawl re-sends everything. */
    async resetSyncState() {
      const db = await this.open();
      const tx = db.transaction(["fingerprints", "orders"], "readwrite");
      tx.objectStore("fingerprints").clear();
      tx.objectStore("orders").clear();
      await txDone(tx);
    }
  }

  return { Outbox, fingerprint, fpKey };
});
