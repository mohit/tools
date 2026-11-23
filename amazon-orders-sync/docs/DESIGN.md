# Amazon Orders Sync — product, design and execution plan

Status: **v0.1, first PR.** Chrome build verified end to end; Safari build produced but not
yet run in Safari (needs a Mac). Decisions that need your call are in
[§6](#6-decisions-to-review).

## 1. Product

### Problem
The datalake already holds sources that have an API (Strava, Last.fm) or a periodic privacy
export (Apple Health, Google Takeout). Some data changes often but has neither: Amazon
orders, items and returns are the first. Amazon's "Request your data" export takes days and
has to be requested by hand, so it can't keep the datalake current.

### Goal
Keep an always-current, queryable copy of Amazon purchase history in the datalake without
manual exports:

- **Orders:** date, status, totals, tax, shipping, promotions, refunds, payment method, ship-to.
- **Items:** ASIN, title, quantity, unit price, seller, delivery status, return window.
- **Returns:** which items were returned, status, refund amount and date.

### Users and constraints
- One user (you). Two browsers: Chrome and Safari, from the same source.
- The data has to land on a machine you control, reachable only over Tailscale.
- Laptops leave the tailnet. Nothing may be lost while offline.
- Stay polite to Amazon: no headless bots and no stored credentials. Only the signed-in browser
  session reads the pages.

### Non-goals (v0.1)
- Marketplaces other than amazon.com, Whole Foods/Fresh receipts, digital content libraries,
  invoices as PDFs, gift-card ledgers, Subscribe & Save management.
- Any write access to Amazon.
- Multi-user or public hosting.

### Success criteria
- After a full backfill, the curated `orders` row count matches the order count Amazon shows
  for each year, and the sum of `grand_total` matches within a few dollars.
- Each order's status and returns are no more than about a day out of date while you use
  Amazon normally.
- No record is lost across a week offline. Re-sending causes no duplicates.

## 2. How it works

```
content script (amazon.com)  ──observations──►  background  ──batches──►  ingest server  ──►  raw / DuckDB / parquet
  passive capture + crawler                      IndexedDB outbox            (tailnet)
```

1. **Capture.** The content script parses order-history, order-detail and returns pages,
   either the page you're looking at (passive) or pages it fetches itself (crawl).
   Fetches are same-origin from the Amazon tab, so they carry your session cookies.
   Parsing uses `DOMParser`.
2. **Cache.** The background context stores records in an IndexedDB **outbox**. A
   fingerprint per `(type, source, key)` skips records whose content hasn't changed, so
   re-visiting pages costs nothing.
3. **Deliver.** The background drains the outbox in gzipped batches of up to 200 records
   (2 MB). It tries after each capture, every 5 minutes (`chrome.alarms`), when the browser
   comes back `online`, and when you press *Send cache now*. A record leaves the cache only
   after the server acknowledges it.
4. **Store.** The server first writes each batch verbatim to the raw layer, then merges it
   into DuckDB. It then exports curated parquet, debounced to 15 s and written atomically.

### Crawl strategy
- **Recent** (default and auto): history filter `months-3`, all pages, then the returns page.
- **Full backfill**: every `year-YYYY` option in the history page's dropdown. If the dropdown
  isn't found, it walks back year by year until three empty years in a row.
- **Detail pages** are fetched for an order when:
  - it has never been fetched, or
  - its list-page status changed since the last fetch, or
  - it is younger than 120 days and its detail is more than 3 days old.

  The last rule picks up late returns and refunds without re-reading the whole history.
- **Pacing:** 2 s plus up to 1.5 s jitter between requests. Sign-in and CAPTCHA pages stop
  the crawl and tell you, rather than retrying.
- **Encrypted cards:** Amazon sometimes serves history cards encrypted for client-side
  decryption. The crawler then refetches with `disableCsd=no-js`, the trick the
  `amazon-orders` project uses. Passive capture doesn't need it, because the live page is
  already decrypted.

### Shared code across Chrome and Safari

| | Chrome | Safari |
|---|---|---|
| Source | `src/` (identical) | `src/` (identical) |
| Background | MV3 service worker `background/sw.js` (generated: `importScripts` of the shared files) | Non-persistent background page listing the same files |
| Manifest | `base.json` + `chrome.json` | `base.json` + `safari.json` |
| Packaging | Load unpacked `dist/chrome` | `xcrun safari-web-extension-converter` → Xcode app |

All browser API access goes through `src/shared/browser.js`. It picks `browser` or `chrome`,
and its `onMessage` uses the `sendResponse` + `return true` form that both browsers support.
Shared modules use a small classic-script pattern. They register on `globalThis.AOS` and also
export via `module.exports`, so the same file runs as a content script, in a service worker,
in a background page, and in node tests. There is no bundler and there are no runtime
dependencies.

### Data model
Wire record (extension → server):
```json
{ "type": "order" | "return", "key": "112-…" | "112-…:B0ASIN", "source": "order_list" | "order_detail" | "returns_page",
  "source_url": "…", "marketplace": "www.amazon.com", "observed_at": "ISO-8601",
  "data": { …parsed fields…, "items": [ … ] }, "raw_html": "<sanitised snippet, optional>" }
```
Envelope: `{schema_version: 1, source: "amazon", batch_id, sent_at, client: {browser, extension_version, install_id}, records}`.

Merge rule on the server: **per column, the newest observation that supplied a value wins,
and a NULL never overwrites a known value.** Freshness is tracked per column (an internal
`field_observed_at` JSON column, not exported), so the sparse list view never blocks a newer
value from the detail view. Observations can arrive in any order (a cache flushed days later,
two browsers) and the result is the same; a test checks every permutation. `rebuild` replays
raw and reproduces the live database; a test checks this too. Order-level refund placeholders
(from a bare "Refund total" line) are dropped whenever item-level returns exist for that order,
whichever arrives first.

### Security and privacy
- Order data only ever goes to your server. There's no third-party service and no analytics.
- Tailnet ACLs are the main boundary. `tailscale serve` adds TLS, and the server binds to
  `127.0.0.1` by default.
- A bearer token is a second layer. It lives in the extension's local storage and is never
  given to content scripts. An optional allow-list checks the `Tailscale-User-Login` header.
- Raw HTML snippets have `<script>`, `<style>`, `<iframe>` and `<svg>` removed and are capped
  at 200 KB per record. They include your name and address as Amazon shows them. You can turn
  them off in Settings.
- Card numbers are never on the page. Only the brand and last four digits are stored.

## 3. What's in this PR

| Area | Done | Verified by |
|---|---|---|
| Parsers (list, detail current + legacy layouts, returns page, sign-in/CAPTCHA detection) | ✅ | 11 fixture tests |
| Crawler (pagination, detail planning, CSD fallback, year discovery, returns, abort) | ✅ | 4 tests with a fake site |
| Offline outbox (IndexedDB, dedupe, FIFO batches, dead-letter, export, reset) | ✅ | 4 tests (fake-indexeddb) |
| Delivery (gzip, idempotent batch ids, offline/auth/413/poison handling) | ✅ | 6 tests |
| Background, popup, options, badge, auto-crawl, progress toast | ✅ | Chrome e2e |
| Chrome + Safari builds from one tree; icons generated | ✅ | build test; Chrome e2e |
| Server: ingest, raw layer, merge, parquet, query API, CORS, auth, rebuild, status | ✅ | 24 pytest tests |
| **End to end in real Chromium:** passive capture offline → crawl offline → reconnect → flush → server merge → no re-sends | ✅ | `npm run e2e` |
| Safari runtime | ⚠️ build only | Needs a Mac: `npm run safari:xcode` |
| Real Amazon pages | ⚠️ not yet | Fixtures are synthetic. First real run is phase 1 below |

## 4. Execution plan

**Phase 0 (this PR): scaffold and pipeline.** Everything in §3.

**Phase 1: first real run (about 1 hour, needs you).**
1. Start the server on the always-on Mac (`launchd` plist + `tailscale serve --bg 8765`).
2. Load the Chrome build, set URL and token, then *Test connection*.
3. Open *Your Orders*, then *Sync recent*. Compare a few orders against the page, field by field.
4. If a field is wrong, use its `raw_html` from the raw batch to make a scrubbed fixture,
   fix the selector, and add a test. Repeat for an order with a return.
5. Run *Full backfill*. Check yearly counts against Amazon (the success criteria in §1).
6. Build for Safari, run once, and confirm the same results.

**Phase 2: hardening (after a week of use).**
- Confirm the returns-centre URL and markup (best effort today; see §6, decision 9).
- Add a cap on outbox size with an oldest-first eviction warning.
- Have the server re-parse `raw_html` so improved parsers can backfill history without
  re-crawling (`rebuild --reparse`). This needs the parsers in Python, or node invoked from
  the server.
- Add an entry for the source in `catalog.yaml`, plus a staleness alert like Last.fm's
  (no new batch in more than 7 days).
- Add a CI job that runs the Chrome e2e in headless Chromium.

**Phase 3: more sources, same pipe.**
The wire format is already source-tagged. The next "no API" sites (for example
Costco/Target orders, utility bills, Uber/Lyft trips) would each add a parser module and a
`source` value. Rename the tool to something like `browser-datalake-sync` when the second
source lands, not before.

## 5. Risks and limitations
- **Markup drift.** Amazon changes layouts without notice. Mitigations:
  - selector lists, newest layout first, with text heuristics as a fallback;
  - raw HTML kept so history can be re-parsed;
  - fixture-driven tests.

  Expect a parser fix now and then.
- **Account risk.** Automated browsing is against Amazon's conditions of use. The crawler
  runs in your own signed-in session, at human-ish pace and modest volume, and stops at the
  first CAPTCHA. It still carries some risk; see decision 8.
- **Safari specifics.** Safari can unload the background page aggressively. All state is in
  storage or IndexedDB, and the crawl runs in the tab, so this should be fine, but it hasn't
  been verified.
- **Single writer.** DuckDB is held open by the server, so other tools read the parquet, not
  the `.duckdb` file. `status` and `curate` from the CLI need the server stopped, or use
  `POST /v1/admin/export`.

## 6. Decisions to review

Each entry gives what I chose and the alternative. ★ marks the ones where your input matters most.

1. **Scrape in a real tab via the content script**, not in the background or headless.
   - MV3 service workers have no `DOMParser`.
   - Safari doesn't reliably send Amazon cookies from extension background contexts.
   - Same-origin fetch from the tab works the same in both browsers.
   - Alternative: an offscreen document (Chrome only, which breaks the shared-code goal), or a
     Playwright bot holding Amazon credentials (rejected: credential storage, bot detection).
2. ★ **Service = Flask + DuckDB on one tailnet Mac, exposed with `tailscale serve`.**
   - It matches the repo's stack: Flask is the only allowed framework, and DuckDB/parquet are
     what the other tools use.
   - Alternatives: Postgres or SQLite with a REST layer, which allows multiple readers on the
     DB but adds another stack; or writing straight to iCloud files, which risks sync
     conflicts and has no "server is reachable" signal.
3. **DuckDB file at `~/.local/share/datalake/amazon/`, off iCloud.** Raw and curated go in the
   iCloud datalake as usual. A live DuckDB file in an iCloud folder risks corruption.
4. ★ **Raw layer = the received batch, including sanitised HTML snippets** (on by default).
   - It gives re-parseability, at the cost of size: roughly 5–50 KB per order, so a few
     hundred MB for a long history (gzipped on disk).
   - Alternative: parsed JSON only.
5. **Auth = tailnet + bearer token**, with an optional Tailscale identity allow-list. Without
   the token, anything on the tailnet could write to the server.
6. **Merge = newest non-null wins per column** (per-column freshness), keeping
   `first_observed_at` and `last_observed_at`. This version keeps no observation history in the database; history
   lives in raw. Alternative: an SCD2 history table if you want status timelines in SQL.
7. **Cache = IndexedDB outbox with fingerprint dedupe.** Records move to a dead-letter store
   after 8 rejections. There is no size cap yet.
8. ★ **Auto-sync defaults to every 24 h when an Amazon tab is open**, crawling recent orders
   with 2 s+ pacing. Alternatives: off by default (passive and manual only), or more often.
   This is the main account-risk knob.
9. ★ **Returns come mainly from order pages** (status lines, refund totals); the returns-centre
   page (`/spr/returns/history`) is best effort with a heuristic parser. I couldn't verify
   that URL or its markup without a real account. Phase 1 will show whether it's needed.
10. **amazon.com only.** Date and currency parsing already handle `£`/`€` and day-first dates;
    adding `.co.uk` or `.ca` is mostly manifest matches plus fixtures.
11. **No bundler or TypeScript; plain JS with a global namespace.** Zero build dependencies,
    and debugging shows the exact source. Revisit if the extension grows a lot.
12. **Tool name and location `amazon-orders-sync/` (extension + server)**, not a generic
    "browser collector" yet; see phase 3.
13. **Process:** AGENTS.md asks for `fix/issue-N` branches and one issue per PR. This is new
    work with no issue, on the session branch `claude/friendly-feynman-9f03le`. It also adds
    the tool to the root README, the AGENTS.md structure and the CI matrix.
