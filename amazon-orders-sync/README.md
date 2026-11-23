# Amazon Orders Sync

Captures Amazon **orders, items and returns** from your browser (Chrome and Safari, one
shared codebase) and syncs them to a small ingest server on your **Tailscale** network.
While the tailnet or server is unreachable, records are **cached in the browser** and
delivered automatically once it's back.

This is the first "no API, changes often" source for the datalake: the server writes the
same raw → curated layout as the other tools.

```
 amazon.com tab (Chrome or Safari)                         always-on Mac on the tailnet
┌──────────────────────────────────┐                     ┌──────────────────────────────────────────┐
│ content script                    │                     │ amazon_orders_server.py (Flask)           │
│  • passive: parse pages you visit │   runtime msgs      │  POST /v1/ingest ──► raw/amazon/batches/  │
│  • crawl: same-origin fetch +     │ ─────────────┐      │                        *.json.gz (immutable)│
│    DOMParser, paced, CSD fallback │              ▼      │                  └──► DuckDB (merge)       │
└──────────────────────────────────┘   background        │                        └──► curated/amazon/ │
                                        ┌──────────────┐  │                          *.parquet          │
                                        │ IndexedDB    │  │  GET /v1/orders|items|returns|stats        │
                                        │ outbox cache │──┼──► https://<host>.<tailnet>.ts.net         │
                                        └──────────────┘  └──────────────────────────────────────────┘
```

Design, decisions and the execution plan: [`docs/DESIGN.md`](docs/DESIGN.md).

## Layout

```
amazon-orders-sync/
├── extension/                 # one WebExtension source tree → Chrome + Safari builds
│   ├── src/shared/            # ~95% of the code: parsers, crawler, outbox cache, client, planning
│   ├── src/background/        # background entry (service worker in Chrome, event page in Safari)
│   ├── src/content/           # amazon.com content script (passive capture + crawl host)
│   ├── src/ui/                # popup + options
│   ├── manifest/              # base.json + chrome.json / safari.json overrides
│   ├── scripts/build.mjs      # copies src → dist/<browser>, merges manifest, draws icons
│   └── test/                  # node:test unit tests, fixtures, Chrome e2e
└── server/
    ├── amazon_orders_server.py  # Flask API + DuckDB + parquet export; serve|curate|rebuild|status
    ├── tests/
    └── launchd/               # LaunchAgent to keep it running on macOS
```

## 1. Run the server (on the always-on machine)

```bash
cd amazon-orders-sync/server
uv sync
```

```bash
export AMAZON_SYNC_TOKEN="$(openssl rand -hex 24)"   # keep this; the extension needs it
uv run python amazon_orders_server.py serve          # listens on 127.0.0.1:8765
```

Expose it to the tailnet over HTTPS (MagicDNS certificate, tailnet-only):

```bash
tailscale serve --bg 8765
```

The server is then at `https://<machine>.<tailnet>.ts.net`. To keep it running across
reboots, use [`server/launchd/com.mohit.tools.amazon-orders-server.plist`](server/launchd/com.mohit.tools.amazon-orders-server.plist)
(reads the token from the login keychain).

| Variable | Default | Purpose |
|---|---|---|
| `AMAZON_SYNC_TOKEN` | — (required unless `--no-auth`) | Bearer token the extension sends |
| `AMAZON_SYNC_TAILSCALE_USERS` | — | Optional comma list of tailnet logins allowed (checks the `Tailscale-User-Login` header set by `tailscale serve`) |
| `DATALAKE_ROOT` | `/Volumes/MoErMedia/datalake.me` | Verified datalake root (see [`datalake-paths/`](../datalake-paths/README.md)). Raw batches go to `raw/amazon/batches/YYYY/MM/DD/`, parquet to `curated/amazon/`, the serving DuckDB to `databases/amazon-orders-sync/amazon.duckdb` |
| `AMAZON_SYNC_HOST` / `AMAZON_SYNC_PORT` | `127.0.0.1` / `8765` | Bind address |

Every subcommand verifies the datalake root (volume UUID, `.volume-identity.json`, mount
point) before it opens the database or writes anything, so an unplugged or wrong disk fails
with "no data was written". moer-mini is the only production writer. `--db`, `--raw-dir` and
`--curated-dir` override single locations but must stay inside the datalake; pass all three
to run against throwaway paths without touching the lake (tests and `npm run e2e` do this).
Curated parquet is published atomically (new file renamed over the old one); raw batches are
written once and never replaced.

Other commands:

```bash
uv run python amazon_orders_server.py status    # table counts, last batch (server must be stopped)
```

```bash
uv run python amazon_orders_server.py rebuild   # recreate DuckDB from raw batches, re-export parquet
```

```bash
curl -s -H "Authorization: Bearer $AMAZON_SYNC_TOKEN" https://<host>.<tailnet>.ts.net/v1/orders?since=2025-01-01
```

### API

| Method | Path | Notes |
|---|---|---|
| GET | `/v1/health` | Unauthenticated liveness |
| POST | `/v1/ingest` | Batch of records; gzip accepted; idempotent per `batch_id` |
| GET | `/v1/stats` | Counts, last batch, last parquet export |
| GET | `/v1/orders?since=&until=&limit=&offset=` | By `order_date` |
| GET | `/v1/orders/<order_id>` | Order + items + returns |
| GET | `/v1/items?asin=&order_id=&since=` | |
| GET | `/v1/returns?status=&order_id=&since=` | |
| POST | `/v1/admin/export` | Write parquet now |

### Curated tables (`$DATALAKE_ROOT/curated/amazon/*.parquet`)

- **orders** — one row per order: date, status, totals (`total_amount`, `subtotal`, `shipping`, `promotion`, `tax`, `grand_total`, `refund_total`), payment method/last4, ship-to, links, `has_detail`, first/last observed.
- **order_items** — one row per (order, item): ASIN, title, quantity, unit price, seller, condition, delivery status, return status and return-window dates.
- **returns** — one row per returned item (or per order when only a refund total is known): status (`return_started` … `refunded`), refund amount, event date.

```sql
-- duckdb
SELECT date_trunc('month', order_date) AS month, sum(grand_total) - coalesce(sum(refund_total), 0) AS net
FROM '/Volumes/MoErMedia/datalake.me/curated/amazon/orders.parquet' GROUP BY 1 ORDER BY 1;
```

## 2. Install the extension

```bash
cd amazon-orders-sync/extension
npm install
```

```bash
npm run build          # → dist/chrome and dist/safari
```

**Chrome / Chromium / Edge:** `chrome://extensions` → Developer mode → *Load unpacked* → `dist/chrome`.

**Safari (macOS, needs Xcode):**

```bash
npm run safari:xcode   # wraps dist/safari in an Xcode app project at dist/xcode
```

Open the project in Xcode, pick your personal team for signing, Run, then enable the extension
in Safari → Settings → Extensions. For local development without signing, enable
*Develop → Allow Unsigned Extensions*. The Xcode project references `dist/safari`, so after
`npm run build:safari` just re-run from Xcode.

Then open the extension's **Settings**: set the server URL (`https://<host>.<tailnet>.ts.net`)
and token, and press **Test connection**.

## 3. Use it

- **Just browse.** Any *Your Orders*, order-details or returns page you open is captured.
- **Sync recent** (popup) crawls the last 3 months of history and the returns page in an
  Amazon tab (it opens one in the background if needed). A small toast shows progress.
- **Full backfill** walks every year in your history. Polite pacing (≈2–3.5 s per page), so a
  few hundred orders take a while — leave the tab open.
- **Auto-sync:** when an amazon.com tab is open and the last sync is older than the interval
  (default 24 h), a recent-orders crawl runs quietly.
- **Offline:** the badge shows how many records are cached. They're delivered on the next
  attempt (every 5 min, when the network comes back, or *Send cache now*). *Download cache*
  exports them as JSON.

Only `www.amazon.com` (US) is supported for now.

## Tests

```bash
cd amazon-orders-sync/extension && npm test                # parsers, cache, client, crawler, build
```

```bash
cd amazon-orders-sync/extension && npm run e2e             # real Chrome build + real server, fixture pages
```

```bash
cd amazon-orders-sync/server && uv run --extra dev python -m pytest tests/
```

Parser fixtures are synthetic (no real orders). When Amazon changes its markup, see
[`extension/test/fixtures/README.md`](extension/test/fixtures/README.md).

## Credits

Selector lists were cross-checked against the MIT-licensed
[amazon-orders](https://github.com/alexdlaird/amazon-orders) project.
