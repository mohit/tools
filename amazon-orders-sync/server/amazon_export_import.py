"""Bootstrap from Amazon's "Request your data" export (Your Orders).

Amazon's privacy export covers the whole order history (back to the account's first order,
including Whole Foods in-store purchases) that the browser extension can only reach by a slow
crawl. This module:

1. copies the export unchanged to ``raw/amazon/exports/<label>/`` with a SHA-256 manifest
   (immutable: a second import of the same bytes is a no-op, different bytes are refused);
2. converts Order History, Digital Content Orders, Returns Status, Refund Details, Return
   Requests and Digital Returns into the *same* ``order`` / ``return`` records the extension
   sends, keyed the same way (order_id; item_key = ASIN, ``ASIN#2`` for repeats; returns
   ``<order>:<item_key>`` or ``<order>:order``);
3. stores those records as one raw batch (``raw/amazon/batches/...``, so ``rebuild`` replays
   it) and merges them with the server's normal per-field newest-wins rules, observed at the
   export time, so anything the extension sees later wins and the export never overwrites
   newer data;
4. publishes curated parquet atomically.

Cart history, Kindle loans, payment plans, delivery photos and invoice PDFs stay in raw only.
"""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import re
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import datalake_paths as dp

SOURCE_TAG = "privacy_export"
MISSING = {"", "not available", "not applicable", "n/a", "none", "null"}

ORDER_HISTORY = "Your Amazon Orders/Order History.csv"
DIGITAL_ORDERS = "Your Amazon Orders/Digital Content Orders.csv"
DIGITAL_RETURNS = "Your Amazon Orders/Digital Returns.csv"
RETURNS_STATUS = "Your Returns & Refunds/Returns Status.csv"
REFUND_DETAILS = "Your Returns & Refunds/Refund Details.csv"
RETURN_REQUESTS = "Your Returns & Refunds/Return Requests.csv"

# Website / Marketplace values → the host the extension records as `marketplace`.
MARKETPLACES = {
    "amazon.com": "www.amazon.com",
    "www.amazon.com": "www.amazon.com",
    "amazon.in": "www.amazon.in",
    "audible": "www.audible.com",
    "www.audible.com": "www.audible.com",
}

ISO_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?")


# --- value parsing -------------------------------------------------------------------------


def blank(value: Any) -> bool:
    return value is None or str(value).strip().strip("'\"").strip().lower() in MISSING


def text(value: Any) -> str | None:
    return None if blank(value) else str(value).strip()


def money(value: Any) -> float | None:
    if blank(value):
        return None
    try:
        # Some cells carry spreadsheet-guard quotes: "'-5.00'".
        cleaned = str(value).strip().strip("'\"").replace(",", "").replace("$", "").strip()
        return round(float(cleaned), 2)
    except ValueError:
        return None


def integer(value: Any) -> int | None:
    m = money(value)
    return int(m) if m is not None else None


def timestamp(value: Any) -> dt.datetime | None:
    """First ISO-8601 UTC timestamp in the cell ("A and B" cells exist), as aware UTC."""
    if blank(value):
        return None
    m = ISO_RE.search(str(value))
    if not m:
        return None
    raw = m.group(0).rstrip("Z")
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed.replace(tzinfo=dt.UTC)


def local_date(value: Any, tz: ZoneInfo) -> str | None:
    """Amazon pages show order dates in the shopper's local time; match that."""
    ts = timestamp(value)
    return ts.astimezone(tz).date().isoformat() if ts else None


def payment(value: Any) -> tuple[str | None, str | None]:
    raw = text(value)
    if not raw:
        return None, None
    m = re.search(r"(\d{4})\s*$", raw)
    method = re.sub(r"\s*-\s*\d{4}\s*$", "", raw).strip() or None
    return method, m.group(1) if m else None


def marketplace(value: Any) -> str | None:
    raw = text(value)
    return MARKETPLACES.get(raw.lower(), raw) if raw else None


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def _sum(values) -> float | None:
    present = [v for v in values if v is not None]
    return round(sum(present), 2) if present else None


# --- records -------------------------------------------------------------------------------


@dataclass
class Built:
    records: list[dict[str, Any]] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    warnings: list[str] = field(default_factory=list)


def _record(rtype: str, key: str, data: dict[str, Any], mkt: str | None, observed: str) -> dict[str, Any]:
    return {
        "type": rtype,
        "key": key,
        "source": SOURCE_TAG,
        "observed_at": observed,
        "marketplace": mkt,
        "data": data,
    }


def _assign_item_keys(lines: list[dict[str, Any]]) -> None:
    """Same rule as the extension: ASIN, then ASIN#2, ASIN#3 … in page (shipment) order."""
    seen: dict[str, int] = defaultdict(int)
    for line in lines:
        base = line["asin"] or "t-" + hashlib.sha1((line["title"] or "").encode()).hexdigest()[:10]
        seen[base] += 1
        line["item_key"] = base if seen[base] == 1 else f"{base}#{seen[base]}"


def retail_orders(rows: list[dict[str, str]], tz: ZoneInfo, out: Built) -> dict[str, dict]:
    by_order: OrderedDict[str, list[tuple[int, dict[str, str]]]] = OrderedDict()
    for i, row in enumerate(rows):
        oid = text(row.get("Order ID"))
        if oid:
            by_order.setdefault(oid, []).append((i, row))
    orders: dict[str, dict] = {}
    for oid, indexed in by_order.items():
        indexed.sort(key=lambda p: (timestamp(p[1].get("Ship Date")) or dt.datetime.max.replace(tzinfo=dt.UTC), p[0]))
        first = indexed[0][1]
        lines = []
        for _i, row in indexed:
            qty = integer(row.get("Original Quantity")) or 1
            cancelled = (text(row.get("Order Status")) or "").lower() == "cancelled"
            asin = text(row.get("ASIN"))
            mkt = marketplace(row.get("Website"))
            lines.append({
                "asin": asin,
                "title": text(row.get("Product Name")),
                "quantity": qty,
                "unit_price": money(row.get("Unit Price")),
                "unit_tax": money(row.get("Unit Price Tax")),
                "shipping": money(row.get("Shipping Charge")),
                "discount": money(row.get("Total Discounts")),
                "total": money(row.get("Total Amount")),
                "currency": text(row.get("Currency")),
                "condition": text(row.get("Product Condition")),
                "delivery_status": "Cancelled" if cancelled else text(row.get("Shipment Status")),
                "item_url": f"https://{mkt}/dp/{asin}" if asin and mkt and mkt.startswith("www.amazon.") else None,
                "cancelled": cancelled,
            })
        _assign_item_keys(lines)
        live = [ln for ln in lines if not ln["cancelled"]]
        all_cancelled = not live
        # Lines charged $0 (cancelled, free replacements) don't count towards the subtotal.
        priced = [ln for ln in live if ln["total"]]
        method, last4 = payment(first.get("Payment Method Type"))
        subtotal = _sum((ln["unit_price"] or 0) * ln["quantity"] if ln["unit_price"] is not None else None for ln in priced)
        tax = _sum((ln["unit_tax"] or 0) * ln["quantity"] if ln["unit_tax"] is not None else None for ln in priced)
        discount = _sum(ln["discount"] for ln in priced)
        grand = _sum(ln["total"] for ln in priced)
        if all_cancelled:  # nothing was charged; leave totals unknown rather than $0
            subtotal = tax = discount = grand = None
        elif not priced:
            grand = 0.0
        data = {
            "order_id": oid,
            "order_date": local_date(first.get("Order Date"), tz),
            "status": "Cancelled" if all_cancelled else text(first.get("Order Status")),
            "is_cancelled": True if all_cancelled else None,
            "is_digital": None,
            "currency": text(first.get("Currency")),
            "total_amount": grand,
            "subtotal": subtotal,
            "shipping": _sum(ln["shipping"] for ln in priced),
            # Total Discounts is reported negative, like the order page's "Promotion Applied".
            "promotion": -abs(discount) if discount else None,
            "tax": tax,
            "grand_total": grand,
            "payment_method": method,
            "payment_last4": last4,
            "item_count": len(lines),
            "items": [
                {k: ln[k] for k in ("item_key", "asin", "title", "quantity", "unit_price", "currency",
                                    "condition", "delivery_status", "item_url")}
                for ln in lines
            ],
        }
        mkt = marketplace(first.get("Website"))
        orders[oid] = {"data": data, "marketplace": mkt}
        out.counts["retail_orders"] += 1
        out.counts["retail_items"] += len(lines)
        if mkt == "wholefoods.com":
            out.counts["wholefoods_orders"] += 1
    return orders


def digital_orders(rows: list[dict[str, str]], tz: ZoneInfo, out: Built) -> dict[str, dict]:
    by_order: OrderedDict[str, list[dict[str, str]]] = OrderedDict()
    for row in rows:
        oid = text(row.get("Order ID"))
        if oid:
            by_order.setdefault(oid, []).append(row)
    orders: dict[str, dict] = {}
    for oid, rws in by_order.items():
        first = rws[0]
        items: OrderedDict[str, dict[str, Any]] = OrderedDict()
        price_lines, promo_lines, tax_lines = [], [], []
        for row in rws:
            amount = money(row.get("Transaction Amount"))
            component = (text(row.get("Component Type")) or "").lower()
            promo = (text(row.get("Offer Type Code")) or "").lower() == "promotion"
            if component == "tax":
                tax_lines.append(amount)
            elif promo:
                promo_lines.append(amount)
            else:
                price_lines.append(amount)
            asin = text(row.get("ASIN"))
            ident = text(row.get("Digital Order Item ID")) or asin or text(row.get("Product Name")) or ""
            if ident not in items:
                items[ident] = {
                    "asin": asin,
                    "title": text(row.get("Product Name")),
                    "quantity": integer(row.get("Quantity Ordered")) or 1,
                    "unit_price": money(row.get("Price")),
                    "currency": text(row.get("Price Currency Code")) or text(row.get("Base Currency Code")),
                    "seller": text(row.get("Seller of Record")) or text(row.get("Publisher")),
                    "delivery_status": "Delivered" if (text(row.get("Item Fulfilled")) or "").lower() == "yes" else None,
                }
        lines = list(items.values())
        _assign_item_keys(lines)
        cancelled = all((text(r.get("Order Status")) or "").upper().startswith("CANCELLED") for r in rws)
        method, last4 = payment(first.get("Payment Information"))
        grand = _sum(price_lines + promo_lines + tax_lines)
        mkt = marketplace(first.get("Marketplace"))
        orders[oid] = {
            "marketplace": mkt,
            "data": {
                "order_id": oid,
                "order_date": local_date(first.get("Order Date"), tz),
                "status": "Cancelled" if cancelled else "Digital order",
                "is_cancelled": True if cancelled else None,
                "is_digital": True,
                "currency": lines[0]["currency"] if lines else None,
                "total_amount": grand,
                "subtotal": _sum(price_lines),
                "promotion": _sum(promo_lines),
                "tax": _sum(tax_lines),
                "grand_total": grand,
                "payment_method": method,
                "payment_last4": last4,
                "item_count": len(lines),
                "items": lines,
            },
        }
        out.counts["digital_orders"] += 1
        out.counts["digital_items"] += len(lines)
    return orders


def returns(
    export_dir: Path,
    orders: dict[str, dict],
    tz: ZoneInfo,
    out: Built,
) -> list[tuple[str, dict[str, Any], str | None]]:
    """(return_key, data, marketplace) per return, plus refund_total written onto orders."""
    status_rows = read_csv(export_dir / RETURNS_STATUS)
    refund_rows = read_csv(export_dir / REFUND_DETAILS)
    request_rows = read_csv(export_dir / RETURN_REQUESTS)
    digital_rows = read_csv(export_dir / DIGITAL_RETURNS)

    refunds: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in refund_rows:
        if (text(row.get("Disbursement Type")) or "").lower() == "refund" and text(row.get("Order ID")):
            refunds[row["Order ID"].strip()].append(row)
    statuses: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in status_rows:
        if text(row.get("Order ID")):
            statuses[row["Order ID"].strip()].append(row)
    requested: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in request_rows:
        if text(row.get("Order ID")):
            requested[row["Order ID"].strip()].append(row)

    result: list[tuple[str, dict[str, Any], str | None]] = []
    for oid in sorted(set(refunds) | set(statuses) | set(requested)):
        refund_amount = _sum(money(r.get("Refund Amount")) for r in refunds.get(oid, []))
        refund_date = max(
            (d for d in (local_date(r.get("Refund Date"), tz) for r in refunds.get(oid, [])) if d),
            default=None,
        )
        srows = statuses.get(oid, [])
        resolutions = {(text(r.get("Return Resolution")) or "").lower() for r in srows}
        states = {(text(r.get("Return Receivable State")) or "").lower() for r in srows}
        if refund_amount:
            status = "refunded"
        elif "exchange" in resolutions:
            status = "replacement"
        elif "closed" in states:
            status = "return_complete"
        elif states and states <= {"expired"}:
            status = "return_cancelled"
        else:
            status = "return_started"
        return_dates = [local_date(r.get("Date of Return"), tz) or local_date(r.get("Return Creation Date"), tz) for r in srows]
        event_date = refund_date or max((d for d in return_dates if d), default=None)
        reasons = sorted({text(r.get("Return Reason")) for r in srows} - {None})
        status_text = "; ".join(
            p for p in (
                f"Privacy export: {status.replace('_', ' ')}",
                ", ".join(sorted(r.title() for r in resolutions if r and r != "not applicable")) or None,
                f"reason: {', '.join(reasons)}" if reasons else None,
            ) if p
        )
        order = orders.get(oid)
        mkt = order["marketplace"] if order else None
        currency = text((refunds.get(oid) or srows or [{}])[0].get("Currency")) or text(
            (srows or [{}])[0].get("Return Amount Currency")
        )
        if order and refund_amount:
            order["data"]["refund_total"] = refund_amount

        items = (order or {}).get("data", {}).get("items", [])
        by_asin = {it["asin"]: it for it in items if it.get("asin")}
        req_items = [by_asin[a] for a in (text(r.get("ASIN")) for r in requested.get(oid, [])) if a in by_asin]
        common = {"order_id": oid, "status": status, "status_text": status_text,
                  "event_date": event_date, "currency": currency}
        if req_items:
            single = len(req_items) == 1
            for it in req_items:
                result.append((f"{oid}:{it['item_key']}", {
                    **common, "item_key": it["item_key"], "asin": it["asin"], "title": it["title"],
                    "refund_amount": refund_amount if single else None,
                }, mkt))
        else:
            result.append((f"{oid}:order", {**common, "item_key": None, "refund_amount": refund_amount}, mkt))

    digital: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in digital_rows:
        if text(row.get("Order ID")):
            digital[row["Order ID"].strip()].append(row)
    for oid, rws in sorted(digital.items()):
        amount = _sum(abs(m) for m in (money(r.get("Amount Refunded")) or money(r.get("Transaction Amount")) for r in rws) if m is not None)
        order = orders.get(oid)
        if order and amount:
            order["data"]["refund_total"] = amount
        result.append((f"{oid}:order", {
            "order_id": oid, "item_key": None, "status": "refunded",
            "status_text": f"Privacy export: {text(rws[0].get('Return Status')) or 'digital return'}",
            "event_date": max((d for d in (local_date(r.get("Return Date"), tz) for r in rws) if d), default=None),
            "refund_amount": amount, "currency": text(rws[0].get("Base Currency")),
        }, order["marketplace"] if order else None))
    out.counts["returns"] = len(result)
    return result


def build_records(export_dir: Path, observed_at: dt.datetime, tz: ZoneInfo) -> Built:
    out = Built()
    observed = observed_at.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")
    orders = retail_orders(read_csv(export_dir / ORDER_HISTORY), tz, out)
    for oid, order in digital_orders(read_csv(export_dir / DIGITAL_ORDERS), tz, out).items():
        if oid in orders:
            out.warnings.append(f"digital order {oid} also in Order History; keeping retail")
            continue
        orders[oid] = order
    rets = returns(export_dir, orders, tz, out)
    for oid, order in orders.items():
        out.records.append(_record("order", oid, order["data"], order["marketplace"], observed))
    for key, data, mkt in rets:
        out.records.append(_record("return", key, data, mkt, observed))
    if not orders:
        out.warnings.append(f"no orders found: is {export_dir} the 'Your Orders' folder?")
    return out


def latest_timestamp(export_dir: Path) -> dt.datetime | None:
    """Newest timestamp anywhere in the order/return CSVs: a lower bound for export time."""
    latest = None
    for rel in (ORDER_HISTORY, DIGITAL_ORDERS, DIGITAL_RETURNS, RETURNS_STATUS, REFUND_DETAILS):
        for row in read_csv(export_dir / rel):
            for value in row.values():
                ts = timestamp(value)
                if ts and (latest is None or ts > latest):
                    latest = ts
    extra = export_dir / "Additional Data"
    for path in sorted(extra.rglob("*.csv")) if extra.exists() else []:
        for row in read_csv(path):
            for value in row.values():
                ts = timestamp(value)
                if ts and (latest is None or ts > latest):
                    latest = ts
    return latest


# --- raw copy --------------------------------------------------------------------------------


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def export_files(export_dir: Path) -> list[Path]:
    return sorted(
        p for p in export_dir.rglob("*")
        if p.is_file() and not p.name.startswith(".") and not p.is_symlink()
    )


def manifest_for(export_dir: Path) -> dict[str, str]:
    return {str(p.relative_to(export_dir)): _sha256(p) for p in export_files(export_dir)}


def copy_to_raw(export_dir: Path, dest: Path, manifest: dict[str, str]) -> str:
    """Copy the export to *dest* once. Returns "copied" or "unchanged"; refuses to alter it."""
    manifest_path = dest / "MANIFEST.json"
    if dest.exists():
        if not manifest_path.exists():
            raise SystemExit(f"{dest} exists without MANIFEST.json; refusing to touch raw data")
        existing = json.loads(manifest_path.read_text())["files"]
        if existing != manifest:
            raise SystemExit(
                f"{dest} already holds a different export. Raw is immutable: use another --label."
            )
        return "unchanged"
    for rel in manifest:
        dp.publish_file(export_dir / rel, dest / rel)
    dp.atomic_write_text(
        manifest_path,
        json.dumps({"source": "amazon privacy export (Your Orders)", "files": manifest}, indent=2, sort_keys=True) + "\n",
    )
    copied = manifest_for_dest(dest)
    if copied != manifest:
        raise SystemExit(f"verification failed after copying to {dest}")
    return "copied"


def manifest_for_dest(dest: Path) -> dict[str, str]:
    return {k: v for k, v in manifest_for(dest).items() if k != "MANIFEST.json"}


def copy_tree_size(export_dir: Path) -> int:
    return sum(p.stat().st_size for p in export_files(export_dir))

