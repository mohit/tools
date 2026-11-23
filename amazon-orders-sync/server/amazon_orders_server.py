#!/usr/bin/env python3
"""Ingest server for the Amazon Orders Sync browser extension.

Runs on an always-on machine in the tailnet. The extension POSTs batches of observed
records (orders with their items, and returns); this server:

1. writes every accepted batch, exactly as received, to the raw layer
   (``~/datalake.me/raw/amazon/batches/YYYY/MM/DD/*.json.gz``, immutable);
2. merges the records into a DuckDB database (``~/.local/share/datalake/amazon/amazon.duckdb``,
   kept off iCloud on purpose) using "newest observation wins, never overwrite a value with
   null" semantics;
3. exports curated parquet (``~/datalake.me/curated/amazon/{orders,order_items,returns}.parquet``)
   shortly after each ingest, written atomically so other tools can read it at any time.

Because (2) and (3) are derived from (1), ``rebuild`` can always regenerate them from raw.

Subcommands::

    serve     run the HTTP API (default 127.0.0.1:8765; expose with `tailscale serve`)
    curate    export curated parquet from the database
    rebuild   recreate the database from raw batches, then export
    status    print table counts and the latest batch
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hmac
import json
import os
import re
import shutil
import sys
import threading
import uuid
import zlib
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
from flask import Flask, Response, jsonify, request

SERVICE = "amazon-orders-sync"
VERSION = "0.1.0"
SCHEMA_VERSION = 1
MAX_BODY_BYTES = 32 * 1024 * 1024
MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024
BATCH_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
RECORD_TYPES = {"order", "return"}

# --- schema ------------------------------------------------------------------------------

ORDER_COLUMNS: dict[str, str] = {
    "order_id": "VARCHAR PRIMARY KEY",
    "marketplace": "VARCHAR",
    "order_date": "DATE",
    "status": "VARCHAR",
    "is_cancelled": "BOOLEAN",
    "is_digital": "BOOLEAN",
    "currency": "VARCHAR",
    "total_amount": "DECIMAL(12,2)",
    "subtotal": "DECIMAL(12,2)",
    "shipping": "DECIMAL(12,2)",
    "promotion": "DECIMAL(12,2)",
    "total_before_tax": "DECIMAL(12,2)",
    "tax": "DECIMAL(12,2)",
    "grand_total": "DECIMAL(12,2)",
    "refund_total": "DECIMAL(12,2)",
    "gift_card": "DECIMAL(12,2)",
    "payment_method": "VARCHAR",
    "payment_last4": "VARCHAR",
    "ship_to_name": "VARCHAR",
    "ship_to_address": "VARCHAR",
    "detail_url": "VARCHAR",
    "invoice_url": "VARCHAR",
    "item_count": "INTEGER",
    "has_detail": "BOOLEAN",
    "first_observed_at": "TIMESTAMP",
    "last_observed_at": "TIMESTAMP",
    "last_detail_at": "TIMESTAMP",
    "last_source": "VARCHAR",
    "last_batch_id": "VARCHAR",
}

ITEM_COLUMNS: dict[str, str] = {
    "order_id": "VARCHAR",
    "item_key": "VARCHAR",
    "asin": "VARCHAR",
    "title": "VARCHAR",
    "quantity": "INTEGER",
    "unit_price": "DECIMAL(12,2)",
    "currency": "VARCHAR",
    "seller": "VARCHAR",
    "condition": "VARCHAR",
    "subscription": "VARCHAR",
    "item_url": "VARCHAR",
    "image_url": "VARCHAR",
    "delivery_status": "VARCHAR",
    "return_status": "VARCHAR",
    "return_policy_text": "VARCHAR",
    "return_eligible_until": "DATE",
    "return_window_closed_on": "DATE",
    "first_observed_at": "TIMESTAMP",
    "last_observed_at": "TIMESTAMP",
    "last_source": "VARCHAR",
}

RETURN_COLUMNS: dict[str, str] = {
    "return_key": "VARCHAR PRIMARY KEY",
    "order_id": "VARCHAR",
    "item_key": "VARCHAR",
    "asin": "VARCHAR",
    "title": "VARCHAR",
    "status": "VARCHAR",
    "status_text": "VARCHAR",
    "event_date": "DATE",
    "refund_amount": "DECIMAL(12,2)",
    "currency": "VARCHAR",
    "first_observed_at": "TIMESTAMP",
    "last_observed_at": "TIMESTAMP",
    "last_source": "VARCHAR",
}

BATCH_COLUMNS: dict[str, str] = {
    "batch_id": "VARCHAR PRIMARY KEY",
    "received_at": "TIMESTAMP",
    "sent_at": "TIMESTAMP",
    "client_browser": "VARCHAR",
    "client_version": "VARCHAR",
    "install_id": "VARCHAR",
    "record_count": "INTEGER",
    "raw_path": "VARCHAR",
}

TABLES = {
    "orders": (ORDER_COLUMNS, ""),
    "order_items": (ITEM_COLUMNS, ", PRIMARY KEY (order_id, item_key)"),
    "returns": (RETURN_COLUMNS, ""),
    "ingest_batches": (BATCH_COLUMNS, ""),
}

# Columns the merge never takes from an incoming observation directly.
BOOKKEEPING = {
    "first_observed_at",
    "last_observed_at",
    "last_detail_at",
    "last_source",
    "last_batch_id",
}
MONEY_TYPE = "DECIMAL(12,2)"


# --- config ------------------------------------------------------------------------------


def _env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value).expanduser() if value else default


@dataclass
class Config:
    raw_dir: Path = field(
        default_factory=lambda: (
            _env_path("DATALAKE_RAW_ROOT", Path.home() / "datalake.me" / "raw")
            / "amazon"
        )
    )
    curated_dir: Path = field(
        default_factory=lambda: (
            _env_path("DATALAKE_CURATED_ROOT", Path.home() / "datalake.me" / "curated")
            / "amazon"
        )
    )
    db_path: Path = field(
        default_factory=lambda: _env_path(
            "AMAZON_SYNC_DB",
            Path.home() / ".local" / "share" / "datalake" / "amazon" / "amazon.duckdb",
        )
    )
    token: str | None = field(
        default_factory=lambda: os.environ.get("AMAZON_SYNC_TOKEN") or None
    )
    allowed_tailscale_users: frozenset[str] = field(
        default_factory=lambda: frozenset(
            u.strip()
            for u in os.environ.get("AMAZON_SYNC_TAILSCALE_USERS", "").split(",")
            if u.strip()
        )
    )
    export_delay_s: float = 15.0
    allow_no_auth: bool = False


# --- value coercion ------------------------------------------------------------------------


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def parse_ts(value: Any) -> dt.datetime | None:
    """ISO-8601 → naive UTC datetime (DuckDB TIMESTAMP)."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.UTC).replace(tzinfo=None)
    return parsed


def coerce(value: Any, sql_type: str) -> Any:
    """Coerce a JSON value to the column type; anything unparseable becomes NULL."""
    if value is None or value == "":
        return None
    try:
        if sql_type.startswith("DATE"):
            return dt.date.fromisoformat(str(value)[:10])
        if sql_type.startswith("TIMESTAMP"):
            return value if isinstance(value, dt.datetime) else parse_ts(value)
        if sql_type.startswith("DECIMAL"):
            if isinstance(value, bool):
                return None
            return Decimal(str(value)).quantize(Decimal("0.01"))
        if sql_type.startswith("INTEGER"):
            if isinstance(value, bool):
                return None
            return int(value)
        if sql_type.startswith("BOOLEAN"):
            return bool(value) if isinstance(value, bool) else None
        text = str(value)
        return text[:4000]
    except (ValueError, ArithmeticError, TypeError):
        return None


def project(data: dict[str, Any], columns: dict[str, str]) -> dict[str, Any]:
    return {
        col: coerce(data.get(col), sql_type)
        for col, sql_type in columns.items()
        if col not in BOOKKEEPING
    }


def merge_row(
    existing: dict[str, Any] | None,
    incoming: dict[str, Any],
    observed_at: dt.datetime,
    source: str,
) -> dict[str, Any]:
    """Merge one observation into the stored row.

    Newest observation wins per column, but a NULL never overwrites a known value. This lets
    the sparse order-list view and the rich order-detail view of the same order coexist, and
    makes replaying batches out of order (cache flushed after days offline) converge to the
    same result.
    """
    if existing is None:
        row = dict(incoming)
        row["first_observed_at"] = observed_at
        row["last_observed_at"] = observed_at
        row["last_source"] = source
        return row
    last_seen = existing.get("last_observed_at") or dt.datetime.min
    newer = observed_at >= last_seen
    row = dict(existing)
    for col, new in incoming.items():
        old = existing.get(col)
        if new is None:
            continue
        if old is None or newer:
            row[col] = new
    if "has_detail" in incoming:
        # Once an order's detail page has been seen it stays "has_detail", whatever arrives later.
        row["has_detail"] = bool(existing.get("has_detail")) or bool(
            incoming.get("has_detail")
        )
    row["first_observed_at"] = min(
        existing.get("first_observed_at") or observed_at, observed_at
    )
    row["last_observed_at"] = max(last_seen, observed_at)
    if newer:
        row["last_source"] = source
    return row


# --- raw layer -----------------------------------------------------------------------------


def write_raw_batch(
    raw_dir: Path, envelope: dict[str, Any], received_at: dt.datetime
) -> Path:
    """Write the received envelope verbatim (plus receipt metadata) as gzipped JSON, atomically."""
    day_dir = (
        raw_dir
        / "batches"
        / f"{received_at:%Y}"
        / f"{received_at:%m}"
        / f"{received_at:%d}"
    )
    day_dir.mkdir(parents=True, exist_ok=True)
    name = f"{received_at:%Y%m%dT%H%M%S%f}Z_{envelope['batch_id']}.json.gz"
    final = day_dir / name
    tmp = day_dir / f".{name}.{uuid.uuid4().hex}.tmp"
    payload = {"received_at": received_at.isoformat() + "Z", "envelope": envelope}
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
    os.replace(tmp, final)
    return final


def iter_raw_batches(raw_dir: Path):
    for path in sorted((raw_dir / "batches").rglob("*.json.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            payload = json.load(fh)
        yield path, payload


# --- validation ----------------------------------------------------------------------------


class BadRequest(ValueError):
    pass


def validate_envelope(envelope: Any) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        raise BadRequest("body must be a JSON object")
    if envelope.get("source", "amazon") != "amazon":
        raise BadRequest(f"unsupported source {envelope.get('source')!r}")
    if envelope.get("schema_version") != SCHEMA_VERSION:
        raise BadRequest(
            f"unsupported schema_version {envelope.get('schema_version')!r}"
        )
    batch_id = envelope.get("batch_id")
    if not isinstance(batch_id, str) or not BATCH_ID_RE.match(batch_id):
        raise BadRequest("batch_id must match [A-Za-z0-9._-]{1,128}")
    records = envelope.get("records")
    if not isinstance(records, list) or not records:
        raise BadRequest("records must be a non-empty list")
    for i, rec in enumerate(records):
        if not isinstance(rec, dict):
            raise BadRequest(f"records[{i}] must be an object")
        if rec.get("type") not in RECORD_TYPES:
            raise BadRequest(f"records[{i}].type must be one of {sorted(RECORD_TYPES)}")
        key = rec.get("key")
        if not isinstance(key, str) or not key or len(key) > 256:
            raise BadRequest(f"records[{i}].key must be a non-empty string")
        if not isinstance(rec.get("data"), dict):
            raise BadRequest(f"records[{i}].data must be an object")
        if rec["type"] == "order" and rec["data"].get("order_id") != key:
            raise BadRequest(f"records[{i}]: order key must equal data.order_id")
    return envelope


# --- database ------------------------------------------------------------------------------


def _ddl(table: str) -> str:
    columns, suffix = TABLES[table]
    cols = ", ".join(f"{name} {sql_type}" for name, sql_type in columns.items())
    return f"CREATE TABLE IF NOT EXISTS {table} ({cols}{suffix})"


class Store:
    """All DuckDB access. One connection, serialised by a lock (personal-scale traffic)."""

    def __init__(self, db_path: Path, read_only: bool = False):
        self.db_path = db_path
        if not read_only:
            db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(db_path), read_only=read_only)
        self.lock = threading.RLock()
        if not read_only:
            with self.lock:
                for table in TABLES:
                    self.con.execute(_ddl(table))

    def close(self) -> None:
        with self.lock:
            self.con.close()

    # -- helpers --
    def _fetch_dicts(
        self, sql: str, params: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        cur = self.con.execute(sql, params or [])
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]

    def _get(self, table: str, where: dict[str, Any]) -> dict[str, Any] | None:
        clause = " AND ".join(f"{k} = ?" for k in where)
        rows = self._fetch_dicts(
            f"SELECT * FROM {table} WHERE {clause}", list(where.values())
        )
        return rows[0] if rows else None

    def _put(self, table: str, row: dict[str, Any]) -> None:
        columns = list(TABLES[table][0])
        placeholders = ", ".join("?" for _ in columns)
        self.con.execute(
            f"INSERT OR REPLACE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            [row.get(c) for c in columns],
        )

    # -- ingest --
    def has_batch(self, batch_id: str) -> bool:
        with self.lock:
            return self._get("ingest_batches", {"batch_id": batch_id}) is not None

    def apply_envelope(
        self, envelope: dict[str, Any], raw_path: Path, received_at: dt.datetime
    ) -> dict[str, int]:
        counts = {"orders": 0, "order_items": 0, "returns": 0}
        batch_id = envelope["batch_id"]
        client = envelope.get("client") or {}
        with self.lock:
            self.con.execute("BEGIN TRANSACTION")
            try:
                for rec in envelope["records"]:
                    observed_at = parse_ts(rec.get("observed_at")) or received_at
                    source = str(rec.get("source") or "unknown")
                    if rec["type"] == "order":
                        self._apply_order(rec, observed_at, source, batch_id, counts)
                    else:
                        self._apply_return(rec, observed_at, source, counts)
                self._put(
                    "ingest_batches",
                    {
                        "batch_id": batch_id,
                        "received_at": received_at,
                        "sent_at": parse_ts(envelope.get("sent_at")),
                        "client_browser": coerce(client.get("browser"), "VARCHAR"),
                        "client_version": coerce(
                            client.get("extension_version"), "VARCHAR"
                        ),
                        "install_id": coerce(client.get("install_id"), "VARCHAR"),
                        "record_count": len(envelope["records"]),
                        "raw_path": str(raw_path),
                    },
                )
                self.con.execute("COMMIT")
            except Exception:
                self.con.execute("ROLLBACK")
                raise
        return counts

    def _apply_order(self, rec, observed_at, source, batch_id, counts) -> None:
        data = rec["data"]
        order_id = rec["key"]
        incoming = project(
            {**data, "marketplace": rec.get("marketplace")}, ORDER_COLUMNS
        )
        incoming["has_detail"] = True if source == "order_detail" else None
        existing = self._get("orders", {"order_id": order_id})
        row = merge_row(existing, incoming, observed_at, source)
        row["last_batch_id"] = batch_id
        if source == "order_detail":
            prev = existing.get("last_detail_at") if existing else None
            row["last_detail_at"] = max(prev, observed_at) if prev else observed_at
        self._put("orders", row)
        counts["orders"] += 1
        for item in data.get("items") or []:
            if not isinstance(item, dict) or not item.get("item_key"):
                continue
            item_in = project({**item, "order_id": order_id}, ITEM_COLUMNS)
            key = {"order_id": order_id, "item_key": item_in["item_key"]}
            item_row = merge_row(
                self._get("order_items", key), item_in, observed_at, source
            )
            self._put("order_items", item_row)
            counts["order_items"] += 1

    def _apply_return(self, rec, observed_at, source, counts) -> None:
        incoming = project({**rec["data"], "return_key": rec["key"]}, RETURN_COLUMNS)
        existing = self._get("returns", {"return_key": rec["key"]})
        self._put("returns", merge_row(existing, incoming, observed_at, source))
        # An item-level return supersedes the order-level placeholder created from a bare
        # "Refund total" line, so refunds aren't double counted.
        if incoming.get("item_key") and incoming.get("order_id"):
            self.con.execute(
                "DELETE FROM returns WHERE return_key = ?",
                [f"{incoming['order_id']}:order"],
            )
        counts["returns"] += 1

    # -- reads --
    def stats(self) -> dict[str, Any]:
        with self.lock:
            tables = {
                t: self.con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                for t in TABLES
            }
            last = self._fetch_dicts(
                "SELECT * FROM ingest_batches ORDER BY received_at DESC LIMIT 1"
            )
            latest = self.con.execute(
                "SELECT max(order_date), min(order_date) FROM orders"
            ).fetchone()
        return {
            "tables": tables,
            "last_batch": last[0] if last else None,
            "latest_order_date": latest[0],
            "earliest_order_date": latest[1],
        }

    def query(
        self,
        table: str,
        *,
        since: str | None,
        until: str | None,
        limit: int,
        offset: int,
        filters: dict[str, Any],
    ):
        date_col = {"orders": "order_date", "returns": "event_date"}.get(table)
        where, params = [], []
        if table == "order_items" and (since or until):
            date_sql = "(SELECT order_date FROM orders o WHERE o.order_id = order_items.order_id)"
        else:
            date_sql = date_col
        if since and date_sql:
            where.append(f"{date_sql} >= ?")
            params.append(since)
        if until and date_sql:
            where.append(f"{date_sql} <= ?")
            params.append(until)
        for col, value in filters.items():
            if value:
                where.append(f"{col} = ?")
                params.append(value)
        sql = f"SELECT * FROM {table}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        order_col = (
            date_sql if date_sql and table != "order_items" else "last_observed_at"
        )
        sql += f" ORDER BY {order_col} DESC NULLS LAST LIMIT ? OFFSET ?"
        with self.lock:
            return self._fetch_dicts(sql, [*params, limit, offset])

    def order_bundle(self, order_id: str) -> dict[str, Any] | None:
        with self.lock:
            order = self._get("orders", {"order_id": order_id})
            if not order:
                return None
            order["items"] = self._fetch_dicts(
                "SELECT * FROM order_items WHERE order_id = ? ORDER BY item_key",
                [order_id],
            )
            order["returns"] = self._fetch_dicts(
                "SELECT * FROM returns WHERE order_id = ?", [order_id]
            )
        return order

    # -- curated export --
    def export_curated(self, curated_dir: Path) -> dict[str, str]:
        curated_dir.mkdir(parents=True, exist_ok=True)
        written = {}
        orderings = {
            "orders": "order_date, order_id",
            "order_items": "order_id, item_key",
            "returns": "order_id, return_key",
        }
        with self.lock:
            for table, ordering in orderings.items():
                final = curated_dir / f"{table}.parquet"
                tmp = curated_dir / f".{table}.{uuid.uuid4().hex}.parquet.tmp"
                self.con.execute(
                    f"COPY (SELECT * FROM {table} ORDER BY {ordering}) TO '{_sql_str(tmp)}' (FORMAT 'parquet')"
                )
                os.replace(tmp, final)
                written[table] = str(final)
        return written


def _sql_str(path: Path) -> str:
    return str(path).replace("'", "''")


def to_jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dt.datetime):
        return value.isoformat() + "Z"
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_jsonable(v) for v in value]
    return value


# --- service -------------------------------------------------------------------------------


class Ingestor:
    """Raw write + DB merge + debounced curated export."""

    def __init__(self, config: Config, store: Store):
        self.config = config
        self.store = store
        self._timer: threading.Timer | None = None
        self._timer_lock = threading.Lock()
        self.last_export_at: dt.datetime | None = None
        self.last_export_error: str | None = None

    def ingest(self, envelope: dict[str, Any]) -> dict[str, Any]:
        validate_envelope(envelope)
        batch_id = envelope["batch_id"]
        if self.store.has_batch(batch_id):
            return {"ok": True, "duplicate": True, "batch_id": batch_id, "accepted": 0}
        received_at = utcnow()
        raw_path = write_raw_batch(self.config.raw_dir, envelope, received_at)
        counts = self.store.apply_envelope(envelope, raw_path, received_at)
        self.schedule_export()
        return {
            "ok": True,
            "duplicate": False,
            "batch_id": batch_id,
            "accepted": len(envelope["records"]),
            **counts,
        }

    def export_now(self) -> dict[str, str]:
        try:
            written = self.store.export_curated(self.config.curated_dir)
            self.last_export_at = utcnow()
            self.last_export_error = None
            return written
        except Exception as exc:  # keep serving even if the curated dir is unavailable
            self.last_export_error = str(exc)
            raise

    def schedule_export(self) -> None:
        if self.config.export_delay_s <= 0:
            self.export_now()
            return
        with self._timer_lock:
            if self._timer is not None:
                return
            self._timer = threading.Timer(
                self.config.export_delay_s, self._run_scheduled_export
            )
            self._timer.daemon = True
            self._timer.start()

    def _run_scheduled_export(self) -> None:
        with self._timer_lock:
            self._timer = None
        try:
            self.export_now()
        except Exception as exc:  # noqa: BLE001
            print(f"[{SERVICE}] curated export failed: {exc}", file=sys.stderr)


def _read_body() -> Any:
    raw = request.get_data(cache=False)
    if request.headers.get("Content-Encoding", "").lower() == "gzip":
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
        try:
            raw = decompressor.decompress(raw, MAX_DECOMPRESSED_BYTES)
        except zlib.error as exc:
            raise BadRequest(f"invalid gzip body: {exc}") from exc
        if decompressor.unconsumed_tail:
            raise BadRequest("decompressed body too large")
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise BadRequest(f"invalid JSON: {exc}") from exc


def create_app(config: Config, store: Store | None = None) -> Flask:
    if not config.token and not config.allow_no_auth:
        raise SystemExit(
            "AMAZON_SYNC_TOKEN is not set. Set it (e.g. `openssl rand -hex 24`) or pass --no-auth."
        )
    store = store or Store(config.db_path)
    ingestor = Ingestor(config, store)
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = MAX_BODY_BYTES
    app.extensions["amazon_sync"] = {
        "store": store,
        "ingestor": ingestor,
        "config": config,
    }

    def authorized() -> Response | None:
        if config.allowed_tailscale_users:
            # Set by `tailscale serve` for requests from tailnet users. Only meaningful when this
            # server listens on localhost behind tailscale serve.
            login = request.headers.get("Tailscale-User-Login", "")
            if login not in config.allowed_tailscale_users:
                return jsonify(error="tailscale user not allowed"), 403
        if config.token:
            header = request.headers.get("Authorization", "")
            supplied = header[7:] if header.startswith("Bearer ") else ""
            if not hmac.compare_digest(supplied.encode(), config.token.encode()):
                return jsonify(error="unauthorized"), 401
        return None

    @app.after_request
    def cors(resp: Response) -> Response:
        # The extension calls from a chrome-extension:// or safari-web-extension:// origin.
        # Auth is a bearer token (not cookies), so a wildcard origin is safe here.
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Headers"] = (
            "Authorization, Content-Type, Content-Encoding"
        )
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Private-Network"] = "true"
        resp.headers["Access-Control-Max-Age"] = "600"
        return resp

    # CORS preflight (OPTIONS) is answered by Flask's automatic OPTIONS handling + the hook above.

    @app.get("/v1/health")
    def health():
        return jsonify(
            ok=True,
            service=SERVICE,
            version=VERSION,
            schema_version=SCHEMA_VERSION,
            time=utcnow().isoformat() + "Z",
        )

    @app.post("/v1/ingest")
    def ingest():
        if (denied := authorized()) is not None:
            return denied
        try:
            envelope = _read_body()
            result = ingestor.ingest(envelope)
        except BadRequest as exc:
            return jsonify(error=str(exc)), 400
        if not result["duplicate"]:
            print(
                f"[{SERVICE}] batch {result['batch_id']}: {result['accepted']} records "
                f"({result['orders']} orders, {result['order_items']} items, {result['returns']} returns)",
                flush=True,
            )
        return jsonify(result)

    @app.get("/v1/stats")
    def stats():
        if (denied := authorized()) is not None:
            return denied
        payload = store.stats()
        payload["last_export_at"] = ingestor.last_export_at
        payload["last_export_error"] = ingestor.last_export_error
        return jsonify(to_jsonable(payload))

    def _list(table: str, filters: dict[str, Any]):
        if (denied := authorized()) is not None:
            return denied
        try:
            limit = min(max(int(request.args.get("limit", 100)), 1), 5000)
            offset = max(int(request.args.get("offset", 0)), 0)
        except ValueError:
            return jsonify(error="limit/offset must be integers"), 400
        rows = store.query(
            table,
            since=request.args.get("since"),
            until=request.args.get("until"),
            limit=limit,
            offset=offset,
            filters=filters,
        )
        return jsonify(to_jsonable({"rows": rows, "limit": limit, "offset": offset}))

    @app.get("/v1/orders")
    def list_orders():
        return _list("orders", {})

    @app.get("/v1/orders/<order_id>")
    def get_order(order_id: str):
        if (denied := authorized()) is not None:
            return denied
        bundle = store.order_bundle(order_id)
        if bundle is None:
            return jsonify(error="not found"), 404
        return jsonify(to_jsonable(bundle))

    @app.get("/v1/items")
    def list_items():
        return _list(
            "order_items",
            {
                "asin": request.args.get("asin"),
                "order_id": request.args.get("order_id"),
            },
        )

    @app.get("/v1/returns")
    def list_returns():
        return _list(
            "returns",
            {
                "status": request.args.get("status"),
                "order_id": request.args.get("order_id"),
            },
        )

    @app.post("/v1/admin/export")
    def export():
        if (denied := authorized()) is not None:
            return denied
        return jsonify(written=ingestor.export_now())

    return app


# --- CLI -----------------------------------------------------------------------------------


def config_from_args(args: argparse.Namespace) -> Config:
    cfg = Config()
    if getattr(args, "raw_dir", None):
        cfg.raw_dir = Path(args.raw_dir).expanduser()
    if getattr(args, "curated_dir", None):
        cfg.curated_dir = Path(args.curated_dir).expanduser()
    if getattr(args, "db", None):
        cfg.db_path = Path(args.db).expanduser()
    if getattr(args, "export_delay", None) is not None:
        cfg.export_delay_s = args.export_delay
    if getattr(args, "no_auth", False):
        cfg.allow_no_auth = True
        cfg.token = None
    return cfg


def cmd_serve(args: argparse.Namespace) -> int:
    cfg = config_from_args(args)
    app = create_app(cfg)
    print(f"[{SERVICE}] db={cfg.db_path} raw={cfg.raw_dir} curated={cfg.curated_dir}")
    print(f"[{SERVICE}] listening on http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, threaded=True)
    return 0


def _open_store_or_explain(cfg: Config, read_only: bool) -> Store:
    try:
        return Store(cfg.db_path, read_only=read_only)
    except duckdb.IOException as exc:
        raise SystemExit(
            f"Could not open {cfg.db_path}: {exc}\n"
            "If the server is running, use `curl -X POST .../v1/admin/export` instead, or stop it first."
        ) from exc


def cmd_curate(args: argparse.Namespace) -> int:
    cfg = config_from_args(args)
    store = _open_store_or_explain(cfg, read_only=True)
    for table, path in store.export_curated(cfg.curated_dir).items():
        print(f"{table}: {path}")
    store.close()
    return 0


def cmd_rebuild(args: argparse.Namespace) -> int:
    cfg = config_from_args(args)
    if cfg.db_path.exists():
        backup = cfg.db_path.with_suffix(f".{utcnow():%Y%m%dT%H%M%S}.bak")
        shutil.move(cfg.db_path, backup)
        print(f"moved existing database to {backup}")
    store = _open_store_or_explain(cfg, read_only=False)
    batches = records = 0
    for path, payload in iter_raw_batches(cfg.raw_dir):
        envelope = payload["envelope"]
        try:
            validate_envelope(envelope)
        except BadRequest as exc:
            print(f"skipping {path}: {exc}", file=sys.stderr)
            continue
        if store.has_batch(envelope["batch_id"]):
            continue
        received_at = parse_ts(payload.get("received_at")) or utcnow()
        store.apply_envelope(envelope, path, received_at)
        batches += 1
        records += len(envelope["records"])
    print(f"replayed {batches} batches ({records} records)")
    for table, path in store.export_curated(cfg.curated_dir).items():
        print(f"{table}: {path}")
    store.close()
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    cfg = config_from_args(args)
    store = _open_store_or_explain(cfg, read_only=True)
    print(json.dumps(to_jsonable(store.stats()), indent=2))
    store.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--db",
            help="DuckDB path (env AMAZON_SYNC_DB; default ~/.local/share/datalake/amazon/amazon.duckdb)",
        )
        p.add_argument(
            "--raw-dir",
            help="Raw batch dir (default $DATALAKE_RAW_ROOT/amazon or ~/datalake.me/raw/amazon)",
        )
        p.add_argument(
            "--curated-dir",
            help="Curated parquet dir (default $DATALAKE_CURATED_ROOT/amazon or ~/datalake.me/curated/amazon)",
        )

    serve = sub.add_parser("serve", help="Run the ingest/query HTTP API")
    common(serve)
    serve.add_argument(
        "--host", default=os.environ.get("AMAZON_SYNC_HOST", "127.0.0.1")
    )
    serve.add_argument(
        "--port", type=int, default=int(os.environ.get("AMAZON_SYNC_PORT", "8765"))
    )
    serve.add_argument(
        "--export-delay",
        type=float,
        default=None,
        help="Seconds to debounce parquet export (default 15)",
    )
    serve.add_argument(
        "--no-auth",
        action="store_true",
        help="Disable the bearer token (tailnet ACLs only)",
    )
    serve.set_defaults(func=cmd_serve)

    for name, func, help_text in (
        ("curate", cmd_curate, "Export curated parquet from the database"),
        ("rebuild", cmd_rebuild, "Recreate the database from raw batches, then export"),
        ("status", cmd_status, "Print table counts and the latest batch"),
    ):
        p = sub.add_parser(name, help=help_text)
        common(p)
        p.set_defaults(func=func)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
