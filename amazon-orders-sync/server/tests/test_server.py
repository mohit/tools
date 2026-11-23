import datetime as dt
import gzip
import itertools
import json
from decimal import Decimal

import amazon_orders_server as srv
import duckdb
import pytest

TOKEN = "test-token"


@pytest.fixture
def cfg(tmp_path):
    return srv.Config(
        raw_dir=tmp_path / "raw" / "amazon",
        curated_dir=tmp_path / "curated" / "amazon",
        db_path=tmp_path / "state" / "amazon.duckdb",
        token=TOKEN,
        allowed_tailscale_users=frozenset(),
        export_delay_s=0,
    )


@pytest.fixture
def app(cfg):
    application = srv.create_app(cfg)
    yield application
    application.extensions["amazon_sync"]["store"].close()


@pytest.fixture
def client(app):
    return app.test_client()


def auth(extra=None):
    return {"Authorization": f"Bearer {TOKEN}", **(extra or {})}


def order_record(
    order_id="112-1234567-7654321",
    source="order_list",
    observed_at="2025-11-20T12:00:00Z",
    **data,
):
    base = {
        "order_id": order_id,
        "order_date": "2025-11-03",
        "total_amount": 1042.17,
        "currency": "USD",
        "status": "Delivered November 5",
        "items": [
            {
                "item_key": "B0ABCDEF12",
                "asin": "B0ABCDEF12",
                "title": "USB-C Cable",
                "quantity": 2,
            },
            {
                "item_key": "B09ZZZZZZ9",
                "asin": "B09ZZZZZZ9",
                "title": "Desk Frame",
                "quantity": 1,
            },
        ],
    }
    base.update(data)
    return {
        "type": "order",
        "key": order_id,
        "source": source,
        "source_url": "https://www.amazon.com/your-orders/orders",
        "marketplace": "www.amazon.com",
        "observed_at": observed_at,
        "data": base,
        "raw_html": "<div class='order-card'>…</div>",
    }


def return_record(key="113-7777777-1111111:B07RETURN1", **data):
    order_id, item_key = key.split(":")
    base = {
        "return_key": key,
        "order_id": order_id,
        "item_key": None if item_key == "order" else item_key,
        "asin": None if item_key == "order" else item_key,
        "status": "refunded",
        "refund_amount": 24.99,
        "currency": "USD",
        "event_date": "2025-10-20",
    }
    base.update(data)
    return {
        "type": "return",
        "key": key,
        "source": "order_detail",
        "observed_at": "2025-11-20T12:00:00Z",
        "data": base,
    }


def envelope(records, batch_id="b-1"):
    return {
        "schema_version": 1,
        "source": "amazon",
        "batch_id": batch_id,
        "sent_at": "2025-11-20T12:00:01Z",
        "client": {
            "browser": "safari",
            "extension_version": "0.1.0",
            "install_id": "abcd1234",
        },
        "records": records,
    }


def post(client, env, gz=False):
    body = json.dumps(env).encode()
    headers = auth({"Content-Type": "application/json"})
    if gz:
        body = gzip.compress(body)
        headers["Content-Encoding"] = "gzip"
    return client.post("/v1/ingest", data=body, headers=headers)


def test_requires_token_unless_no_auth(cfg):
    cfg.token = None
    with pytest.raises(SystemExit):
        srv.create_app(cfg)


def test_health_is_public_and_cors(client):
    res = client.get("/v1/health")
    assert res.status_code == 200
    assert res.json["service"] == "amazon-orders-sync"
    assert res.headers["Access-Control-Allow-Origin"] == "*"
    pre = client.options("/v1/ingest")
    assert pre.status_code in (200, 204)
    assert "Authorization" in pre.headers["Access-Control-Allow-Headers"]
    assert pre.headers["Access-Control-Allow-Private-Network"] == "true"


def test_auth_required(client):
    assert client.post("/v1/ingest", json=envelope([order_record()])).status_code == 401
    bad = client.post(
        "/v1/ingest",
        json=envelope([order_record()]),
        headers={"Authorization": "Bearer nope"},
    )
    assert bad.status_code == 401
    assert client.get("/v1/orders").status_code == 401


def test_tailscale_identity_allowlist(cfg):
    cfg.allowed_tailscale_users = frozenset({"me@example.com"})
    app = srv.create_app(cfg)
    c = app.test_client()
    assert c.get("/v1/stats", headers=auth()).status_code == 403
    assert (
        c.get(
            "/v1/stats", headers=auth({"Tailscale-User-Login": "me@example.com"})
        ).status_code
        == 200
    )
    app.extensions["amazon_sync"]["store"].close()


def test_ingest_writes_raw_db_and_curated(client, cfg):
    res = post(client, envelope([order_record(), return_record()]), gz=True)
    assert res.status_code == 200, res.json
    assert res.json == {
        "ok": True,
        "duplicate": False,
        "batch_id": "b-1",
        "accepted": 2,
        "orders": 1,
        "order_items": 2,
        "returns": 1,
    }

    raw_files = list((cfg.raw_dir / "batches").rglob("*.json.gz"))
    assert len(raw_files) == 1
    payload = json.loads(gzip.decompress(raw_files[0].read_bytes()))
    assert payload["envelope"]["records"][0]["raw_html"].startswith("<div")

    for table in ("orders", "order_items", "returns"):
        assert (cfg.curated_dir / f"{table}.parquet").exists()
    rows = duckdb.sql(
        f"SELECT order_id, total_amount FROM '{cfg.curated_dir / 'orders.parquet'}'"
    ).fetchall()
    assert rows == [("112-1234567-7654321", Decimal("1042.17"))]

    order = client.get("/v1/orders/112-1234567-7654321", headers=auth()).json
    assert order["order_date"] == "2025-11-03"
    assert order["marketplace"] == "www.amazon.com"
    assert [i["item_key"] for i in order["items"]] == ["B09ZZZZZZ9", "B0ABCDEF12"]
    assert order["has_detail"] is None


def test_duplicate_batch_is_idempotent(client, cfg):
    env = envelope([order_record()])
    assert post(client, env).json["duplicate"] is False
    again = post(client, env)
    assert again.status_code == 200
    assert again.json["duplicate"] is True
    assert len(list((cfg.raw_dir / "batches").rglob("*.json.gz"))) == 1


def test_detail_enriches_list_and_null_never_overwrites(client):
    post(client, envelope([order_record()], "b-1"))
    detail = order_record(
        source="order_detail",
        observed_at="2025-11-20T12:05:00Z",
        status="Delivered November 5, 2025",
        grand_total=1042.17,
        tax=22.17,
        payment_method="Visa",
        payment_last4="4242",
        items=[
            {
                "item_key": "B0ABCDEF12",
                "asin": "B0ABCDEF12",
                "title": "USB-C Cable (2-Pack)",
                "unit_price": 10.0,
            }
        ],
    )
    post(client, envelope([detail], "b-2"))
    # A later list observation without the detail-only fields must not erase them.
    post(
        client,
        envelope(
            [order_record(observed_at="2025-11-21T09:00:00Z", status="Delivered")],
            "b-3",
        ),
    )

    order = client.get("/v1/orders/112-1234567-7654321", headers=auth()).json
    assert order["tax"] == 22.17
    assert order["payment_last4"] == "4242"
    assert order["has_detail"] is True
    assert order["status"] == "Delivered"
    assert order["last_source"] == "order_list"
    assert order["last_detail_at"] == "2025-11-20T12:05:00Z"
    cable = next(i for i in order["items"] if i["item_key"] == "B0ABCDEF12")
    assert cable["unit_price"] == 10.0
    assert cable["quantity"] == 2
    assert (
        cable["title"] == "USB-C Cable"
    )  # newest observation (list) wins for non-null values


def test_out_of_order_batches_converge(client):
    newer = order_record(observed_at="2025-11-22T00:00:00Z", status="Return complete")
    older = order_record(observed_at="2025-11-01T00:00:00Z", status="Shipped")
    post(client, envelope([newer], "b-new"))
    post(client, envelope([older], "b-old"))  # cache flushed late
    order = client.get("/v1/orders/112-1234567-7654321", headers=auth()).json
    assert order["status"] == "Return complete"
    assert order["first_observed_at"] == "2025-11-01T00:00:00Z"
    assert order["last_observed_at"] == "2025-11-22T00:00:00Z"


def test_item_return_replaces_order_level_placeholder(client):
    post(client, envelope([return_record("113-7777777-1111111:order")], "b-1"))
    post(client, envelope([return_record("113-7777777-1111111:B07RETURN1")], "b-2"))
    rows = client.get("/v1/returns", headers=auth()).json["rows"]
    assert [r["return_key"] for r in rows] == ["113-7777777-1111111:B07RETURN1"]
    assert rows[0]["refund_amount"] == 24.99


@pytest.mark.parametrize(
    "mutate,message",
    [
        (lambda e: e.update(schema_version=2), "schema_version"),
        (lambda e: e.update(source="ebay"), "source"),
        (lambda e: e.update(batch_id="../etc/passwd"), "batch_id"),
        (lambda e: e.update(records=[]), "records"),
        (lambda e: e["records"][0].update(type="nope"), "type"),
        (lambda e: e["records"][0].update(key="other"), "order key"),
    ],
)
def test_validation(client, cfg, mutate, message):
    env = envelope([order_record()])
    mutate(env)
    res = post(client, env)
    assert res.status_code == 400
    assert message in res.json["error"]
    assert not (cfg.raw_dir / "batches").exists()


def test_bad_gzip_and_json(client):
    res = client.post(
        "/v1/ingest", data=b"not gzip", headers=auth({"Content-Encoding": "gzip"})
    )
    assert res.status_code == 400
    res = client.post("/v1/ingest", data=b"{nope", headers=auth())
    assert res.status_code == 400


def test_list_filters_and_stats(client):
    post(
        client,
        envelope(
            [
                order_record("111-0000000-0000001", order_date="2024-01-05"),
                order_record("111-0000000-0000002", order_date="2025-06-01"),
                return_record(),
            ]
        ),
    )
    rows = client.get("/v1/orders?since=2025-01-01", headers=auth()).json["rows"]
    assert [r["order_id"] for r in rows] == ["111-0000000-0000002"]
    items = client.get("/v1/items?asin=B09ZZZZZZ9", headers=auth()).json["rows"]
    assert {i["order_id"] for i in items} == {
        "111-0000000-0000001",
        "111-0000000-0000002",
    }
    items_since = client.get("/v1/items?since=2025-01-01", headers=auth()).json["rows"]
    assert {i["order_id"] for i in items_since} == {"111-0000000-0000002"}
    stats = client.get("/v1/stats", headers=auth()).json
    assert stats["tables"] == {
        "orders": 2,
        "order_items": 4,
        "returns": 1,
        "ingest_batches": 1,
    }
    assert stats["latest_order_date"] == "2025-06-01"
    assert stats["last_batch"]["client_browser"] == "safari"
    assert client.get("/v1/orders?limit=abc", headers=auth()).status_code == 400
    assert client.get("/v1/orders/nope", headers=auth()).status_code == 404


def test_rebuild_from_raw_matches_live_db(client, app, cfg):
    post(client, envelope([order_record()], "b-1"))
    post(
        client,
        envelope(
            [
                order_record(
                    source="order_detail", tax=1.5, observed_at="2025-11-21T00:00:00Z"
                )
            ],
            "b-2",
        ),
    )
    post(client, envelope([return_record()], "b-3"))
    live = app.extensions["amazon_sync"]["store"].order_bundle("112-1234567-7654321")
    app.extensions["amazon_sync"]["store"].close()

    assert (
        srv.main(
            [
                "rebuild",
                "--db",
                str(cfg.db_path),
                "--raw-dir",
                str(cfg.raw_dir),
                "--curated-dir",
                str(cfg.curated_dir),
            ]
        )
        == 0
    )
    assert list(cfg.db_path.parent.glob("*.bak"))
    rebuilt_store = srv.Store(cfg.db_path, read_only=True)
    rebuilt = rebuilt_store.order_bundle("112-1234567-7654321")
    stats = rebuilt_store.stats()
    rebuilt_store.close()
    assert rebuilt == live
    assert stats["tables"]["ingest_batches"] == 3


def test_merge_row_semantics():
    t1 = dt.datetime(2025, 1, 1)
    t2 = dt.datetime(2025, 1, 2)
    row = srv.merge_row(None, {"a": 1, "b": None}, t2, "order_list")
    assert {k: v for k, v in row.items() if k != srv.FIELD_TS} == {
        "a": 1,
        "b": None,
        "first_observed_at": t2,
        "last_observed_at": t2,
        "last_source": "order_list",
    }
    assert json.loads(row[srv.FIELD_TS]) == {"a": t2.isoformat()}
    older = srv.merge_row(row, {"a": 0, "b": 5}, t1, "order_detail")
    assert older["a"] == 1  # stale value does not win
    assert older["b"] == 5  # but fills a gap
    assert older["first_observed_at"] == t1
    assert older["last_source"] == "order_list"


def test_merge_is_order_independent_per_field():
    """Review finding: a sparse newer observation must not block a newer value for a field it
    didn't supply (detail t1 refund=0, list t3 no refund, detail t2 refund=25 → 25)."""
    t1, t2, t3 = (dt.datetime(2025, 1, d) for d in (1, 2, 3))
    obs = [
        ({"refund_total": 0, "status": "Delivered"}, t1, "order_detail"),
        ({"refund_total": None, "status": "Return complete"}, t3, "order_list"),
        ({"refund_total": 25, "status": "Return started"}, t2, "order_detail"),
    ]
    results = set()
    for perm in itertools.permutations(obs):
        row = None
        for data, ts, source in perm:
            row = srv.merge_row(row, dict(data), ts, source)
        results.add((row["refund_total"], row["status"], row["last_source"]))
    assert results == {(25, "Return complete", "order_list")}


def test_order_refund_placeholder_after_item_return_is_suppressed(client):
    """Review finding: placeholder arriving after the item-level return must not be stored."""
    post(client, envelope([return_record("113-7777777-1111111:B07RETURN1")], "b-1"))
    post(client, envelope([return_record("113-7777777-1111111:order")], "b-2"))
    rows = client.get("/v1/returns", headers=auth()).json["rows"]
    assert [r["return_key"] for r in rows] == ["113-7777777-1111111:B07RETURN1"]


def test_field_timestamps_not_exposed(client, cfg):
    post(client, envelope([order_record(), return_record()]))
    order = client.get("/v1/orders/112-1234567-7654321", headers=auth()).json
    assert srv.FIELD_TS not in order
    assert all(srv.FIELD_TS not in i for i in order["items"])
    assert srv.FIELD_TS not in client.get("/v1/returns", headers=auth()).json["rows"][0]
    cols = duckdb.sql(
        f"DESCRIBE SELECT * FROM '{cfg.curated_dir / 'orders.parquet'}'"
    ).fetchall()
    assert srv.FIELD_TS not in {c[0] for c in cols}


def test_schema_migration_adds_new_columns(tmp_path):
    db = tmp_path / "old.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE orders (order_id VARCHAR PRIMARY KEY, status VARCHAR)")
    con.close()
    store = srv.Store(db)
    cols = {r[0] for r in store.con.execute("DESCRIBE orders").fetchall()}
    store.close()
    assert set(srv.ORDER_COLUMNS) <= cols


def test_coerce():
    assert srv.coerce("2025-11-03", "DATE") == dt.date(2025, 11, 3)
    assert srv.coerce("garbage", "DATE") is None
    assert srv.coerce(10.5, "DECIMAL(12,2)") == Decimal("10.50")
    assert srv.coerce(True, "INTEGER") is None
    assert srv.coerce("x", "BOOLEAN") is None
    assert srv.parse_ts("2025-11-20T12:00:00.000Z") == dt.datetime(2025, 11, 20, 12, 0)
