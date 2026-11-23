"""import-export: Amazon privacy export → raw copy + records merged like extension data.

All rows are synthetic; columns match the real "Your Orders" export.
"""

import csv
import gzip
import json
from pathlib import Path

import amazon_orders_server as srv
import duckdb
import pytest

OH_COLS = [
    "ASIN", "Billing Address", "Carrier Name & Tracking Number", "Currency", "Department",
    "Gift Message", "Gift Recipient Contact", "Gift Sender Name", "Item Serial Number",
    "Order Date", "Order ID", "Order Status", "Original Quantity", "Payment Method Type",
    "Product Condition", "Product Name", "Purchase Order Number", "Ship Date",
    "Shipment Item Subtotal", "Shipment Item Subtotal Tax", "Shipment Status",
    "Shipping Address", "Shipping Charge", "Total Amount", "Total Discounts", "Unit Price",
    "Unit Price Tax", "Website",
]
NA = "Not Available"


def oh(order_id, asin, name, unit, tax, total, *, date="2020-01-15T12:00:00Z", ship="2020-01-16T12:00:00Z",
       qty="1", status="Closed", discount="0", shipping="0", website="Amazon.com", currency="USD",
       payment="Visa - 1234"):
    row = dict.fromkeys(OH_COLS, NA)
    row.update({
        "ASIN": asin, "Currency": currency, "Order Date": date, "Order ID": order_id,
        "Order Status": status, "Original Quantity": qty, "Payment Method Type": payment,
        "Product Condition": "New", "Product Name": name, "Ship Date": ship,
        "Shipment Status": "Shipped" if status == "Closed" else NA, "Shipping Charge": shipping,
        "Total Amount": total, "Total Discounts": discount, "Unit Price": unit,
        "Unit Price Tax": tax, "Website": website, "Shipping Address": "Alex Example 1 Main St",
    })
    return row


def write_csv(path: Path, rows: list[dict], cols: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = cols or list(rows[0])
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


@pytest.fixture
def export(tmp_path):
    d = tmp_path / "Your Orders"
    write_csv(d / "Your Amazon Orders/Order History.csv", [
        # two shipments, same ASIN twice, a quoted negative discount
        oh("111-0000001-0000001", "B000000001", "Cable", "10.00", "1.00", "11.00", ship="2020-03-02T00:00:00Z",
           date="2020-03-01T06:00:00Z"),
        oh("111-0000001-0000001", "B000000002", "Lamp", "20.00", "2.00", "17.00", ship="2020-03-02T00:00:00Z",
           date="2020-03-01T06:00:00Z", discount="'-5.00'"),
        oh("111-0000001-0000001", "B000000001", "Cable", "10.00", "1.00", "11.00", ship="2020-03-03T00:00:00Z",
           date="2020-03-01T06:00:00Z"),
        # fully cancelled
        oh("111-0000002-0000002", "B000000003", "Desk", "59.99", "0", "0", status="Cancelled", ship=NA),
        # Whole Foods in-store, late-evening UTC → previous local day
        oh("111-0000003-0000003", "B000000004", "Bananas", "1,000.00", "0", "1,000.00", website="wholefoods.com",
           date="2024-03-02T03:30:00Z"),
        # refunded order with a requested item return
        oh("111-0000004-0000004", "B000000005", "Mouse", "25", "2", "27", date="2020-06-10T12:00:00Z"),
        oh("111-0000004-0000004", "B000000006", "Pad", "5", "0", "5", date="2020-06-10T12:00:00Z"),
    ], OH_COLS)
    write_csv(d / "Your Amazon Orders/Digital Content Orders.csv", [
        {"ASIN": "B0DIGITAL1", "Component Type": "Price Amount", "Order ID": "D01-0000001-0000001",
         "Order Date": "2020-09-01T00:00:00Z", "Order Status": "SUCCESS", "Price": "20.00",
         "Price Currency Code": "USD", "Product Name": "Test Audiobook", "Quantity Ordered": "1",
         "Transaction Amount": "20.00", "Offer Type Code": "Not Applicable", "Marketplace": "www.audible.com",
         "Seller of Record": "Audible", "Digital Order Item ID": "X1", "Item Fulfilled": "Yes",
         "Payment Information": NA},
        {"ASIN": "B0DIGITAL1", "Component Type": "Price Amount", "Order ID": "D01-0000001-0000001",
         "Order Date": "2020-09-01T00:00:00Z", "Order Status": "SUCCESS", "Price": "20.00",
         "Price Currency Code": "USD", "Product Name": "Test Audiobook", "Quantity Ordered": "1",
         "Transaction Amount": "-20.00", "Offer Type Code": "Promotion", "Marketplace": "www.audible.com",
         "Seller of Record": "Audible", "Digital Order Item ID": "X1", "Item Fulfilled": "Yes",
         "Payment Information": NA},
        {"ASIN": "B0DIGITAL1", "Component Type": "Tax", "Order ID": "D01-0000001-0000001",
         "Order Date": "2020-09-01T00:00:00Z", "Order Status": "SUCCESS", "Price": "20.00",
         "Price Currency Code": "USD", "Product Name": "Test Audiobook", "Quantity Ordered": "1",
         "Transaction Amount": "0.0", "Offer Type Code": "Not Applicable", "Marketplace": "www.audible.com",
         "Seller of Record": "Audible", "Digital Order Item ID": "X1", "Item Fulfilled": "Yes",
         "Payment Information": NA},
    ])
    write_csv(d / "Your Returns & Refunds/Returns Status.csv", [
        {"Order ID": "111-0000004-0000004", "Return Resolution": "Refund", "Return Receivable State": "Closed",
         "Return Reason": "Changed mind", "Date of Return": "2020-06-12T00:00:00Z",
         "Return Creation Date": "2020-06-12T00:00:00Z", "Return Amount": "27", "Return Amount Currency": "USD"},
    ])
    write_csv(d / "Your Returns & Refunds/Refund Details.csv", [
        {"Order ID": "111-0000004-0000004", "Disbursement Type": "Refund", "Refund Amount": "27",
         "Refund Date": "2020-06-15T20:00:00Z", "Currency": "USD", "Creation Date": "2020-06-15T00:00:00Z"},
    ])
    write_csv(d / "Your Returns & Refunds/Return Requests.csv", [
        {"ASIN": "B000000005", "Order ID": "111-0000004-0000004", "Product Name": "Mouse",
         "Return Reason Code": "CR-TEST", "Return Shipping Method": NA},
    ])
    (d / "Additional Data/Retail.TransactionalInvoicing.1").mkdir(parents=True)
    (d / "Additional Data/Retail.TransactionalInvoicing.1/Retail.TransactionalInvoicing.1.pdf").write_bytes(b"%PDF-1.4 fake")
    return d


@pytest.fixture
def paths(tmp_path):
    root = tmp_path / "lake"
    return {"db": root / "db" / "amazon.duckdb", "raw": root / "raw" / "amazon", "curated": root / "curated" / "amazon"}


def run(export, paths, *extra):
    return srv.main([
        "import-export", str(export), "--db", str(paths["db"]), "--raw-dir", str(paths["raw"]),
        "--curated-dir", str(paths["curated"]), "--exported-at", "2026-11-21T00:00:00Z", *extra,
    ])


def read(paths, table, where=""):
    con = duckdb.connect()
    rel = con.sql(f"SELECT * FROM '{paths['curated'] / (table + '.parquet')}' {where}")
    cols = rel.columns
    return [dict(zip(cols, r, strict=True)) for r in rel.fetchall()]


def test_dry_run_writes_nothing(export, paths, capsys):
    assert run(export, paths, "--dry-run") == 0
    out = json.loads(capsys.readouterr().out)
    assert out["retail_orders"] == 4 and out["digital_orders"] == 1 and out["returns"] == 1
    assert not paths["db"].parent.exists() and not paths["raw"].exists()


def test_import_maps_orders_items_and_returns(export, paths):
    assert run(export, paths) == 0
    orders = {o["order_id"]: o for o in read(paths, "orders")}
    assert len(orders) == 5

    multi = orders["111-0000001-0000001"]
    assert str(multi["order_date"]) == "2020-02-29"  # 06:00Z is the previous evening in Los Angeles
    assert float(multi["grand_total"]) == 39.0
    assert float(multi["subtotal"]) == 40.0 and float(multi["promotion"]) == -5.0
    assert multi["payment_method"] == "Visa" and multi["payment_last4"] == "1234"
    assert multi["marketplace"] == "www.amazon.com"
    assert multi["ship_to_name"] is None, "export addresses aren't split reliably; order details supply them"

    items = read(paths, "order_items", "WHERE order_id = '111-0000001-0000001' ORDER BY item_key")
    assert [i["item_key"] for i in items] == ["B000000001", "B000000001#2", "B000000002"]

    cancelled = orders["111-0000002-0000002"]
    assert cancelled["is_cancelled"] is True and cancelled["grand_total"] is None

    wf = orders["111-0000003-0000003"]
    assert wf["marketplace"] == "wholefoods.com" and str(wf["order_date"]) == "2024-03-01"
    assert float(wf["grand_total"]) == 1000.0

    digital = orders["D01-0000001-0000001"]
    assert digital["is_digital"] is True and float(digital["grand_total"]) == 0.0
    assert float(digital["promotion"]) == -20.0 and digital["marketplace"] == "www.audible.com"

    assert float(orders["111-0000004-0000004"]["refund_total"]) == 27.0
    (ret,) = read(paths, "returns")
    assert ret["return_key"] == "111-0000004-0000004:B000000005", "same key the extension uses"
    assert ret["status"] == "refunded" and float(ret["refund_amount"]) == 27.0
    assert str(ret["event_date"]) == "2020-06-15"


def test_raw_copy_is_immutable_and_rerun_is_a_no_op(export, paths, capsys):
    assert run(export, paths) == 0
    first = json.loads(capsys.readouterr().out)
    dest = paths["raw"] / "exports" / "2026-11-21"
    assert first["raw_copy"] == "copied"
    manifest = json.loads((dest / "MANIFEST.json").read_text())["files"]
    assert "Your Amazon Orders/Order History.csv" in manifest
    assert (dest / "Additional Data/Retail.TransactionalInvoicing.1/Retail.TransactionalInvoicing.1.pdf").exists()
    batches = list((paths["raw"] / "batches").rglob("*.json.gz"))
    assert len(batches) == 1
    with gzip.open(batches[0], "rt") as fh:
        assert json.load(fh)["envelope"]["client"]["browser"] == "privacy_export"

    assert run(export, paths) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["raw_copy"] == "unchanged" and second["batch"] == "already imported"
    assert len(list((paths["raw"] / "batches").rglob("*.json.gz"))) == 1

    (export / "Your Returns & Refunds/Return Requests.csv").write_text("ASIN,Order ID\n")
    with pytest.raises(SystemExit, match="immutable"):
        run(export, paths)


def test_newer_extension_observations_win(export, paths):
    cfg = srv.Config(raw_dir=paths["raw"], curated_dir=paths["curated"], db_path=paths["db"], export_delay_s=0)
    store = srv.Store(paths["db"])
    srv.Ingestor(cfg, store).ingest({
        "source": "amazon", "schema_version": 1, "batch_id": "ext-1",
        "records": [{
            "type": "order", "key": "111-0000004-0000004", "source": "order_detail",
            "observed_at": "2026-11-22T00:00:00Z", "marketplace": "www.amazon.com",
            "data": {"order_id": "111-0000004-0000004", "status": "Return complete",
                     "ship_to_name": "Alex Example",
                     "items": [{"item_key": "B000000005", "asin": "B000000005", "title": "Wireless Mouse"}]},
        }],
    })
    store.close()
    assert run(export, paths) == 0
    (order,) = read(paths, "orders", "WHERE order_id = '111-0000004-0000004'")
    assert order["status"] == "Return complete", "newer extension value kept"
    assert order["ship_to_name"] == "Alex Example" and order["has_detail"] is True
    assert float(order["grand_total"]) == 32.0, "export fills fields the extension never saw"
    (item,) = read(paths, "order_items", "WHERE item_key = 'B000000005'")
    assert item["title"] == "Wireless Mouse" and float(item["unit_price"]) == 25.0


def test_rebuild_replays_the_import(export, paths):
    assert run(export, paths) == 0
    before = read(paths, "orders")
    assert srv.main(["rebuild", "--db", str(paths["db"]), "--raw-dir", str(paths["raw"]),
                     "--curated-dir", str(paths["curated"])]) == 0
    assert read(paths, "orders") == before


def test_without_explicit_paths_the_lake_is_verified_first(export, tmp_path, monkeypatch):
    import datalake_paths as dp

    dp._verified.clear()
    missing = tmp_path / "unplugged" / "datalake.me"
    monkeypatch.setenv("DATALAKE_ROOT", str(missing))
    with pytest.raises(SystemExit):
        srv.main(["import-export", str(export), "--exported-at", "2026-11-21T00:00:00Z"])
    assert not missing.exists()
    dp._verified.clear()
