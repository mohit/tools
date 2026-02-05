from datetime import datetime, timezone

from location_pipeline.database import init_db
from location_pipeline.runner import run_with_audit
from location_pipeline.sources.base import VisitRecord


def test_multiple_sources_share_single_db(duckdb_conn, monkeypatch) -> None:
    init_db(duckdb_conn)

    def fake_manual_csv(_path: str) -> list[VisitRecord]:
        return [
            VisitRecord(
                visit_id="manual-1",
                source_name="manual_csv",
                started_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
                ended_at=None,
                lat=37.78,
                lon=-122.41,
                place_name="Manual Cafe",
                place_id="manual-place",
                list_name=None,
                confidence=None,
                payload={"source": "manual"},
            )
        ]

    def fake_foursquare_export(_path: str) -> list[VisitRecord]:
        return [
            VisitRecord(
                visit_id="fs-1",
                source_name="foursquare_export",
                started_at=datetime(2024, 2, 1, tzinfo=timezone.utc),
                ended_at=None,
                lat=40.71,
                lon=-74.0,
                place_name="FS Deli",
                place_id="fs-place",
                list_name=None,
                confidence=None,
                payload={"source": "fs"},
            )
        ]

    monkeypatch.setattr(
        "location_pipeline.runner.load_manual_csv",
        fake_manual_csv,
    )
    monkeypatch.setattr(
        "location_pipeline.runner.load_foursquare_export",
        fake_foursquare_export,
    )

    run_with_audit(duckdb_conn, "manual_csv", {"path": "ignored"})
    run_with_audit(duckdb_conn, "foursquare_export", {"path": "ignored"})

    visit_count = duckdb_conn.execute("select count(*) from visits").fetchone()[0]
    place_count = duckdb_conn.execute("select count(*) from place_dim").fetchone()[0]

    assert visit_count == 2
    assert place_count == 2
