from location_pipeline.database import init_db


def test_schema_includes_behavior_tables(duckdb_conn) -> None:
    init_db(duckdb_conn)
    tables = {row[0] for row in duckdb_conn.execute("show tables").fetchall()}
    assert "visits" in tables
    assert "saved_places" in tables
    assert "place_reviews" in tables


def test_behavior_tables_are_separate(duckdb_conn) -> None:
    init_db(duckdb_conn)
    duckdb_conn.execute("insert into visits (visit_id, source_name) values ('v1', 'test')")
    duckdb_conn.execute("insert into saved_places (saved_id, source_name) values ('s1', 'test')")
    duckdb_conn.execute("insert into place_reviews (review_id, source_name) values ('r1', 'test')")

    assert duckdb_conn.execute("select count(*) from visits").fetchone()[0] == 1
    assert duckdb_conn.execute("select count(*) from saved_places").fetchone()[0] == 1
    assert duckdb_conn.execute("select count(*) from place_reviews").fetchone()[0] == 1
