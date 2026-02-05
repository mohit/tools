import duckdb
import pytest


@pytest.fixture
def duckdb_conn():
    conn = duckdb.connect(":memory:")
    try:
        yield conn
    finally:
        conn.close()
