"""Unit tests for DuckDBLineageStore low-level operations.

Phase 38 merged lineage into the observability store. The ``lineage_store``
fixture was removed from conftest; provide a local DuckDBLineageStore.
"""

import pytest
from pathlib import Path

pytestmark = pytest.mark.unit


@pytest.fixture
def lineage_store(tmp_path: Path):
    from aqueduct.stores.duckdb_ import DuckDBLineageStore
    return DuckDBLineageStore(tmp_path / "lineage.db")


def test_lineage_store_roundtrip(lineage_store):
    with lineage_store.connect() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS column_lineage (source VARCHAR, target VARCHAR)")
        
        # Test executemany and param rewriting
        rows = [("a", "b"), ("x", "y")]
        cur.executemany("INSERT INTO column_lineage VALUES (?, ?)", rows)
        
        # Verify
        res = cur.execute("SELECT target FROM column_lineage WHERE source=?", ["a"]).fetchone()
        assert res[0] == "b"
        
        all_rows = cur.execute("SELECT source, target FROM column_lineage ORDER BY source").fetchall()
        assert len(all_rows) == 2
        assert all_rows[0][0] == "a"
        assert all_rows[1][0] == "x"

def test_lineage_store_idempotent_connect(lineage_store):
    with lineage_store.connect() as cur:
        cur.execute("SELECT 1")
    # a second connect on the same store still yields a working cursor
    with lineage_store.connect() as cur:
        assert cur.execute("SELECT 1").fetchone()[0] == 1

def test_lineage_store_location_label(lineage_store):
    label = lineage_store.location_label
    assert isinstance(label, str)
    assert len(label) > 0
    if lineage_store.backend == "postgres":
        # password must be redacted: userinfo (between // and @) carries no user:pass
        if "@" in label:
            userinfo = label.split("//", 1)[1].split("@", 1)[0]
            assert ":" not in userinfo
