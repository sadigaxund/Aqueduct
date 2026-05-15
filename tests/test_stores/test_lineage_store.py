import pytest

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
    with lineage_store.connect():
        pass
    with lineage_store.connect():
        pass

def test_lineage_store_location_label(lineage_store):
    label = lineage_store.location_label
    assert isinstance(label, str)
    assert len(label) > 0
    if lineage_store.backend == "postgres":
        assert ":" not in label.split("@")[0]  # rough check that password isn't there
