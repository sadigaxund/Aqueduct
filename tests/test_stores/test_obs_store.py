import pytest
from pydantic import ValidationError
from aqueduct.config import AqueductConfig

def test_obs_store_roundtrip(obs_store):
    with obs_store.connect() as cur:
        # run_records
        cur.execute("CREATE TABLE IF NOT EXISTS run_records (run_id VARCHAR, val VARCHAR)")
        cur.execute("INSERT INTO run_records VALUES (?, ?)", ["run_1", "ok"])
        assert cur.execute("SELECT val FROM run_records WHERE run_id=?", ["run_1"]).fetchone()[0] == "ok"
        
        # module_metrics
        cur.execute("CREATE TABLE IF NOT EXISTS module_metrics (module_id VARCHAR, val INT)")
        cur.execute("INSERT INTO module_metrics VALUES (?, ?)", ["mod_1", 42])
        assert cur.execute("SELECT val FROM module_metrics WHERE module_id=?", ["mod_1"]).fetchone()[0] == 42
        
        # probe_signals
        cur.execute("CREATE TABLE IF NOT EXISTS probe_signals (probe_id VARCHAR, passed BOOLEAN)")
        cur.execute("INSERT INTO probe_signals VALUES (?, ?)", ["probe_1", True])
        assert cur.execute("SELECT passed FROM probe_signals WHERE probe_id=?", ["probe_1"]).fetchone()[0] is True

def test_obs_store_idempotent_connect(obs_store):
    with obs_store.connect():
        pass
    with obs_store.connect():
        pass

def test_obs_store_location_label(obs_store):
    label = obs_store.location_label
    assert isinstance(label, str)
    assert len(label) > 0
    # Redaction checks if applicable (Postgres passwords)
    if obs_store.backend == "postgres":
        # password must be redacted: userinfo (between // and @) carries no user:pass
        if "@" in label:
            userinfo = label.split("//", 1)[1].split("@", 1)[0]
            assert ":" not in userinfo

def test_redis_obs_rejected():
    with pytest.raises(ValidationError):
        AqueductConfig(**{
            "stores": {
                "observability": {"backend": "redis", "path": "redis://localhost:6379/15"},
                "lineage": {"backend": "duckdb", "path": "lineage.db"},
                "depot": {"backend": "duckdb", "path": "depot.db"}
            }
        })

def test_duckdb_creates_parent_dirs(tmp_path):
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
    nested_path = tmp_path / "deep" / "nested" / "dir" / "observability.db"
    assert not nested_path.parent.exists()
    
    store = DuckDBObservabilityStore(nested_path)
    with store.connect():
        pass
        
    assert nested_path.parent.exists()
    assert nested_path.exists()
