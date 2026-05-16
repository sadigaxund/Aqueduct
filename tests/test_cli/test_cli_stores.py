import pytest
from click.testing import CliRunner
from aqueduct.cli import cli

pytestmark = pytest.mark.integration

def test_cli_stores_info(tmp_path):
    runner = CliRunner()
    config = tmp_path / "aq.yml"
    config.write_text("aqueduct_config: '1.0'")
    result = runner.invoke(cli, ["stores", "info", "--config", str(config)])
    assert result.exit_code == 0
    assert "observability" in result.output
    assert "lineage" in result.output
    assert "depot" in result.output

def test_cli_stores_migrate_empty(tmp_path):
    runner = CliRunner()
    config = tmp_path / "aq.yml"
    config.write_text("aqueduct_config: '1.0'")
    empty_db = tmp_path / "empty.db"
    # Touch the file to ensure it exists
    import duckdb
    conn = duckdb.connect(str(empty_db))
    conn.execute("CREATE TABLE depot_kv (key VARCHAR, value BLOB, updated_at TIMESTAMP)")
    conn.close()
    
    result = runner.invoke(cli, ["stores", "migrate", "--from-duckdb", str(empty_db), "--store", "depot", "--config", str(config)])
    assert result.exit_code == 0
    assert "0 rows" in result.output

def test_cli_stores_migrate_populated(tmp_path):
    runner = CliRunner()
    config = tmp_path / "aq.yml"
    config.write_text("aqueduct_config: '1.0'")
    pop_db = tmp_path / "pop.db"
    import duckdb
    from datetime import datetime
    conn = duckdb.connect(str(pop_db))
    conn.execute("CREATE TABLE depot_kv (key VARCHAR, value BLOB, updated_at TIMESTAMP)")
    conn.execute("INSERT INTO depot_kv VALUES ('k1', '\\x00', ?)", [datetime.now()])
    conn.close()
    
    result = runner.invoke(cli, ["stores", "migrate", "--from-duckdb", str(pop_db), "--store", "depot", "--config", str(config)])
    assert result.exit_code == 0
    assert "1 depot key(s)" in result.output

def test_cli_stores_migrate_same_file_refused(tmp_path):
    runner = CliRunner()
    config = tmp_path / "aq.yml"
    db_path = tmp_path / "same.db"
    config.write_text(f"aqueduct_config: '1.0'\nstores:\n  depot: {{ path: {str(db_path)} }}")
    
    import duckdb
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE depot_kv (key VARCHAR, value BLOB, updated_at TIMESTAMP)")
    conn.close()
    
    result = runner.invoke(cli, ["stores", "migrate", "--from-duckdb", str(db_path), "--store", "depot", "--config", str(config)])
    assert result.exit_code != 0
    assert "same" in result.output.lower()

def test_cli_stores_migrate_unsupported_store(tmp_path):
    runner = CliRunner()
    config = tmp_path / "aq.yml"
    config.write_text("aqueduct_config: '1.0'")
    db_path = tmp_path / "test.db"
    db_path.touch()
    
    result = runner.invoke(cli, ["stores", "migrate", "--from-duckdb", str(db_path), "--store", "observability", "--config", str(config)])
    assert result.exit_code != 0
    assert "invalid value for '--store'" in result.output.lower()
    assert "depot" in result.output.lower()
