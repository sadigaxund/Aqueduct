import pytest
from pathlib import Path
from aqueduct.parser.parser import parse
from aqueduct.config import load_config

pytestmark = pytest.mark.unit

def test_blueprint_path_resolution_relative(tmp_path):
    # Setup directories
    bp_dir = tmp_path / "blueprints"
    bp_dir.mkdir()
    data_dir = bp_dir / "data" / "input"
    data_dir.mkdir(parents=True)
    
    csv_file = data_dir / "events.csv"
    csv_file.write_text("col1,col2\n1,2")
    
    bp_file = bp_dir / "blueprint.yml"
    bp_file.write_text("""
aqueduct: "1.0"
id: test_path_res
name: Test Path Resolution
modules:
  - id: ingress
    type: Ingress
    label: Ingress
    config:
      format: csv
      path: data/input/events.csv
edges: []
""")
    
    bp = parse(bp_file)
    ingress = next(m for m in bp.modules if m.id == "ingress")
    # Should resolve relative to the blueprint directory (bp_dir)
    expected_path = (bp_dir / "data/input/events.csv").resolve()
    assert ingress.config["path"] == str(expected_path)

def test_blueprint_path_resolution_uri(tmp_path):
    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text("""
aqueduct: "1.0"
id: test_path_res_uri
name: Test Path Resolution URI
modules:
  - id: ingress
    type: Ingress
    label: Ingress
    config:
      format: parquet
      path: s3://bucket/key.parquet
edges: []
""")
    bp = parse(bp_file)
    ingress = next(m for m in bp.modules if m.id == "ingress")
    # URIs should pass through unchanged
    assert ingress.config["path"] == "s3://bucket/key.parquet"

def test_blueprint_path_resolution_absolute(tmp_path):
    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text("""
aqueduct: "1.0"
id: test_path_res_abs
name: Test Path Resolution Absolute
modules:
  - id: ingress
    type: Ingress
    label: Ingress
    config:
      format: csv
      path: /abs/data.csv
edges: []
""")
    bp = parse(bp_file)
    ingress = next(m for m in bp.modules if m.id == "ingress")
    # Absolute paths should pass through unchanged
    assert ingress.config["path"] == "/abs/data.csv"

def test_config_store_path_resolution_relative(tmp_path):
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    cfg_file = cfg_dir / "aqueduct.yml"
    cfg_file.write_text("""
stores:
  observability:
    path: .aqueduct/observability.db
""")
    cfg = load_config(cfg_file)
    expected_path = (cfg_dir / ".aqueduct/observability.db").resolve()
    assert cfg.stores.observability.path == str(expected_path)

def test_module_config_other_keys_resolution(tmp_path):
    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text("""
aqueduct: "1.0"
id: test_other_keys
name: Test Other Keys
modules:
  - id: mod
    type: Channel
    label: Mod
    config:
      data_dir: my_data_dir
      input_dir: my_input_dir
      output_dir: my_output_dir
      jar: my_jar.jar
edges: []
""")
    bp = parse(bp_file)
    mod = next(m for m in bp.modules if m.id == "mod")
    assert mod.config["data_dir"] == str((tmp_path / "my_data_dir").resolve())
    assert mod.config["input_dir"] == str((tmp_path / "my_input_dir").resolve())
    assert mod.config["output_dir"] == str((tmp_path / "my_output_dir").resolve())
    assert mod.config["jar"] == str((tmp_path / "my_jar.jar").resolve())
