import pytest
from pathlib import Path
from datetime import datetime, timezone
from aqueduct.parser.parser import parse
from aqueduct.compiler.compiler import compile as compiler_compile

pytestmark = pytest.mark.unit

@pytest.fixture
def bp_path(tmp_path):
    return tmp_path / "blueprint.yml"

def test_compile_inputs_fingerprint_local(tmp_path, bp_path):
    """local Ingress path -> inputs_fingerprint[module_id] has size_bytes int and ISO-8601 last_modified."""
    in_file = tmp_path / "input.parquet"
    in_file.write_text("data")
    
    bp_content = f"""
aqueduct: "1.0"
id: test_bp
name: Test Blueprint
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: {in_file}
  - id: out
    type: Egress
    label: OUT
    config:
      format: parquet
      path: {tmp_path / 'out'}
edges:
  - from: m1
    to: out
"""
    bp_path.write_text(bp_content)
    
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp, blueprint_path=bp_path)
    
    assert "m1" in manifest.inputs_fingerprint
    fp = manifest.inputs_fingerprint["m1"]
    assert fp["size_bytes"] == 4
    assert isinstance(fp["last_modified"], str)
    # Check ISO-8601 format roughly
    datetime.fromisoformat(fp["last_modified"])
    
    # non-Ingress modules not in inputs_fingerprint
    assert "out" not in manifest.inputs_fingerprint

def test_compile_inputs_fingerprint_remote(tmp_path, bp_path):
    """remote Ingress path (s3a://...) -> inputs_fingerprint[module_id] has size_bytes=None, last_modified=None."""
    bp_content = """
aqueduct: "1.0"
id: test_bp
name: Test Blueprint
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: s3a://bucket/data.parquet
"""
    bp_path.write_text(bp_content)
    
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp, blueprint_path=bp_path)
    
    fp = manifest.inputs_fingerprint["m1"]
    assert fp["size_bytes"] is None
    assert fp["last_modified"] is None

def test_compile_inputs_fingerprint_skip_formats(tmp_path, bp_path):
    """format=jdbc Ingress -> fingerprint entry has size_bytes=None (skip stat)."""
    bp_content = """
aqueduct: "1.0"
id: test_bp
name: Test Blueprint
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: jdbc
      path: "some_table"
"""
    bp_path.write_text(bp_content)
    
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp, blueprint_path=bp_path)
    
    fp = manifest.inputs_fingerprint["m1"]
    assert fp["size_bytes"] is None
    assert fp["last_modified"] is None

def test_compile_inputs_fingerprint_not_exists(tmp_path, bp_path):
    """path does not exist (OSError) -> fingerprint entry has size_bytes=None."""
    bp_content = """
aqueduct: "1.0"
id: test_bp
name: Test Blueprint
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: /tmp/ghost_file_12345.parquet
"""
    bp_path.write_text(bp_content)
    
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp, blueprint_path=bp_path)
    
    fp = manifest.inputs_fingerprint["m1"]
    assert fp["size_bytes"] is None
    assert fp["last_modified"] is None

def test_manifest_to_dict_includes_fingerprint(tmp_path, bp_path):
    """Manifest.to_dict() includes inputs_fingerprint key."""
    bp_content = """
aqueduct: "1.0"
id: test_bp
name: Test Blueprint
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: data.parquet
"""
    bp_path.write_text(bp_content)
    
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp, blueprint_path=bp_path)
    
    d = manifest.to_dict()
    assert "inputs_fingerprint" in d
    assert d["inputs_fingerprint"]["m1"]["path"] == "data.parquet"
