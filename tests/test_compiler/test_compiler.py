import pytest
from pathlib import Path
from datetime import datetime, timezone
from aqueduct.errors import CompileError
from aqueduct.parser.parser import parse
from aqueduct.compiler.compiler import compile as compiler_compile

pytestmark = pytest.mark.unit


def test_probe_wired_as_data_source_raises_compile_error(tmp_path):
    """A Probe used as a main-port edge source used to reach the executor's
    topo-sort (Probes are excluded from its node set) and crash with a bare
    KeyError. The compiler must catch it with an actionable message instead."""
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text("""
aqueduct: "1.0"
id: test_bp
name: Test Blueprint
modules:
  - id: session_metrics
    type: Ingress
    label: Session Metrics
    config:
      format: parquet
      path: data.parquet
  - id: probe_signal_gate
    type: Probe
    label: Probe Gate
    attach_to: session_metrics
    config:
      signals:
        - type: row_count_estimate
  - id: quality_gate
    type: Regulator
    label: Quality Gate
    config: {}
edges:
  - from: session_metrics
    to: probe_signal_gate
  - from: probe_signal_gate
    to: quality_gate
""")
    bp = parse(str(bp_path))
    with pytest.raises(CompileError, match=r"cannot be a data source"):
        compiler_compile(bp, blueprint_path=bp_path)


def test_base_dir_round_trips_from_parse_through_compile(tmp_path):
    """Blueprint.base_dir (the blueprint YAML's own directory) must survive
    into Manifest.base_dir — executor-side user-code import sites (custom
    Assert fn:, Probe module:+entry:, python UDFs, format: custom) rely on
    it to resolve sibling .py files, since the `aqueduct` console script
    never puts the blueprint's dir on sys.path."""
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text("""
aqueduct: "1.0"
id: test_bp
name: Test Blueprint
modules:
  - id: ingress
    type: Ingress
    label: Ingress
    config:
      format: parquet
      path: data.parquet
""")
    bp = parse(str(bp_path))
    assert bp.base_dir == str(tmp_path.resolve())

    manifest = compiler_compile(bp, blueprint_path=bp_path)
    assert manifest.base_dir == str(tmp_path.resolve())
    assert manifest.to_dict()["base_dir"] == str(tmp_path.resolve())


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
    assert d["inputs_fingerprint"]["m1"]["path"] == str(bp_path.parent / "data.parquet")

def test_compile_blueprint_path_none_builds_provenance_map(tmp_path):
    yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: in
    type: Ingress
    label: IN
    config:
      format: parquet
      path: data.parquet
    """
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(yaml_str)
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp, blueprint_path=None)
    assert manifest.provenance_map is not None
    assert manifest.provenance_map.blueprint_path == ""

def test_compile_inputs_fingerprint_arcade_expanded_local(tmp_path, bp_path):
    """
    compile(): Arcade-expanded Ingress (sub-blueprint Ingress namespaced as {arcade_id}__{child_id})
    with a local path -> fingerprint entry exists keyed by the expanded ID with stat fields populated.
    """
    # 1. Create sub-blueprint
    sub_bp_path = tmp_path / "sub.yml"
    in_file = tmp_path / "input.parquet"
    in_file.write_text("arcade_data")
    
    sub_bp_path.write_text(f"""
aqueduct: "1.0"
id: sub_bp
name: Sub Blueprint
modules:
  - id: child_in
    type: Ingress
    label: Child IN
    config:
      format: parquet
      path: {in_file}
""")

    # 2. Create main blueprint with Arcade
    bp_path.write_text(f"""
aqueduct: "1.0"
id: main_bp
name: Main Blueprint
modules:
  - id: arc
    type: Arcade
    label: Arcade Mod
    ref: {sub_bp_path.name}
""")
    
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp, blueprint_path=bp_path)
    
    # Expected namespaced ID: arc__child_in
    assert "arc__child_in" in manifest.inputs_fingerprint
    fp = manifest.inputs_fingerprint["arc__child_in"]
    assert fp["size_bytes"] == len("arcade_data")
    assert isinstance(fp["last_modified"], str)

def test_compile_inputs_fingerprint_arcade_expanded_remote(tmp_path, bp_path):
    """
    compile(): Arcade-expanded Ingress with remote path -> fingerprint entry exists
    keyed by expanded ID with size_bytes=None, last_modified=None.
    """
    sub_bp_path = tmp_path / "sub_remote.yml"
    sub_bp_path.write_text("""
aqueduct: "1.0"
id: sub_bp
name: Sub Remote
modules:
  - id: child_in
    type: Ingress
    label: Child IN
    config:
      format: parquet
      path: s3a://bucket/arcade_data.parquet
""")

    bp_path.write_text(f"""
aqueduct: "1.0"
id: main_bp
name: Main Remote
modules:
  - id: arc
    type: Arcade
    label: Arcade Mod
    ref: {sub_bp_path.name}
""")
    
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp, blueprint_path=bp_path)
    
    assert "arc__child_in" in manifest.inputs_fingerprint
    fp = manifest.inputs_fingerprint["arc__child_in"]
    assert fp["size_bytes"] is None
    assert fp["last_modified"] is None
