
import pytest

from aqueduct.config import load_config
from aqueduct.parser.parser import parse, parse_dict

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
    path: .aqueduct/observability
""")
    cfg = load_config(cfg_file)
    expected_path = (cfg_dir / ".aqueduct/observability").resolve()
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


# ── Phase 36 Part A — parse_dict() API + base_dir anchoring ─────────────────


def _bp_dict(path_value: str) -> dict:
    return {
        "aqueduct": "1.0",
        "id": "test_parse_dict",
        "name": "Test parse_dict",
        "modules": [
            {
                "id": "ingress",
                "type": "Ingress",
                "label": "Ingress",
                "config": {"format": "csv", "path": path_value},
            }
        ],
        "edges": [],
    }


def test_parse_dict_anchors_relative_to_explicit_base_dir(tmp_path):
    """parse_dict() must anchor relative paths to the supplied base_dir,
    not to cwd or to a tempfile location. Regression for the
    tempfile-in-/tmp bug Phase 36 Part A removes."""
    sub_dir = tmp_path / "blueprints" / "sub"
    sub_dir.mkdir(parents=True)
    data_dir = tmp_path / "blueprints" / "data"
    data_dir.mkdir(parents=True)
    (data_dir / "events.csv").write_text("col1\n1\n")

    raw = _bp_dict("../data/events.csv")
    bp = parse_dict(raw, base_dir=sub_dir)
    ingress = next(m for m in bp.modules if m.id == "ingress")
    expected = (sub_dir / "../data/events.csv").resolve()
    assert ingress.config["path"] == str(expected)
    assert ingress.config["path"].startswith(str(tmp_path.resolve()))


def test_parse_dict_uri_passthrough(tmp_path):
    raw = _bp_dict("s3://bucket/key.csv")
    bp = parse_dict(raw, base_dir=tmp_path)
    ingress = next(m for m in bp.modules if m.id == "ingress")
    assert ingress.config["path"] == "s3://bucket/key.csv"


def test_parse_dict_absolute_passthrough(tmp_path):
    raw = _bp_dict("/abs/data.csv")
    bp = parse_dict(raw, base_dir=tmp_path)
    ingress = next(m for m in bp.modules if m.id == "ingress")
    assert ingress.config["path"] == "/abs/data.csv"


def test_parse_dict_matches_parse_for_on_disk_blueprint(tmp_path):
    """parse(path) and parse_dict(load(path), path.parent) must produce
    identical anchored Module configs — that equivalence is the contract
    in-memory patch flows now rely on."""
    import yaml as _yaml

    bp_dir = tmp_path / "sub"
    bp_dir.mkdir()
    bp_file = bp_dir / "blueprint.yml"
    raw = _bp_dict("../data/events.csv")
    bp_file.write_text(_yaml.safe_dump(raw))

    a = parse(bp_file)
    b = parse_dict(_yaml.safe_load(bp_file.read_text()), base_dir=bp_dir)
    a_ing = next(m for m in a.modules if m.id == "ingress")
    b_ing = next(m for m in b.modules if m.id == "ingress")
    assert a_ing.config["path"] == b_ing.config["path"]


def test_parse_dict_rejects_non_mapping(tmp_path):
    from aqueduct.parser.parser import ParseError

    with pytest.raises(ParseError, match="Blueprint must be a mapping"):
        parse_dict([], base_dir=tmp_path)  # type: ignore[arg-type]


# ── Phase 36 Part B — schema-driven anchoring via path_keys registry ─────────


def test_path_keys_registry_per_module_type():
    """Registry returns strict tuple for audited types and legacy fallback for the rest."""
    pytest.importorskip("pyspark")
    from aqueduct.executor.path_keys import get_path_keys
    assert get_path_keys("Ingress") == ("path", "data_dir", "input_dir", "jar")
    assert get_path_keys("Egress") == ("path", "output_dir", "jar")
    assert get_path_keys("UDF") == ("jar",)
    # Phase 65 — `table` is a catalog identifier, never a filesystem path
    assert "table" not in get_path_keys("Ingress")
    assert "table" not in get_path_keys("Egress")
    # Unknown / unregistered types fall back to the legacy blanket tuple.
    legacy = ("path", "data_dir", "input_dir", "output_dir", "jar")
    assert get_path_keys("Channel") == legacy
    assert get_path_keys("Bogus") == legacy


def test_parse_dict_anchors_only_registered_keys_per_type(tmp_path):
    """An Ingress carrying ``output_dir`` (not in its registered tuple) must
    stay untouched, while Egress with the same key anchors it. Catches the
    regression where parser anchored a blanket tuple across all module types."""
    raw = {
        "aqueduct": "1.0",
        "id": "test_registry_per_type",
        "name": "T",
        "modules": [
            {
                "id": "ing",
                "type": "Ingress",
                "label": "I",
                "config": {
                    "format": "csv",
                    "path": "data/in.csv",
                    "output_dir": "should-stay-relative",
                },
            },
            {
                "id": "eg",
                "type": "Egress",
                "label": "E",
                "config": {
                    "format": "parquet",
                    "path": "out",
                    "output_dir": "out/",
                },
            },
        ],
        "edges": [],
    }
    bp = parse_dict(raw, base_dir=tmp_path)
    ing = next(m for m in bp.modules if m.id == "ing")
    eg = next(m for m in bp.modules if m.id == "eg")
    # Ingress: ``output_dir`` is NOT in the registered tuple — passes through.
    assert ing.config["output_dir"] == "should-stay-relative"
    assert ing.config["path"] == str((tmp_path / "data/in.csv").resolve())
    # Egress: ``output_dir`` IS in the registered tuple — anchored.
    assert eg.config["output_dir"] == str((tmp_path / "out/").resolve())


def test_fs_path_marker_visible_on_store_fields():
    """RelationalStoreConfig.path and KVStoreConfig.path must carry the FsPath
    marker so the schema-driven walker in config.py picks them up."""
    from aqueduct.config import KVStoreConfig, RelationalStoreConfig
    from aqueduct.parser.fs_path import field_is_fs_path
    for cls in (RelationalStoreConfig, KVStoreConfig):
        field_info = cls.model_fields["path"]
        meta = tuple(field_info.metadata)
        assert field_is_fs_path(meta, field_info.annotation) is not None, f"{cls.__name__}.path missing FsPath()"


def test_fs_path_marker_seen_through_optional_on_this_interpreter():
    """``field_is_fs_path`` detects the marker inside an OPTIONAL annotated path
    field (e.g. ``RelationalStoreConfig.path: Annotated[str, FsPath()] | None``)
    on the running interpreter — a version-invariance floor.

    Note on the (fixed, defensive-only) union-origin handling: unlike the
    capability-leaf walker — where ``AgentBudgetConfig | None`` builds a plain
    ``types.UnionType`` on Python 3.11-3.13 and genuinely under-derived — an
    ``Annotated[...] | None`` normalizes to ``typing.Optional[Annotated[...]]``
    (origin ``typing.Union``) on EVERY version, because ``Annotated.__or__``
    normalizes. So the pre-fix ``origin is Union`` check already handled every
    real optional annotated path field correctly; the ``types.UnionType`` branch
    added in ``fs_path.py`` for consistency with the walker is inert for FsPath
    detection (no Annotated union ever reaches it). This test guards the
    behaviour that actually matters — optional annotated path fields stay
    detectable — rather than a cross-version failure that (for this seam) does
    not occur.
    """
    from typing import Annotated, get_args, get_origin

    from aqueduct.config import RelationalStoreConfig
    from aqueduct.parser.fs_path import FsPath, field_is_fs_path

    fi = RelationalStoreConfig.model_fields["path"]
    ann = fi.annotation
    # It really is optional (a union with NoneType), and still detected.
    assert type(None) in get_args(ann) or get_origin(ann) is not None
    assert isinstance(field_is_fs_path(tuple(fi.metadata), ann), FsPath)

    # Required annotated field — regression floor.
    assert isinstance(field_is_fs_path((), Annotated[str, FsPath()]), FsPath)


def test_config_anchors_store_paths_via_fs_path_walker(tmp_path):
    """Schema-driven walker must anchor every FsPath-marked store path
    relative to the config file's parent. Replacement test for the
    hardcoded ``stores.*.path`` loop Phase 36 Part B removes.

    The depot uses a ``redis://`` URI to exercise URI passthrough; CI
    installs the ``aqueduct-core[redis]`` extra (see ``.github/
    workflows/ci.yml``) so config-load succeeds in the integration run.
    """
    cfg_file = tmp_path / "aqueduct.yml"
    cfg_file.write_text(
        "stores:\n"
        "  observability: { backend: duckdb, path: .aqueduct/obs }\n"
        "  depots: { default: { backend: redis,  path: 'redis://h:6379/0' } }\n"
    )
    from aqueduct.config import load_config
    c = load_config(cfg_file)
    assert c.stores.observability.path == str((tmp_path / ".aqueduct/obs").resolve())
    # URI passthrough — walker bails on any ``://`` value.
    assert c.stores.default_depot().path == "redis://h:6379/0"
