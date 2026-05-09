import pytest
from aqueduct.compiler.provenance import (
    ValueProvenance,
    ModuleProvenance,
    ProvenanceMap,
    infer_value_provenance,
    build_config_provenance
)

def test_infer_value_provenance_literal_string():
    vp = infer_value_provenance("just a string", "just a string")
    assert vp.source_type == "literal"
    assert vp.original_expression == "just a string"
    assert vp.resolved_value == "just a string"

def test_infer_value_provenance_non_string_literal():
    vp = infer_value_provenance(123, 123)
    assert vp.source_type == "literal"
    assert vp.original_expression == "123"
    assert vp.resolved_value == 123
    
    vp2 = infer_value_provenance(True, True)
    assert vp2.source_type == "literal"
    assert vp2.original_expression == "True"

def test_infer_value_provenance_ctx_ref():
    vp = infer_value_provenance("${ctx.paths.foo}", "/data/foo")
    assert vp.source_type == "context_ref"
    assert vp.context_key == "paths.foo"
    assert vp.original_expression == "${ctx.paths.foo}"
    assert vp.resolved_value == "/data/foo"

def test_infer_value_provenance_env_ref():
    vp = infer_value_provenance("${ENV_VAR:-default}", "actual_val")
    assert vp.source_type == "env_ref"
    assert vp.env_var == "ENV_VAR"
    assert vp.original_expression == "${ENV_VAR:-default}"
    
    vp2 = infer_value_provenance("${VAR}", "val")
    assert vp2.env_var == "VAR"

def test_infer_value_provenance_tier1():
    vp = infer_value_provenance("@aq.date.today()", "2026-01-01")
    assert vp.source_type == "tier1"
    assert vp.original_expression == "@aq.date.today()"
    assert vp.resolved_value == "2026-01-01"

def test_infer_value_provenance_arcade_ctx():
    vp = infer_value_provenance("${ctx.shared}", "injected", arcade_module_id="arc")
    assert vp.source_type == "arcade_inherited"
    assert vp.arcade_module_id == "arc"
    assert vp.context_key == "shared"
    assert vp.original_expression == "${ctx.shared}"

def test_infer_value_provenance_arcade_literal():
    vp = infer_value_provenance("literal", "literal", arcade_module_id="arc")
    assert vp.source_type == "arcade_inherited"
    assert vp.arcade_module_id == "arc"
    assert vp.context_key is None
    assert vp.original_expression == "literal"

def test_build_config_provenance_flat():
    raw = {"path": "${ctx.path}", "format": "parquet"}
    resolved = {"path": "/data", "format": "parquet"}
    prov = build_config_provenance(raw, resolved)
    
    assert prov["path"].source_type == "context_ref"
    assert prov["path"].context_key == "path"
    assert prov["format"].source_type == "literal"

def test_build_config_provenance_nested():
    raw = {"options": {"mergeSchema": "true"}}
    resolved = {"options": {"mergeSchema": "true"}}
    prov = build_config_provenance(raw, resolved)
    
    assert "options.mergeSchema" in prov
    assert prov["options.mergeSchema"].source_type == "literal"

def test_build_config_provenance_list():
    raw = {"cols": ["a", "b"]}
    resolved = {"cols": ["a", "b"]}
    prov = build_config_provenance(raw, resolved)
    
    # Lists are tracked at the key level, not per-item (per provenance.py line 180)
    assert "cols" in prov
    assert prov["cols"].source_type == "literal"
    assert prov["cols"].resolved_value == ["a", "b"]

def test_build_config_provenance_none():
    assert build_config_provenance(None, None) == {}

def test_provenance_map_to_dict_serializable():
    vp = ValueProvenance(source_type="literal", resolved_value=1)
    mp = ModuleProvenance(module_id="m1", module_type="Ingress", config={"k": vp})
    pmap = ProvenanceMap(blueprint_id="bp1", blueprint_path="/tmp/bp.yml", modules={"m1": mp}, context={})
    
    d = pmap.to_dict()
    assert d["blueprint_id"] == "bp1"
    assert d["modules"]["m1"]["module_id"] == "m1"
    assert d["modules"]["m1"]["config"]["k"]["source_type"] == "literal"
    assert d["modules"]["m1"]["config"]["k"]["resolved_value"] == 1
