import pytest
import os
from pathlib import Path
from aqueduct.parser.parser import parse, ParseError
from aqueduct.compiler.compiler import compile as aq_compile

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "audit"

def test_audit_01_strict_schema_validation():
    """Verify that unknown fields at top-level and module-level raise ParseError."""
    blueprint_path = FIXTURE_DIR / "01-strict-schema-validation" / "invalid_blueprint.yml"
    
    with pytest.raises(ParseError) as excinfo:
        parse(blueprint_path)
    
    error_msg = str(excinfo.value)
    # Pydantic 2.x reports extra fields
    assert "unknown_field" in error_msg
    assert "unknown_module_field" in error_msg

def test_audit_02_context_resolution(monkeypatch):
    """Verify recursive context resolution and profile overrides."""
    monkeypatch.setenv("MY_VAR", "env_value")
    blueprint_path = FIXTURE_DIR / "02-context-resolution" / "blueprint.yml"
    
    # 1. Parse
    blueprint = parse(blueprint_path)
    
    # 2. Compile with default profile
    manifest = aq_compile(blueprint, blueprint_path=blueprint_path, run_id="test-run")
    
    # Check resolution logic (mimicking the validate.py logic)
    # The blueprint should have:
    # paths:
    #   base: "/data"
    #   input: "${ctx.paths.base}/raw" -> "/data/raw"
    #   output: "${env.AQ_ENV_VAR}/processed" -> "env_value/processed"
    
    # We need to find the module and check its resolved config
    m1 = next(m for m in manifest.modules if m.id == "m1")
    assert m1.config["path"] == "/data/raw/users"
    assert m1.config["env_check"] == "env_value"  # MY_VAR set by monkeypatch in test
    assert m1.config["direct_env"] == "env_value"
    
    # Check that tier1 was resolved (today's date)
    import datetime
    today = datetime.date.today().strftime("%Y-%m-%d")
    assert m1.config["tier1_check"] == today

def test_audit_05_provenance_map(monkeypatch):
    """Verify deep provenance tracking for context, env, and arcade inheritance."""
    monkeypatch.setenv("MY_VAR", "real_env_value")
    main_path = FIXTURE_DIR / "05-provenance-map" / "main_prov.yml"
    
    # 1. Parse
    blueprint = parse(main_path)
    
    # 2. Compile
    manifest = aq_compile(blueprint, blueprint_path=main_path, run_id="test-run-123")
    prov = manifest.provenance_map
    assert prov is not None
    
    # Check Module m1
    m1_prov = prov.for_module("m1").config
    
    assert m1_prov["c1"].source_type == "context_ref"
    assert m1_prov["c1"].context_key == "my_ctx"
    assert m1_prov["c1"].original_expression == "${ctx.my_ctx}"
    
    assert m1_prov["e1"].source_type == "context_ref"
    assert m1_prov["e1"].context_key == "my_env"
    
    assert m1_prov["t1"].source_type == "context_ref"
    assert m1_prov["t1"].context_key == "my_tier1"
    
    assert m1_prov["l1"].source_type == "literal"
    
    # Check context provenance
    assert prov.context["my_ctx"].source_type == "literal"
    assert prov.context["my_env"].source_type == "env_ref"
    assert prov.context["my_env"].env_var == "MY_VAR"
    assert prov.context["my_tier1"].source_type == "tier1"

    # Check Arcade Module arc__m_sub
    sub_id = "arc__m_sub"
    arc_prov = prov.for_module(sub_id).config
    
    assert arc_prov["sub_key"].source_type == "arcade_inherited"
    assert arc_prov["sub_key"].arcade_module_id == "arc"
    assert arc_prov["sub_key"].original_expression == "${ctx.shared_val}"
    assert arc_prov["sub_key"].resolved_value == "injected"
