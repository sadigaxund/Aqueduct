"""Compile-time integration of the hub type vocabulary (Phase 80, work
package 1 of 4) — aqueduct/compiler/compiler.py routes every inventoried
type-string surface (Ingress schema_hint, Channel op=cast columns, UDF
return_type) through aqueduct.typehub.parse_type for validation +
canonicalization only. The ORIGINAL string still reaches the Manifest
unchanged this package (no engine mapping, no runtime behavior change).
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest

from aqueduct import AqueductWarning
from aqueduct.compiler.compiler import compile as compiler_compile
from aqueduct.errors import CompileError
from aqueduct.parser.parser import parse
from aqueduct.typehub import AMBIGUOUS_TYPE_SPELLING_RULE_ID

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _ensure_warnings_caught():
    warnings.simplefilter("always", AqueductWarning)
    yield


def _compile_yaml(yaml_str: str, tmp_path: Path, **kwargs):
    bp_path = tmp_path / "test.yml"
    bp_path.write_text(yaml_str)
    bp = parse(str(bp_path))
    return compiler_compile(bp, blueprint_path=bp_path, **kwargs)


_INGRESS_EGRESS = """
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: /dummy/in
  - id: e1
    type: Egress
    label: E1
    config:
      format: parquet
      path: /dummy/out
"""


# ── Channel op=cast ──────────────────────────────────────────────────────────

def test_cast_valid_type_compiles(tmp_path):
    yaml_str = f"""
aqueduct: "1.0"
id: test
name: Test
modules:
{_INGRESS_EGRESS}
  - id: c1
    type: Channel
    label: C1
    config:
      op: cast
      columns:
        n: bigint
edges:
  - from: m1
    to: c1
  - from: c1
    to: e1
"""
    manifest = _compile_yaml(yaml_str, tmp_path)
    # Original string reaches the Manifest unchanged — no canonicalization
    # of the ORIGINAL config this package.
    c1 = next(m for m in manifest.modules if m.id == "c1")
    assert c1.config["columns"]["n"] == "bigint"


def test_cast_unknown_type_raises_compile_error(tmp_path):
    yaml_str = f"""
aqueduct: "1.0"
id: test
name: Test
modules:
{_INGRESS_EGRESS}
  - id: c1
    type: Channel
    label: C1
    config:
      op: cast
      columns:
        n: notarealtype
edges:
  - from: m1
    to: c1
  - from: c1
    to: e1
"""
    with pytest.raises(CompileError, match="notarealtype"):
        _compile_yaml(yaml_str, tmp_path)


def test_cast_list_form_validated(tmp_path):
    yaml_str = f"""
aqueduct: "1.0"
id: test
name: Test
modules:
{_INGRESS_EGRESS}
  - id: c1
    type: Channel
    label: C1
    config:
      op: cast
      columns:
        - {{column: n, type: notarealtype}}
edges:
  - from: m1
    to: c1
  - from: c1
    to: e1
"""
    with pytest.raises(CompileError, match="notarealtype"):
        _compile_yaml(yaml_str, tmp_path)


# ── Ingress schema_hint ──────────────────────────────────────────────────────

def test_schema_hint_dict_shorthand_valid(tmp_path):
    yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: /dummy/in
      schema_hint:
        id: bigint
        name: string
  - id: e1
    type: Egress
    label: E1
    config:
      format: parquet
      path: /dummy/out
edges:
  - from: m1
    to: e1
"""
    manifest = _compile_yaml(yaml_str, tmp_path)  # must not raise
    m1 = next(m for m in manifest.modules if m.id == "m1")
    assert m1.config["schema_hint"] == {"id": "bigint", "name": "string"}


def test_schema_hint_dict_shorthand_bad_type_raises(tmp_path):
    yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: /dummy/in
      schema_hint:
        id: notarealtype
  - id: e1
    type: Egress
    label: E1
    config:
      format: parquet
      path: /dummy/out
edges:
  - from: m1
    to: e1
"""
    with pytest.raises(CompileError, match="notarealtype"):
        _compile_yaml(yaml_str, tmp_path)


def test_schema_hint_columns_mode_form_bad_type_raises(tmp_path):
    yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: /dummy/in
      schema_hint:
        mode: strict
        columns:
          - {name: id, type: notarealtype}
  - id: e1
    type: Egress
    label: E1
    config:
      format: parquet
      path: /dummy/out
edges:
  - from: m1
    to: e1
"""
    with pytest.raises(CompileError, match="notarealtype"):
        _compile_yaml(yaml_str, tmp_path)


# ── UDF return_type ───────────────────────────────────────────────────────────

def test_udf_return_type_valid(tmp_path):
    yaml_str = f"""
aqueduct: "1.0"
id: test
name: Test
udf_registry:
  - id: my_udf
    lang: python
    module: my_udfs
    entry: fn
    return_type: STRING
modules:
{_INGRESS_EGRESS}
edges:
  - from: m1
    to: e1
"""
    manifest = _compile_yaml(yaml_str, tmp_path)
    assert manifest.udf_registry[0]["return_type"] == "STRING"


def test_udf_return_type_bad_raises(tmp_path):
    yaml_str = f"""
aqueduct: "1.0"
id: test
name: Test
udf_registry:
  - id: my_udf
    lang: python
    module: my_udfs
    entry: fn
    return_type: notarealtype
modules:
{_INGRESS_EGRESS}
edges:
  - from: m1
    to: e1
"""
    with pytest.raises(CompileError, match="notarealtype"):
        _compile_yaml(yaml_str, tmp_path)


# ── Bare `timestamp` — deprecation-window warning, not an error ─────────────

def test_bare_timestamp_in_schema_hint_warns_not_errors(tmp_path):
    yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: /dummy/in
      schema_hint:
        ts: timestamp
  - id: e1
    type: Egress
    label: E1
    config:
      format: parquet
      path: /dummy/out
edges:
  - from: m1
    to: e1
"""
    with warnings.catch_warnings(record=True) as caught:
        _compile_yaml(yaml_str, tmp_path)  # must not raise
    assert any(AMBIGUOUS_TYPE_SPELLING_RULE_ID in str(w.message) for w in caught)


def test_bare_timestamp_suppressible_via_blueprint_warnings_block(tmp_path):
    yaml_str = """
aqueduct: "1.0"
id: test
name: Test
warnings:
  suppress: [ambiguous_type_spelling]
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: /dummy/in
      schema_hint:
        ts: timestamp
  - id: e1
    type: Egress
    label: E1
    config:
      format: parquet
      path: /dummy/out
edges:
  - from: m1
    to: e1
"""
    with warnings.catch_warnings(record=True) as caught:
        _compile_yaml(yaml_str, tmp_path)
    assert not any(AMBIGUOUS_TYPE_SPELLING_RULE_ID in str(w.message) for w in caught)


def test_bare_timestamp_suppressible_via_engine_level_suppress(tmp_path):
    yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: /dummy/in
      schema_hint:
        ts: timestamp
  - id: e1
    type: Egress
    label: E1
    config:
      format: parquet
      path: /dummy/out
edges:
  - from: m1
    to: e1
"""
    with warnings.catch_warnings(record=True) as caught:
        _compile_yaml(yaml_str, tmp_path, warnings_suppress={"ambiguous_type_spelling"})
    assert not any(AMBIGUOUS_TYPE_SPELLING_RULE_ID in str(w.message) for w in caught)


# ── Disabled modules are skipped (same convention as sections 7-8) ──────────

def test_disabled_module_bad_type_does_not_block_compile(tmp_path):
    yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: /dummy/in
  - id: c1
    type: Channel
    label: C1
    enabled: false
    config:
      op: cast
      columns:
        n: notarealtype
  - id: e1
    type: Egress
    label: E1
    config:
      format: parquet
      path: /dummy/out
edges:
  - from: m1
    to: c1
  - from: c1
    to: e1
"""
    manifest = _compile_yaml(yaml_str, tmp_path)  # must not raise — c1 (and cascaded e1) disabled
    c1 = next(m for m in manifest.modules if m.id == "c1")
    assert not c1.enabled
