"""Unit tests for aqueduct/agent/regression_artifact.py (post-heal .aqtest.yml).

All external state (Manifest, PatchSpec, FailureContext) is constructed
in-memory; no Spark, no network, no live LLM. File I/O is confined to
tmp_path.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

from aqueduct.agent.regression_artifact import generate
from aqueduct.compiler.models import Manifest
from aqueduct.parser.models import Edge, Module
from aqueduct.patch.grammar import PatchSpec
from aqueduct.surveyor.models import FailureContext


def _failure_ctx(**overrides) -> FailureContext:
    base = dict(
        run_id="run-1",
        blueprint_id="bp",
        failed_module="clean_orders",
        error_message="[clean_orders] schema_hint type mismatch on 'amount': expected 'string', actual 'double'",
        stack_trace=None,
        manifest_json="{}",
        started_at="2026-01-01T00:00:00Z",
        finished_at="2026-01-01T00:01:00Z",
        engine="spark",
    )
    base.update(overrides)
    return FailureContext(**base)


def _patch(**overrides) -> PatchSpec:
    base = dict(
        patch_id="fix-amount-type",
        rationale="Aligned schema_hint for amount to the inferred double type.",
        confidence=0.9,
        category="type_mismatch",
        root_cause="schema_hint declared string but data is double.",
        operations=[
            {"op": "set_module_config_key", "module_id": "clean_orders", "key": "query", "value": "SELECT * FROM raw_orders"}
        ],
    )
    base.update(overrides)
    return PatchSpec.model_validate(base)


def _manifest(modules: list[Module], edges: list[Edge]) -> Manifest:
    return Manifest(
        blueprint_id="bp",
        context={},
        modules=tuple(modules),
        edges=tuple(edges),
        spark_config={},
    )


def _channel_with_upstream(schema_hint: list[dict] | None = "MISSING") -> tuple[list[Module], list[Edge]]:
    ingress_config: dict = {"format": "parquet", "path": "data/raw.parquet"}
    if schema_hint != "MISSING":
        ingress_config["schema_hint"] = schema_hint
    ingress = Module(id="raw_orders", type="Ingress", label="raw", config=ingress_config)
    channel = Module(id="clean_orders", type="Channel", label="clean", config={"op": "sql", "query": "SELECT * FROM raw_orders"})
    edge = Edge(from_id="raw_orders", to_id="clean_orders", port="main")
    return [ingress, channel], [edge]


class TestGenerateHappyPath:
    def test_writes_valid_aqtest_yaml(self, tmp_path: Path):
        modules, edges = _channel_with_upstream(
            schema_hint=[{"name": "order_id", "type": "bigint"}, {"name": "amount", "type": "double"}]
        )
        manifest = _manifest(modules, edges)
        blueprint_path = tmp_path / "blueprint.yml"
        blueprint_path.write_text("aqueduct: 1.0\n")

        result = generate(manifest, _patch(), _failure_ctx(), blueprint_path)

        assert result.written is True
        assert result.path is not None
        assert result.path.exists()
        assert result.path.parent == tmp_path / "aqtests"

        doc = yaml.safe_load(result.path.read_text())
        assert doc["aqueduct_test"] == "1.0"
        assert doc["blueprint"] == "../blueprint.yml"
        [test_case] = doc["tests"]
        assert test_case["module"] == "clean_orders"
        assert "raw_orders" in test_case["inputs"]
        assert test_case["inputs"]["raw_orders"]["schema"] == {"order_id": "bigint", "amount": "double"}
        assert test_case["inputs"]["raw_orders"]["rows"] == [[1, 1.0]]
        assert test_case["assertions"][0]["type"] == "sql"

    def test_never_overwrites_existing_file(self, tmp_path: Path):
        modules, edges = _channel_with_upstream(schema_hint=[{"name": "order_id", "type": "bigint"}])
        manifest = _manifest(modules, edges)
        blueprint_path = tmp_path / "blueprint.yml"
        blueprint_path.write_text("aqueduct: 1.0\n")

        first = generate(manifest, _patch(), _failure_ctx(), blueprint_path)
        second = generate(manifest, _patch(patch_id="fix-amount-type"), _failure_ctx(), blueprint_path)

        assert first.written and second.written
        assert first.path != second.path
        assert first.path.exists() and second.path.exists()


class TestGenerateConservativeSkips:
    def test_skip_when_patch_touches_multiple_modules(self, tmp_path: Path):
        modules, edges = _channel_with_upstream(schema_hint=[{"name": "order_id", "type": "bigint"}])
        manifest = _manifest(modules, edges)
        blueprint_path = tmp_path / "blueprint.yml"
        blueprint_path.write_text("aqueduct: 1.0\n")
        multi = _patch(operations=[
            {"op": "set_module_config_key", "module_id": "clean_orders", "key": "query", "value": "SELECT 1"},
            {"op": "set_module_config_key", "module_id": "raw_orders", "key": "format", "value": "csv"},
        ])

        result = generate(manifest, multi, _failure_ctx(), blueprint_path)

        assert result.written is False
        assert "multiple modules" in result.skip_reason
        assert not (tmp_path / "aqtests").exists()

    def test_skip_when_patch_has_non_config_op(self, tmp_path: Path):
        modules, edges = _channel_with_upstream(schema_hint=[{"name": "order_id", "type": "bigint"}])
        manifest = _manifest(modules, edges)
        blueprint_path = tmp_path / "blueprint.yml"
        blueprint_path.write_text("aqueduct: 1.0\n")
        patch = _patch(operations=[
            {"op": "replace_context_value", "key": "paths.input", "value": "data/x.parquet"},
        ])

        result = generate(manifest, patch, _failure_ctx(), blueprint_path)

        assert result.written is False
        assert result.skip_reason is not None

    def test_skip_when_module_type_not_testable(self, tmp_path: Path):
        ingress = Module(id="raw_orders", type="Ingress", label="raw", config={"format": "parquet", "path": "p"})
        edge = Edge(from_id="_x", to_id="raw_orders", port="main")
        manifest = _manifest([ingress], [edge])
        blueprint_path = tmp_path / "blueprint.yml"
        blueprint_path.write_text("aqueduct: 1.0\n")
        patch = _patch(operations=[
            {"op": "set_module_config_key", "module_id": "raw_orders", "key": "format", "value": "csv"},
        ])

        result = generate(manifest, patch, _failure_ctx(), blueprint_path)

        assert result.written is False
        assert "not testable" in result.skip_reason

    def test_skip_when_no_upstream_inputs(self, tmp_path: Path):
        channel = Module(id="clean_orders", type="Channel", label="clean", config={"op": "sql", "query": "SELECT 1"})
        manifest = _manifest([channel], [])
        blueprint_path = tmp_path / "blueprint.yml"
        blueprint_path.write_text("aqueduct: 1.0\n")

        result = generate(manifest, _patch(), _failure_ctx(), blueprint_path)

        assert result.written is False
        assert "no upstream inputs" in result.skip_reason

    def test_skip_when_upstream_has_no_schema_hint(self, tmp_path: Path):
        modules, edges = _channel_with_upstream(schema_hint="MISSING")
        manifest = _manifest(modules, edges)
        blueprint_path = tmp_path / "blueprint.yml"
        blueprint_path.write_text("aqueduct: 1.0\n")

        result = generate(manifest, _patch(), _failure_ctx(), blueprint_path)

        assert result.written is False
        assert "schema_hint" in result.skip_reason

    def test_skip_when_schema_hint_type_unmapped(self, tmp_path: Path):
        modules, edges = _channel_with_upstream(schema_hint=[{"name": "geo", "type": "array<string>"}])
        manifest = _manifest(modules, edges)
        blueprint_path = tmp_path / "blueprint.yml"
        blueprint_path.write_text("aqueduct: 1.0\n")

        result = generate(manifest, _patch(), _failure_ctx(), blueprint_path)

        assert result.written is False
        assert "unmapped type" in result.skip_reason

    def test_skip_when_patched_module_not_found(self, tmp_path: Path):
        modules, edges = _channel_with_upstream(schema_hint=[{"name": "order_id", "type": "bigint"}])
        manifest = _manifest(modules, edges)
        blueprint_path = tmp_path / "blueprint.yml"
        blueprint_path.write_text("aqueduct: 1.0\n")
        patch = _patch(operations=[
            {"op": "set_module_config_key", "module_id": "does_not_exist", "key": "query", "value": "SELECT 1"},
        ])

        result = generate(manifest, patch, _failure_ctx(), blueprint_path)

        assert result.written is False
        assert "not found in manifest" in result.skip_reason

    def test_no_ops_produces_skip_not_crash(self, tmp_path: Path):
        modules, edges = _channel_with_upstream(schema_hint=[{"name": "order_id", "type": "bigint"}])
        manifest = _manifest(modules, edges)
        blueprint_path = tmp_path / "blueprint.yml"
        blueprint_path.write_text("aqueduct: 1.0\n")
        patch = _patch(operations=[{"op": "defer_to_human", "diagnosis": "x", "suggestions": ["y"], "confidence_reason": "z"}])

        result = generate(manifest, patch, _failure_ctx(), blueprint_path)

        assert result.written is False
