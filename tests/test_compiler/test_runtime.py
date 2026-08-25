"""Tests for the Compiler layer: Tier 1 runtime functions."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from aqueduct.compiler.compiler import compile
from aqueduct.compiler.runtime import AqFunctions, resolve_tier1_str
from aqueduct.errors import CompileError
from aqueduct.parser.parser import parse

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).parent.parent / "fixtures"


class TestTier1Resolution:
    def setup_method(self):
        self.reg = AqFunctions(run_id="test-run-001")

    def test_date_today_returns_iso(self):
        result = resolve_tier1_str("@aq.date.today()", self.reg)
        assert result == date.today().isoformat()

    def test_date_today_custom_format(self):
        result = resolve_tier1_str("@aq.date.today(format='yyyy/MM/dd')", self.reg)
        assert result == date.today().strftime("%Y/%m/%d")

    def test_date_today_time_pattern_letters_not_left_as_literal_text(self):
        """HH/mm/ss were unmapped in _java_to_strftime, so a common
        'yyyy-MM-dd HH:mm:ss' format string left the literal, unresolved-
        looking text "HH:mm:ss" glued onto the date instead of rendering
        (00:00:00, since a bare `date` has no time component). Proves the
        gap is closed — this must never again silently embed literal Java
        pattern letters into a value used for paths/partitions."""
        result = resolve_tier1_str("@aq.date.today(format='yyyy-MM-dd HH:mm:ss')", self.reg)
        assert result == date.today().strftime("%Y-%m-%d 00:00:00")
        assert "HH" not in result
        assert "mm" not in result
        assert "ss" not in result

    def test_date_yesterday(self):
        from datetime import timedelta

        result = resolve_tier1_str("@aq.date.yesterday()", self.reg)
        assert result == (date.today() - timedelta(days=1)).isoformat()

    def test_date_offset_positive(self):
        result = resolve_tier1_str("@aq.date.offset(base='2024-01-01', days=7)", self.reg)
        assert result == "2024-01-08"

    def test_date_offset_negative(self):
        result = resolve_tier1_str("@aq.date.offset(base='2024-01-10', days=-3)", self.reg)
        assert result == "2024-01-07"

    def test_date_month_start(self):
        result = resolve_tier1_str("@aq.date.month_start()", self.reg)
        assert result == date.today().replace(day=1).isoformat()

    def test_runtime_run_id(self):
        result = resolve_tier1_str("@aq.run.id()", self.reg)
        assert result == "test-run-001"

    def test_runtime_prev_run_id_no_depot_raises(self):
        """No depot backend configured + @aq.run.prev_id() referenced in the
        Blueprint must fail loud (CompileError) rather than silently return
        "" — that silent fallback causes incremental pipelines to re-read
        all data every run. See depot_get for the same ruling."""
        with pytest.raises(CompileError, match=r"@aq\.run\.prev_id"):
            resolve_tier1_str("@aq.run.prev_id()", self.reg)

    def test_runtime_prev_run_id_exists(self, tmp_path):
        from aqueduct.depot.depot import DepotStore

        store = DepotStore(tmp_path / "depot.db")
        store.put("_last_run_id", "test-run-999")

        reg = AqFunctions(run_id="test-run-001", depot=store)
        result = resolve_tier1_str("@aq.run.prev_id()", reg)
        assert result == "test-run-999"

    def test_runtime_timestamp_is_iso(self):
        result = resolve_tier1_str("@aq.run.timestamp()", self.reg)
        from datetime import datetime

        dt = datetime.fromisoformat(result)
        assert dt.tzinfo is not None

    def test_env_function(self, monkeypatch):
        monkeypatch.setenv("MY_TEST_VAR", "hello")
        result = resolve_tier1_str("@aq.env('MY_TEST_VAR')", self.reg)
        assert result == "hello"

    def test_env_missing_raises(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(CompileError, match="not set"):
            resolve_tier1_str("@aq.env('MISSING_VAR')", self.reg)

    def test_depot_get_no_depot_raises(self):
        """No depot backend configured + @aq.depot.get referenced in the
        Blueprint must fail loud (CompileError), not silently fall back to
        the default — that silent fallback causes incremental pipelines to
        re-read all data every run."""
        with pytest.raises(CompileError, match="stores.depots") as exc_info:
            resolve_tier1_str("@aq.depot.get('key', 'fallback')", self.reg)
        assert "aqueduct.yml" in str(exc_info.value)

    def test_depot_get_no_depot_empty_default_raises(self):
        with pytest.raises(CompileError, match="stores.depots"):
            resolve_tier1_str("@aq.depot.get('some.key')", self.reg)

    def test_depot_get_configured_missing_key_returns_default(self, tmp_path):
        from aqueduct.depot.depot import DepotStore

        store = DepotStore(tmp_path / "depot.db")
        reg = AqFunctions(run_id="test-run-001", depot=store)
        result = resolve_tier1_str("@aq.depot.get('missing.key', 'fallback')", reg)
        assert result == "fallback"

    def test_depot_get_configured_present_key_returns_value(self, tmp_path):
        from aqueduct.depot.depot import DepotStore

        store = DepotStore(tmp_path / "depot.db")
        store.put("some.key", "the-value")
        reg = AqFunctions(run_id="test-run-001", depot=store)
        result = resolve_tier1_str("@aq.depot.get('some.key')", reg)
        assert result == "the-value"

    def test_no_depot_reference_resolves_fine_without_depot_configured(self):
        """An unconfigured depot must not raise for a Blueprint that never
        touches @aq.depot.* / @aq.run.prev_id() — the raise only fires when
        the function is actually called during resolution."""
        result = resolve_tier1_str("@aq.date.today()", self.reg)
        assert result == date.today().isoformat()

    def test_run_prev_id_no_depot_raises_names_the_function(self):
        with pytest.raises(CompileError, match=r"@aq\.run\.prev_id"):
            resolve_tier1_str("@aq.run.prev_id()", self.reg)

    def test_nested_call_resolved(self):
        result = resolve_tier1_str("@aq.date.offset(base='@aq.date.today()', days=1)", self.reg)
        from datetime import timedelta

        assert result == (date.today() + timedelta(days=1)).isoformat()

    def test_tier1_in_string_context(self):
        result = resolve_tier1_str("s3://bucket/@aq.date.today()/data", self.reg)
        assert result == f"s3://bucket/{date.today().isoformat()}/data"

    def test_unknown_function_raises(self):
        with pytest.raises(CompileError, match="Unknown @aq function"):
            resolve_tier1_str("@aq.does.not.exist()", self.reg)

    def test_missing_required_arg_raises_compile_error_not_type_error(self):
        """A registered @aq.* function called with too few args raises a bare
        Python TypeError from the underlying method call — a user-reachable
        error (malformed Blueprint) that must surface as CompileError per
        AGENTS.md's "User-reachable errors raise an AqueductError subclass,
        never a bare builtin" rule, not escape uncaught with no exit-code
        mapping."""
        with pytest.raises(CompileError, match="date.offset"):
            resolve_tier1_str("@aq.date.offset()", self.reg)

    def test_wrong_arg_count_zero_arg_function_raises_compile_error(self):
        """Same TypeError->CompileError conversion, exercised on the
        zero-args call path (`method()`, no parens content) rather than the
        ast-parsed-args path — e.g. a zero-arg registered function invoked
        with unexpected positional args via the parsed-args branch."""
        with pytest.raises(CompileError, match="engine_version"):
            resolve_tier1_str("@aq.version(1)", self.reg)

    def test_tier1_resolved_in_manifest_context(self, tmp_path):
        bp_file = tmp_path / "tier1_ctx.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "context:\n"
            "  run_date: '@aq.date.today()'\n"
            '  path: "data/${ctx.run_date}/output"\n'
            "modules:\n  - id: m\n    type: Channel\n    label: M\n    config:\n      op: sql\n      query: SELECT 1\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        manifest = compile(bp, blueprint_path=bp_file)
        assert manifest.context["run_date"] == date.today().isoformat()
        assert manifest.context["path"] == f"data/{date.today().isoformat()}/output"


class TestLogicalExecutionDate:
    def test_base_date_returns_execution_date_when_set(self):
        d = date(2026, 1, 15)
        aq = AqFunctions(execution_date=d)
        assert aq._base_date() == d

    def test_base_date_returns_today_when_not_set(self):
        aq = AqFunctions()
        assert aq._base_date() == date.today()

    def test_date_today_with_execution_date(self):
        aq = AqFunctions(execution_date=date(2026, 1, 15))
        assert aq.date_today() == "2026-01-15"

    def test_runtime_timestamp_with_execution_date_is_midnight_utc(self):
        from datetime import datetime

        aq = AqFunctions(execution_date=date(2026, 1, 15))
        ts = aq.run_timestamp()
        parsed = datetime.fromisoformat(ts)
        assert parsed.hour == 0
        assert parsed.tzinfo is not None


class TestCompilerEdgeCases:
    """Cover uncovered compiler paths."""

    def test_post_tier1_ctx_reresolution(self, tmp_path):
        """Context values that contain ${ctx.*} after Tier1 resolution should be re-resolved."""
        from unittest.mock import MagicMock

        from aqueduct.compiler.compiler import compile as compiler_compile
        from aqueduct.parser.parser import parse

        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: p\nname: P\n"
            "context:\n  base: '2026-01-01'\n  derived: '${ctx.base}'\n"
            "modules: []\nedges: []\n",
            encoding="utf-8",
        )
        bp = parse(str(bp_file))
        manifest = compiler_compile(bp, blueprint_path=bp_file, depot=MagicMock())
        assert manifest is not None

    def test_compile_with_retry_policy_and_append_egress_warns(self, tmp_path):
        """max_attempts > 1 on append Egress should emit a warning."""
        import warnings
        from unittest.mock import MagicMock

        from aqueduct.compiler.compiler import compile as compiler_compile
        from aqueduct.parser.parser import parse

        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: p\nname: P\n"
            "retry_policy:\n  max_attempts: 3\n"
            "modules:\n"
            "  - id: out\n    type: Egress\n    label: Out\n"
            "    config:\n      format: parquet\n      path: /tmp/x\n      mode: append\n"
            "edges: []\n",
            encoding="utf-8",
        )
        bp = parse(str(bp_file))
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compiler_compile(bp, blueprint_path=bp_file, depot=MagicMock())
        assert any("append" in str(warning.message) for warning in w)


class TestDepotReadsRecording:
    """`AqFunctions.depot_reads` — recorded during Tier 1 resolution, consumed by
    Gate 3's staleness notice (aqueduct/patch/preview.py::run_sandbox_gate)."""

    def test_no_depot_function_used_records_nothing(self):
        reg = AqFunctions(run_id="test-run-001")
        resolve_tier1_str("@aq.date.today()", reg)
        assert reg.depot_reads == {}

    def test_default_mount_depot_get_records_key_and_value(self, tmp_path):
        from aqueduct.depot.depot import DepotStore

        store = DepotStore(tmp_path / "depot.db")
        store.put("some.key", "the-value")
        reg = AqFunctions(run_id="test-run-001", depot=store)
        resolve_tier1_str("@aq.depot.get('some.key')", reg)
        assert reg.depot_reads == {"some.key": "the-value"}

    def test_run_prev_id_records_under_last_run_id_key(self, tmp_path):
        from aqueduct.depot.depot import DepotStore

        store = DepotStore(tmp_path / "depot.db")
        store.put("_last_run_id", "test-run-999")
        reg = AqFunctions(run_id="test-run-001", depot=store)
        resolve_tier1_str("@aq.run.prev_id()", reg)
        assert reg.depot_reads == {"_last_run_id": "test-run-999"}

    def test_named_mount_records_namespaced_key(self, tmp_path):
        from aqueduct.depot.depot import DepotStore

        store = DepotStore(tmp_path / "depot.db")
        store.put("some.key", "named-value")
        reg = AqFunctions(run_id="test-run-001", depots={"other": store})
        resolve_tier1_str("@aq.depot.other.get('some.key')", reg)
        assert reg.depot_reads == {"other:some.key": "named-value"}

    def test_default_and_named_mount_same_key_do_not_collide(self, tmp_path):
        from aqueduct.depot.depot import DepotStore

        default_store = DepotStore(tmp_path / "default.db")
        default_store.put("k", "default-value")
        named_store = DepotStore(tmp_path / "named.db")
        named_store.put("k", "named-value")
        reg = AqFunctions(
            run_id="test-run-001",
            depot=default_store,
            depots={"default": default_store, "other": named_store},
        )
        resolve_tier1_str("@aq.depot.get('k')", reg)
        resolve_tier1_str("@aq.depot.other.get('k')", reg)
        assert reg.depot_reads == {"k": "default-value", "other:k": "named-value"}


class TestCompileDepotReadsOut:
    """`compile(depot_reads_out=...)` — the sink populated from AqFunctions.depot_reads."""

    def test_sink_populated_from_module_config_depot_read(self, tmp_path):
        from aqueduct.depot.depot import DepotStore

        store = DepotStore(tmp_path / "depot.db")
        store.put("watermark", "2026-01-01")
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: p\nname: P\n"
            "modules:\n"
            "  - id: out\n    type: Egress\n    label: Out\n"
            "    config:\n"
            "      format: parquet\n"
            '      path: "/tmp/${ctx.watermark}"\n'
            "      mode: append\n"
            "      partition_by: [d]\n"
            "context:\n"
            "  watermark: \"@aq.depot.get('watermark')\"\n"
            "edges: []\n",
            encoding="utf-8",
        )
        bp = parse(str(bp_file))
        sink: dict[str, str] = {}
        compile(bp, blueprint_path=bp_file, depot=store, depot_reads_out=sink)
        assert sink == {"watermark": "2026-01-01"}

    def test_sink_untouched_when_no_depot_function_used(self, tmp_path):
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: p\nname: P\nmodules: []\nedges: []\n",
            encoding="utf-8",
        )
        bp = parse(str(bp_file))
        sink: dict[str, str] = {}
        compile(bp, blueprint_path=bp_file, depot_reads_out=sink)
        assert sink == {}

    def test_sink_omitted_does_not_raise(self, tmp_path):
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: p\nname: P\nmodules: []\nedges: []\n",
            encoding="utf-8",
        )
        bp = parse(str(bp_file))
        manifest = compile(bp, blueprint_path=bp_file)
        assert manifest is not None


class TestAqMeta:
    """@aq.blueprint.* / @aq.deployment.* — pipeline identity / deployment context."""

    def test_meta_resolves(self):
        reg = AqFunctions(
            blueprint_id="my_bp",
            blueprint_name="My BP",
            blueprint_path="/proj/blueprints/my_bp.yml",
            deployment_env="cluster",
            deployment_target="standalone",
            deployment_engine="spark",
        )
        assert resolve_tier1_str("@aq.blueprint.id()", reg) == "my_bp"
        assert resolve_tier1_str("@aq.blueprint.name()", reg) == "My BP"
        assert resolve_tier1_str("@aq.blueprint.path()", reg) == "/proj/blueprints/my_bp.yml"
        assert resolve_tier1_str("@aq.blueprint.dir()", reg) == "/proj/blueprints"
        assert resolve_tier1_str("@aq.deployment.env()", reg) == "cluster"
        assert resolve_tier1_str("@aq.deployment.target()", reg) == "standalone"
        assert resolve_tier1_str("@aq.deployment.engine()", reg) == "spark"
        assert resolve_tier1_str("@aq.version()", reg)  # non-empty

    def test_engine_comes_from_the_compile_target_not_a_config_guess(self):
        """`@aq.deployment.engine()` resolves to the engine compile() was asked for,
        so the same Blueprint can stamp the engine into an output path or tag when
        it runs on more than one engine."""
        from aqueduct.compiler.compiler import compile as compile_bp
        from aqueduct.parser.parser import parse_dict

        bp = parse_dict(
            {
                "aqueduct": "1.0",
                "id": "engine_stamp",
                "name": "Engine stamp",
                "modules": [
                    {
                        "id": "src",
                        "label": "src",
                        "type": "Ingress",
                        "config": {"format": "parquet", "path": "in.parquet"},
                    },
                    {
                        "id": "out",
                        "label": "out",
                        "type": "Egress",
                        "config": {
                            "format": "parquet",
                            "path": "out/@aq.deployment.engine()/data.parquet",
                            "mode": "overwrite",
                        },
                    },
                ],
                "edges": [{"from": "src", "to": "out"}],
            },
            base_dir=".",
        )
        manifest = compile_bp(bp, engine="spark")
        egress = next(m for m in manifest.modules if m.id == "out")
        assert egress.config["path"].endswith("out/spark/data.parquet")

    def test_meta_in_path_expression(self):
        reg = AqFunctions(blueprint_id="sales", blueprint_path="/p/sales.yml")
        out = resolve_tier1_str("out/@aq.blueprint.id()/data", reg)
        assert out == "out/sales/data"

    def test_meta_unavailable_raises(self):
        reg = AqFunctions()  # no metadata threaded
        with pytest.raises(CompileError, match="not available here"):
            resolve_tier1_str("@aq.deployment.env()", reg)

    def test_meta_resolves_through_compile(self, tmp_path):
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: meta_demo\nname: Meta Demo\n"
            'context:\n  tag: "@aq.blueprint.id()-@aq.deployment.env()"\n'
            "modules:\n"
            "  - id: out\n    type: Egress\n    label: Out\n"
            "    config:\n      format: parquet\n      path: /tmp/${ctx.tag}\n      mode: overwrite\n"
            "edges: []\n",
            encoding="utf-8",
        )
        bp = parse(str(bp_file))
        manifest = compile(bp, blueprint_path=bp_file, deployment_env="dev")
        out = next(m for m in manifest.modules if m.id == "out")
        assert out.config["path"] == "/tmp/meta_demo-dev"
