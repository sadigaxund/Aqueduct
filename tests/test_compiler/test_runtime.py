"""Tests for the Compiler layer: Tier 1 runtime functions."""

from __future__ import annotations
from datetime import date
from pathlib import Path
import pytest
from aqueduct.compiler.runtime import AqFunctions, resolve_tier1_str
from aqueduct.compiler.compiler import compile
from aqueduct.parser.parser import parse

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
        result = resolve_tier1_str("@aq.runtime.run_id()", self.reg)
        assert result == "test-run-001"

    def test_runtime_prev_run_id_empty(self):
        result = resolve_tier1_str("@aq.runtime.prev_run_id()", self.reg)
        assert result == ""

    def test_runtime_prev_run_id_exists(self, tmp_path):
        from aqueduct.depot.depot import DepotStore
        store = DepotStore(tmp_path / "depot.db")
        store.put("_last_run_id", "test-run-999")
        
        reg = AqFunctions(run_id="test-run-001", depot=store)
        result = resolve_tier1_str("@aq.runtime.prev_run_id()", reg)
        assert result == "test-run-999"

    def test_runtime_timestamp_is_iso(self):
        result = resolve_tier1_str("@aq.runtime.timestamp()", self.reg)
        from datetime import datetime
        dt = datetime.fromisoformat(result)
        assert dt.tzinfo is not None

    def test_env_function(self, monkeypatch):
        monkeypatch.setenv("MY_TEST_VAR", "hello")
        result = resolve_tier1_str("@aq.env('MY_TEST_VAR')", self.reg)
        assert result == "hello"

    def test_env_missing_raises(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(RuntimeError, match="not set"):
            resolve_tier1_str("@aq.env('MISSING_VAR')", self.reg)

    def test_depot_get_default_when_no_depot(self):
        result = resolve_tier1_str("@aq.depot.get('key', 'fallback')", self.reg)
        assert result == "fallback"

    def test_depot_get_empty_default(self):
        result = resolve_tier1_str("@aq.depot.get('some.key')", self.reg)
        assert result == ""

    def test_nested_call_resolved(self):
        result = resolve_tier1_str(
            "@aq.date.offset(base='@aq.date.today()', days=1)", self.reg
        )
        from datetime import timedelta
        assert result == (date.today() + timedelta(days=1)).isoformat()

    def test_tier1_in_string_context(self):
        result = resolve_tier1_str("s3://bucket/@aq.date.today()/data", self.reg)
        assert result == f"s3://bucket/{date.today().isoformat()}/data"

    def test_unknown_function_raises(self):
        with pytest.raises(ValueError, match="Unknown @aq function"):
            resolve_tier1_str("@aq.does.not.exist()", self.reg)

    def test_tier1_resolved_in_manifest_context(self, tmp_path):
        bp_file = tmp_path / "tier1_ctx.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "context:\n"
            "  run_date: '@aq.date.today()'\n"
            "  path: \"data/${ctx.run_date}/output\"\n"
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
        ts = aq.runtime_timestamp()
        parsed = datetime.fromisoformat(ts)
        assert parsed.hour == 0
        assert parsed.tzinfo is not None


class TestCompilerEdgeCases:
    """Cover uncovered compiler paths."""

    def test_post_tier1_ctx_reresolution(self, tmp_path):
        """Context values that contain ${ctx.*} after Tier1 resolution should be re-resolved."""
        from aqueduct.parser.parser import parse
        from aqueduct.compiler.compiler import compile as compiler_compile
        from unittest.mock import MagicMock
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
        from aqueduct.parser.parser import parse
        from aqueduct.compiler.compiler import compile as compiler_compile
        from unittest.mock import MagicMock
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
