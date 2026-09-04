"""Chained multi-patch healing is the only heal-loop behavior.

Replaces the old opt-in-flag suite: `agent.progressive` / `agent.max_chain`
no longer exist, so what is worth testing is (a) the sandbox guard that now
keys off `agent.max_patches` instead of the deleted flag, and (b) that the
deleted keys are actually rejected rather than silently ignored.

The end-to-end chain semantics (carry a proven patch forward on a
different-module failure, discard-and-retry on the same module, one staged
patch, nothing on disk mid-loop) live in
`tests/test_cli/test_cli_chained_healing_loop.py`.
"""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

# The `run` command imports the Spark executor at function entry
# (aqueduct/cli/run.py `from aqueduct.executor import ExecuteError`), BEFORE
# any config gate can fire — same dependency as every other run-command
# config-gate test in this directory. ImportError happens at CLI-invoke time
# (not collection), so this must be an explicit importorskip.
pytest.importorskip("pyspark", reason="pyspark not installed — install aqueduct-core[spark]")

from aqueduct import exit_codes
from aqueduct.cli import cli

pytestmark = pytest.mark.integration

_BP_TEMPLATE = """\
aqueduct: '1.0'
id: test_bp
name: Test BP
agent:
  approval: {approval}
  max_patches: {max_patches}
  sandbox_mode: "{sandbox}"
modules:
  - id: src
    type: Ingress
    label: Src
    config: {{format: csv, path: /nonexistent/data.csv}}
edges: []
"""

_CFG_TEMPLATE = """\
aqueduct_config: "1.0"
agent:
  provider: openai_compat
  base_url: "http://localhost:8000"
danger:
  allow_skip_sandbox: {allow_skip}
"""


def _write_project(tmp_path, *, max_patches, sandbox, allow_skip=True, approval="auto"):
    bp = tmp_path / "bp.yml"
    bp.write_text(
        _BP_TEMPLATE.format(approval=approval, max_patches=max_patches, sandbox=sandbox),
        encoding="utf-8",
    )
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(_CFG_TEMPLATE.format(allow_skip=str(allow_skip).lower()), encoding="utf-8")
    return bp, cfg


# ── the sandbox guard now keys off max_patches, not a deleted flag ──────────


class TestSandboxGuard:
    """`require_sandbox_for_chained_healing` refuses only when it can chain.

    A single-attempt heal never folds a candidate into an accumulated patch,
    so it has nothing to validate mid-chain and `sandbox_mode: off` stays
    legal there (it is separately gated by `danger.allow_skip_sandbox`).
    """

    def test_chaining_with_sandbox_off_raises(self):
        from aqueduct.cli.run_setup import require_sandbox_for_chained_healing
        from aqueduct.errors import ConfigError

        with pytest.raises(ConfigError) as exc:
            require_sandbox_for_chained_healing(3, "off")
        # The message must name the surviving keys, never the deleted flag.
        msg = str(exc.value)
        assert "max_patches" in msg
        assert "sandbox_mode" in msg
        assert "progressive" not in msg

    def test_single_attempt_with_sandbox_off_is_allowed(self):
        """The whole point of scoping the guard: this must NOT raise."""
        from aqueduct.cli.run_setup import require_sandbox_for_chained_healing

        assert require_sandbox_for_chained_healing(1, "off") is None

    @pytest.mark.parametrize("mode", ["sample", "preflight"])
    def test_chaining_with_a_real_sandbox_is_allowed(self, mode):
        from aqueduct.cli.run_setup import require_sandbox_for_chained_healing

        assert require_sandbox_for_chained_healing(3, mode) is None


@patch("aqueduct.executor.get_executor")
def test_run_refuses_chaining_with_sandbox_off(mock_get_exec, tmp_path):
    """End-to-end: max_patches > 1 + sandbox_mode: off exits CONFIG_ERROR.

    The guard fires during run setup, before any engine session is built, so
    the executor mock only has to keep the import from doing real work."""
    mock_get_exec.return_value = MagicMock()
    bp, cfg = _write_project(tmp_path, max_patches=3, sandbox="off", allow_skip=True)
    result = CliRunner().invoke(cli, ["run", str(bp), "--config", str(cfg)])
    assert result.exit_code == exit_codes.CONFIG_ERROR, result.output
    assert "max_patches" in result.output


# ── the deleted keys are rejected, not ignored ──────────────────────────────


@pytest.mark.parametrize("key,value", [("progressive", "true"), ("max_chain", "3")])
def test_deleted_agent_keys_are_rejected(tmp_path, key, value):
    """`extra="forbid"` must name the key — a silent no-op would be worse."""
    from aqueduct.parser import ParseError, parse

    bp = tmp_path / "bp.yml"
    bp.write_text(
        "aqueduct: '1.0'\n"
        "id: test_bp\n"
        "name: Test BP\n"
        "agent:\n"
        "  approval: auto\n"
        f"  {key}: {value}\n"
        "modules:\n"
        "  - id: src\n"
        "    type: Ingress\n"
        "    label: Src\n"
        "    config: {format: csv, path: /nonexistent/data.csv}\n"
        "edges: []\n",
        encoding="utf-8",
    )
    with pytest.raises(ParseError) as exc:
        parse(bp)
    assert key in str(exc.value)
