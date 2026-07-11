"""Blueprint lifecycle hooks — schema, parser mapping, runner semantics,
static cycle walk, doctor check."""

from pathlib import Path

import pytest

from aqueduct.cli import hooks as hooks_mod
from aqueduct.parser.models import HookEntry
from aqueduct.parser.parser import ParseError, parse_dict

pytestmark = pytest.mark.unit

BASE = Path(".")


def _bp(hooks: dict) -> dict:
    return {
        "aqueduct": "1.0", "id": "bp1", "name": "BP",
        "modules": [
            {"id": "raw", "label": "R", "type": "Ingress",
             "config": {"format": "csv", "path": "d.csv"}},
            {"id": "out", "label": "O", "type": "Egress",
             "config": {"format": "parquet", "path": "o", "coalesce": 1}},
        ],
        "edges": [{"from": "raw", "to": "out"}],
        "hooks": hooks,
    }


class TestHooksParsing:
    def test_all_entry_kinds_parse_verbatim(self):
        b = parse_dict(_bp({
            "on_success": [
                {"blueprint": "next.yml"},
                {"webhook": "https://x.test/h"},
                {"webhook": {"url": "https://x.test/h2", "payload": {"r": "${run_id}"}}},
                {"command": "echo done ${run.id}", "timeout": 30},
            ],
            "on_failure": [{"command": "scripts/cleanup.sh"}],
        }), BASE)
        kinds = [e.kind for e in b.hooks.on_success]
        assert kinds == ["blueprint", "webhook", "webhook", "command"]
        # ${run.id} must survive parse untouched (runtime interpolation)
        assert "${run.id}" in b.hooks.on_success[3].value
        assert b.hooks.on_success[3].timeout == 30
        assert b.hooks.on_success[0].timeout == 300  # default
        assert bool(b.hooks) and len(b.hooks.on_failure) == 1

    @pytest.mark.parametrize("bad", [
        {},                                        # none set
        {"blueprint": "a.yml", "command": "x"},    # two set
    ])
    def test_exactly_one_action_enforced(self, bad):
        with pytest.raises(ParseError, match="exactly one"):
            parse_dict(_bp({"on_success": [bad]}), BASE)

    def test_no_hooks_is_falsy_and_serialized_empty(self):
        from aqueduct.compiler.compiler import compile as cc
        b = parse_dict(_bp({}), BASE)
        assert not b.hooks
        d = cc(b).to_dict()["hooks"]
        assert d == {"on_success": [], "on_failure": [], "on_patch_pending": [], "on_healed": []}

    def test_manifest_carries_hooks(self):
        from aqueduct.compiler.compiler import compile as cc
        b = parse_dict(_bp({"on_failure": [{"command": "x"}]}), BASE)
        m = cc(b)
        assert m.hooks.on_failure[0].kind == "command"
        assert m.to_dict()["hooks"]["on_failure"][0]["kind"] == "command"


class TestRunHooks:
    def _run(self, entries, *, allow=False, event="on_success", bp="bp.yml"):
        return hooks_mod.run_hooks(
            tuple(entries), event,
            run_id="r1", status="success", blueprint_id="bp",
            blueprint_path=bp, allow_command_hooks=allow,
        )

    def test_empty_entries_render_nothing(self):
        assert self._run([]) is False

    def test_command_gated_without_danger_flag(self, capsys):
        assert self._run([HookEntry("command", "echo nope")]) is True
        err = capsys.readouterr().err
        assert "hook_command_disabled" in err
        assert "danger.allow_command_hooks" in err

    def test_command_runs_with_interpolation(self, capsys):
        self._run([HookEntry("command", "echo hook ${run.id}")], allow=True)
        out = capsys.readouterr()
        assert "echo hook r1" in out.out  # interpolated label rendered

    def test_failing_hook_stops_remaining(self, capsys):
        self._run(
            [HookEntry("command", "false"), HookEntry("command", "echo never")],
            allow=True,
        )
        err = capsys.readouterr().err
        assert "[hook_failed]" in err
        assert "1 remaining hook(s) skipped" in err

    def test_blueprint_self_cycle_refused(self, tmp_path, capsys):
        bp = tmp_path / "a.yml"
        bp.write_text("aqueduct: '1.0'\nid: a\nname: A\nmodules: []\n")
        self._run([HookEntry("blueprint", str(bp))], bp=str(bp))
        assert "[hook_cycle]" in capsys.readouterr().err

    def test_blueprint_ancestor_cycle_refused(self, tmp_path, capsys, monkeypatch):
        bp = tmp_path / "a.yml"
        bp.write_text("aqueduct: '1.0'\nid: a\nname: A\nmodules: []\n")
        monkeypatch.setenv("AQUEDUCT_HOOK_CHAIN", str(bp.resolve()))
        self._run([HookEntry("blueprint", str(bp))], bp=str(tmp_path / "other.yml"))
        assert "[hook_cycle]" in capsys.readouterr().err

    def test_blueprint_missing_target(self, tmp_path, capsys):
        self._run([HookEntry("blueprint", "ghost.yml")], bp=str(tmp_path / "a.yml"))
        assert "blueprint not found" in capsys.readouterr().err


class TestHooksV2Schema:
    """New hooks(v2) grammar: on_patch_pending/on_healed events, when_error
    filters, in_process blueprint entries."""

    def test_on_patch_pending_and_on_healed_parse(self):
        b = parse_dict(_bp({
            "on_patch_pending": [{"webhook": "https://x.test/pending"}],
            "on_healed": [{"blueprint": "notify.yml", "in_process": True}],
        }), BASE)
        assert b.hooks.on_patch_pending[0].kind == "webhook"
        assert b.hooks.on_healed[0].kind == "blueprint"
        assert b.hooks.on_healed[0].in_process is True
        assert bool(b.hooks)  # __bool__ covers the new events too

    def test_when_error_parses_on_failure(self):
        b = parse_dict(_bp({
            "on_failure": [{"command": "x", "when_error": ["EmptyDataset", "SparkException"]}],
        }), BASE)
        assert b.hooks.on_failure[0].when_error == ("EmptyDataset", "SparkException")

    def test_when_error_rejected_on_success(self):
        with pytest.raises(ParseError, match="on_success entries cannot set when_error"):
            parse_dict(_bp({"on_success": [{"command": "x", "when_error": ["X"]}]}), BASE)

    def test_in_process_rejected_on_non_blueprint(self):
        with pytest.raises(ParseError, match="in_process: true is only valid on a blueprint"):
            parse_dict(_bp({"on_success": [{"command": "x", "in_process": True}]}), BASE)

    def test_manifest_serializes_new_events(self):
        from aqueduct.compiler.compiler import compile as cc
        b = parse_dict(_bp({
            "on_patch_pending": [{"webhook": "https://x.test"}],
            "on_healed": [{"blueprint": "n.yml"}],
        }), BASE)
        d = cc(b).to_dict()["hooks"]
        assert d["on_patch_pending"][0]["kind"] == "webhook"
        assert d["on_healed"][0]["kind"] == "blueprint"
        assert d["on_healed"][0]["when_error"] == []
        assert d["on_healed"][0]["in_process"] is False


class TestRunHooksWhenError:
    def _ctx(self, error_type=None, stack_trace=None):
        from dataclasses import dataclass

        @dataclass
        class _FakeCtx:
            error_type: str | None = None
            stack_trace: str | None = None

        return _FakeCtx(error_type=error_type, stack_trace=stack_trace)

    def test_matching_error_type_fires(self, capsys):
        hooks_mod.run_hooks(
            (HookEntry("command", "echo hi", when_error=("EmptyDataset",)),), "on_failure",
            run_id="r1", status="failure", blueprint_id="bp", blueprint_path="bp.yml",
            allow_command_hooks=True, failure_ctx=self._ctx(error_type="EmptyDataset"),
        )
        assert "echo hi" in capsys.readouterr().out

    def test_non_matching_error_type_skips_silently(self, capsys):
        rendered = hooks_mod.run_hooks(
            (HookEntry("command", "echo hi", when_error=("OtherError",)),), "on_failure",
            run_id="r1", status="failure", blueprint_id="bp", blueprint_path="bp.yml",
            allow_command_hooks=True, failure_ctx=self._ctx(error_type="EmptyDataset"),
        )
        out = capsys.readouterr()
        assert rendered is False
        assert "echo hi" not in out.out
        assert "hook_failed" not in out.err  # filtered, not a failure

    def test_stack_class_candidate_matches(self, capsys):
        hooks_mod.run_hooks(
            (HookEntry("command", "echo hi", when_error=("SparkException",)),), "on_failure",
            run_id="r1", status="failure", blueprint_id="bp", blueprint_path="bp.yml",
            allow_command_hooks=True,
            failure_ctx=self._ctx(stack_trace="Traceback...\npyspark.errors.SparkException: boom"),
        )
        assert "echo hi" in capsys.readouterr().out

    def test_unset_when_error_always_fires(self, capsys):
        hooks_mod.run_hooks(
            (HookEntry("command", "echo hi"),), "on_failure",
            run_id="r1", status="failure", blueprint_id="bp", blueprint_path="bp.yml",
            allow_command_hooks=True, failure_ctx=self._ctx(error_type="Anything"),
        )
        assert "echo hi" in capsys.readouterr().out


class TestInProcessBlueprintHook:
    def _write_simple_bp(self, path: Path, *, spark_config: dict | None = None) -> None:
        sc = ""
        if spark_config:
            sc = "spark_config:\n" + "\n".join(f"  {k}: {v}" for k, v in spark_config.items()) + "\n"
        path.write_text(
            "aqueduct: '1.0'\nid: target\nname: Target\n"
            "modules:\n"
            "  - id: c\n    label: C\n    type: Channel\n"
            "    config: {sql: 'SELECT 1 AS x'}\n"
            f"{sc}"
        )

    def test_in_process_falls_back_when_spark_config_set(self, tmp_path, capsys, monkeypatch):
        target = tmp_path / "t.yml"
        self._write_simple_bp(target, spark_config={"spark.sql.shuffle.partitions": 4})
        caller = tmp_path / "caller.yml"
        caller.write_text("aqueduct: '1.0'\nid: caller\nname: Caller\nmodules: []\n")

        class _FakeCompleted:
            returncode = 0

        # spark_config check happens before session is ever touched — a
        # sentinel object() (not a real SparkSession) is enough to enter the
        # in_process branch. Fallback then takes the normal subprocess path
        # — stub it out so this stays a fast unit test.
        monkeypatch.setattr(
            "aqueduct.cli.hooks.subprocess.run",
            lambda *a, **kw: _FakeCompleted(),
        )
        hooks_mod.run_hooks(
            (HookEntry("blueprint", "t.yml", in_process=True),), "on_success",
            run_id="r1", status="success", blueprint_id="caller", blueprint_path=str(caller),
            allow_command_hooks=False, session=object(),
        )
        out = capsys.readouterr().out
        assert "hook_inprocess_fallback" in out

    def test_in_process_skipped_without_session(self, tmp_path, capsys):
        target = tmp_path / "t.yml"
        self._write_simple_bp(target)
        caller = tmp_path / "caller.yml"
        caller.write_text("aqueduct: '1.0'\nid: caller\nname: Caller\nmodules: []\n")
        # No session passed → in_process is ignored, subprocess path used
        # (which will fail fast since `aqueduct` isn't invoked with a real
        # target in this unit test context is fine — we only assert it
        # doesn't crash and takes the subprocess branch).
        rendered = hooks_mod.run_hooks(
            (HookEntry("blueprint", "ghost.yml", in_process=True),), "on_success",
            run_id="r1", status="success", blueprint_id="caller", blueprint_path=str(caller),
            allow_command_hooks=False, session=None,
        )
        assert rendered is True
        assert "blueprint not found" in capsys.readouterr().err


class TestStaticHookCheck:
    def _write(self, path: Path, hooks_yaml: str = "") -> None:
        path.write_text(f"aqueduct: '1.0'\nid: x\nname: X\nmodules: []\n{hooks_yaml}")

    def test_healthy_chain(self, tmp_path):
        self._write(tmp_path / "b.yml")
        self._write(tmp_path / "a.yml", "hooks:\n  on_success:\n    - blueprint: b.yml\n")
        assert hooks_mod.static_hook_check(tmp_path / "a.yml") == []

    def test_cycle_detected(self, tmp_path):
        self._write(tmp_path / "a.yml", "hooks:\n  on_success:\n    - blueprint: b.yml\n")
        self._write(tmp_path / "b.yml", "hooks:\n  on_failure:\n    - blueprint: a.yml\n")
        probs = hooks_mod.static_hook_check(tmp_path / "a.yml")
        assert any("cycle" in p for p in probs)

    def test_missing_target_reported(self, tmp_path):
        self._write(tmp_path / "a.yml", "hooks:\n  on_success:\n    - blueprint: ghost.yml\n")
        probs = hooks_mod.static_hook_check(tmp_path / "a.yml")
        assert any("not found" in p for p in probs)

    def test_doctor_check_hooks(self, tmp_path):
        from aqueduct.doctor import check_hooks
        self._write(tmp_path / "plain.yml")
        assert check_hooks(tmp_path / "plain.yml").status == "skip"
        self._write(tmp_path / "a.yml", "hooks:\n  on_success:\n    - blueprint: ghost.yml\n")
        r = check_hooks(tmp_path / "a.yml")
        assert r.status == "warn" and "ghost" in r.detail


class TestValidateHookCycle:
    """`aqueduct validate` reuses `static_hook_check` — the same graph walk
    `aqueduct doctor` runs — as a suppressible WARNING (rule_id `hook_cycle`),
    never a validation failure (Phase 70)."""

    def _write_valid_bp(self, path: Path, hooks_yaml: str = "") -> None:
        path.write_text(
            "aqueduct: '1.0'\nid: bp1\nname: BP\n"
            "modules:\n"
            "  - id: raw\n    label: R\n    type: Ingress\n"
            "    config: {format: csv, path: d.csv}\n"
            "  - id: out\n    label: O\n    type: Egress\n"
            "    config: {format: parquet, path: o, coalesce: 1}\n"
            "edges:\n  - from: raw\n    to: out\n"
            f"{hooks_yaml}"
        )

    def test_hook_cycle_surfaces_as_warning_not_failure(self, tmp_path):
        from click.testing import CliRunner

        from aqueduct.cli import cli
        self._write_valid_bp(
            tmp_path / "a.yml", "hooks:\n  on_success:\n    - blueprint: b.yml\n",
        )
        self._write_valid_bp(
            tmp_path / "b.yml", "hooks:\n  on_failure:\n    - blueprint: a.yml\n",
        )
        result = CliRunner().invoke(cli, ["validate", str(tmp_path / "a.yml")])
        assert result.exit_code == 0  # a hook cycle is a warning, not an invalid file
        assert "[hook_cycle]" in result.output

    def test_hook_missing_target_surfaces_as_warning(self, tmp_path):
        from click.testing import CliRunner

        from aqueduct.cli import cli
        self._write_valid_bp(
            tmp_path / "a.yml", "hooks:\n  on_success:\n    - blueprint: ghost.yml\n",
        )
        result = CliRunner().invoke(cli, ["validate", str(tmp_path / "a.yml")])
        assert result.exit_code == 0
        assert "[hook_cycle]" in result.output
        assert "ghost" in result.output

    def test_no_hooks_no_warning(self, tmp_path):
        from click.testing import CliRunner

        from aqueduct.cli import cli
        self._write_valid_bp(tmp_path / "a.yml")
        result = CliRunner().invoke(cli, ["validate", str(tmp_path / "a.yml")])
        assert result.exit_code == 0
        assert "[hook_cycle]" not in result.output

    def test_hook_cycle_suppressible_via_blueprint_warnings_block(self, tmp_path):
        from click.testing import CliRunner

        from aqueduct.cli import cli
        self._write_valid_bp(
            tmp_path / "a.yml",
            "hooks:\n  on_success:\n    - blueprint: ghost.yml\n"
            "warnings:\n  suppress: [hook_cycle]\n",
        )
        result = CliRunner().invoke(cli, ["validate", str(tmp_path / "a.yml")])
        assert result.exit_code == 0
        assert "[hook_cycle]" not in result.output

    def test_hook_cycle_present_in_json_output(self, tmp_path):
        import json as _json

        from click.testing import CliRunner

        from aqueduct.cli import cli
        self._write_valid_bp(
            tmp_path / "a.yml", "hooks:\n  on_success:\n    - blueprint: ghost.yml\n",
        )
        result = CliRunner().invoke(cli, ["validate", "--format", "json", str(tmp_path / "a.yml")])
        assert result.exit_code == 0
        data = _json.loads(result.output)
        assert any(f.get("hook_warnings") for f in data["files"])
