"""`aqueduct dev scaffold <kind>` — the extension-seam stubs.

Two kinds of test here, and they do different jobs:

1. ACCEPTANCE. Each generated stub, plus the config snippet printed next to it, is
   pushed through the REAL loader for that seam (`probe_plugins.resolve_callable`,
   `assert_._load_callable`, `udf`'s module loader, `secrets.load_resolver_fn`,
   `custom_source.import_datasource_class`) and the real parser. A scaffold that
   produces code the loader cannot import, or YAML the parser rejects, is worthless
   — so the test is the loader, not a string comparison.

2. CONTRACT PINNING. Two callable contracts have no introspectable object: the
   custom Probe signal (`fn(df, sig_cfg)`) and the custom Assert rule (`fn(df)`,
   returning `passed` / `message` / `quarantine_df`). They exist only as a call
   site in the Spark executor. The stubs therefore state them, and the tests below
   parse those call sites with `ast` and fail if the arity or the result keys move.
   That is the whole defence against the stub rotting silently — do not delete it.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.dev import scaffolds

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]


def _scaffold(tmp_path: Path, kind: str, **opts) -> scaffolds.Scaffold:
    args = ["dev", "scaffold", kind, "--out", str(tmp_path)]
    for k, v in opts.items():
        args += [f"--{k}", v]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    return scaffolds.render(kind, name=opts.get("name"), module=opts.get("module"))


# ── registration ─────────────────────────────────────────────────────────────


def test_scaffold_command_covers_every_seam():
    assert "scaffold" in cli.commands["dev"].commands
    assert set(scaffolds.KINDS) == {"probe", "assert", "udf", "datasource", "secrets"}


# ── acceptance: the stub loads through the seam's REAL loader ────────────────


def test_probe_stub_loads_through_the_real_probe_resolver(tmp_path, monkeypatch):
    from aqueduct.executor.probe_plugins import custom_signal_source, resolve_callable

    sc = _scaffold(tmp_path, "probe", name="p99", module="aq_probe")
    signal = yaml.safe_load(sc.config_snippet)["modules"][0]["config"]["signals"][0]

    # The real classifier accepts the emitted signal…
    assert custom_signal_source(signal) == "pointer"
    # …and the real resolver imports the emitted file as a sibling of the blueprint.
    fn = resolve_callable(signal, base_dir=str(tmp_path))
    assert callable(fn)

    class _FakeDF:
        def count(self):
            return 7

    out = fn(_FakeDF(), {"exact": True, "threshold": 10})
    assert set(out) == {"estimate", "metadata", "passed"}
    assert out["estimate"] == 7


def test_assert_stub_loads_through_the_real_assert_loader(tmp_path):
    from aqueduct.executor.spark.assert_ import _load_callable

    sc = _scaffold(tmp_path, "assert", name="no_negative_amounts", module="aq_rules")
    rule = yaml.safe_load(sc.config_snippet)["modules"][0]["config"]["rules"][0]

    fn = _load_callable(rule["fn"], str(tmp_path))
    assert callable(fn)

    class _FakeDF:
        def filter(self, expr):
            assert "amount" in expr
            return self

        def count(self):
            return 0

    out = fn(_FakeDF())
    assert out["passed"] is True
    assert set(out) == {"passed", "message", "quarantine_df"}


def test_udf_stub_loads_through_the_real_udf_loader_and_schema(tmp_path):
    from aqueduct.infra.module_loading import load_module
    from aqueduct.parser.schema import UdfSchema

    sc = _scaffold(tmp_path, "udf", name="normalise", module="aq_udfs")
    entry = yaml.safe_load(sc.config_snippet)["udf_registry"][0]

    # The emitted YAML validates against the REAL schema model (extra="forbid",
    # so a stale key would fail here).
    udf = UdfSchema.model_validate(entry)
    assert udf.id == "normalise" and udf.lang == "python"

    mod = load_module(udf.module, str(tmp_path))
    fn = getattr(mod, udf.entry)
    assert fn("  ABC ") == "abc"
    assert fn(None) is None


def test_secrets_stub_loads_through_the_real_resolver_loader(tmp_path):
    from aqueduct.config import SecretsConfig
    from aqueduct.secrets import load_resolver_fn

    sc = _scaffold(tmp_path, "secrets", name="fetch", module="aq_secrets")
    block = yaml.safe_load(sc.config_snippet)["secrets"]

    cfg = SecretsConfig.model_validate(block)  # real config model
    assert cfg.provider == "custom"

    fn = load_resolver_fn(cfg.resolver, str(tmp_path))
    assert fn("ANYTHING") is None  # the stub's honest default: fall back to os.environ


def test_datasource_stub_loads_through_the_real_datasource_importer(tmp_path):
    pytest.importorskip("pyspark")
    from aqueduct.executor.spark.custom_source import import_datasource_class

    sc = _scaffold(tmp_path, "datasource", name="AcmeSource", module="aq_source")
    cfg = yaml.safe_load(sc.config_snippet)["modules"][0]["config"]
    assert cfg["format"] == "custom"

    cls = import_datasource_class(cfg["class"], str(tmp_path))  # validates the subclass
    assert cls.name() == "aq_source"


def test_datasource_stub_implements_the_installed_pyspark_contract(tmp_path):
    """Introspected, not hardcoded: the stub carries exactly the extension points
    the INSTALLED pyspark DataSource declares (minus the streaming ones — Aqueduct
    is a batch engine). A Spark release that adds one lands in the stub."""
    pytest.importorskip("pyspark")

    sc = scaffolds.render("datasource")
    expected = [m for m, _ in scaffolds._datasource_contract()]
    assert expected, "no DataSource extension points found — introspection broke"
    defined = {n.name for n in ast.walk(ast.parse(sc.code)) if isinstance(n, ast.FunctionDef)}
    assert set(expected) <= defined
    assert not any("stream" in m.lower() for m in defined)


# ── contract pinning: the two seams with no introspectable object ────────────


def _call_args_at(path: Path, callee: str) -> list[str]:
    """Argument names of every `callee(...)` call in a source file."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out: list[str] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == callee
        ):
            out.append([a.id if isinstance(a, ast.Name) else "?" for a in node.args])
    return out


def test_probe_signal_contract_matches_the_real_call_site():
    """`spark/probe.py` invokes the resolved callable with 2 positional args. If that
    changes, this fails — and the stub in aqueduct/dev/scaffolds.py must change with
    it, rather than shipping a signature nobody calls."""
    calls = _call_args_at(_REPO / "aqueduct" / "executor" / "spark" / "probe.py", "fn")
    assert calls, "no `fn(...)` call found in probe.py — the custom-signal seam moved"
    assert any(len(args) == len(scaffolds.PROBE_SIGNAL_ARGS) for args in calls), (
        f"probe.py calls the custom signal with {calls}, but the scaffold generates "
        f"{scaffolds.PROBE_SIGNAL_ARGS} — update aqueduct/dev/scaffolds.py"
    )


def test_assert_rule_contract_matches_the_real_call_site():
    """`spark/assert_.py` calls the rule as `fn(df)` and reads three result keys."""
    src = (_REPO / "aqueduct" / "executor" / "spark" / "assert_.py").read_text(encoding="utf-8")
    calls = _call_args_at(_REPO / "aqueduct" / "executor" / "spark" / "assert_.py", "fn")
    assert any(len(args) == len(scaffolds.ASSERT_RULE_ARGS) for args in calls), (
        f"assert_.py calls the custom rule with {calls}, but the scaffold generates "
        f"{scaffolds.ASSERT_RULE_ARGS} — update aqueduct/dev/scaffolds.py"
    )
    for key in scaffolds.ASSERT_RULE_RESULT_KEYS:
        assert f'result.get("{key}"' in src or f'"{key}"' in src, (
            f"assert_.py no longer reads {key!r} from a custom rule's result — "
            "the scaffold still tells users to return it"
        )


def test_config_keys_come_from_the_schema_models_not_string_literals():
    """The generators read `model_fields` and raise on drift. Rename a field and the
    scaffold must fail LOUDLY rather than emit YAML the parser will reject."""
    with pytest.raises(KeyError, match="scaffold contract drift"):
        scaffolds._require({"id": "id"}, "id", "gone_field")


# ── mechanics ────────────────────────────────────────────────────────────────


def test_scaffold_refuses_to_overwrite_without_force(tmp_path):
    args = ["dev", "scaffold", "udf", "--out", str(tmp_path)]
    runner = CliRunner()
    assert runner.invoke(cli, args).exit_code == 0
    again = runner.invoke(cli, args)
    assert again.exit_code != 0
    assert "already exists" in again.output
    assert runner.invoke(cli, [*args, "--force"]).exit_code == 0


@pytest.mark.parametrize("kind", ["probe", "assert", "udf", "secrets"])
def test_every_stub_is_importable_python(tmp_path, kind):
    """A stub that does not even compile is worse than no stub."""
    sc = _scaffold(tmp_path, kind)
    compile((tmp_path / sc.filename).read_text(encoding="utf-8"), sc.filename, "exec")
    assert sc.config_target in ("blueprint.yml", "aqueduct.yml")
    assert yaml.safe_load(sc.config_snippet)  # the snippet is real YAML


def test_scaffold_kind_is_validated():
    result = CliRunner().invoke(cli, ["dev", "scaffold", "nonsense"])
    assert result.exit_code != 0
    with pytest.raises(ValueError, match="unknown scaffold kind"):
        scaffolds.render("nonsense")


def test_scaffold_module_is_python_311_compatible(tmp_path):
    """The floor is 3.11 (AGENTS.md): a stub must parse there too."""
    assert sys.version_info >= (3, 11)
    sc = _scaffold(tmp_path, "probe")
    ast.parse((tmp_path / sc.filename).read_text(encoding="utf-8"))
