"""Extension-seam scaffolds — generated FROM the real contracts, never from prose.

Aqueduct has five "bring your own code" seams (plus the engine seam, which has its
own generator in ``aqueduct/executor/capability_tooling.py``):

    probe        a ``type: custom`` Probe signal        fn(df, sig_cfg) -> dict
    assert       a ``type: custom`` Assert rule         fn(df) -> dict
    udf          a python UDF in the ``udf_registry``   fn(*cols) -> value
    datasource   a ``format: custom`` Python DataSource pyspark DataSource subclass
    secrets      a ``provider: custom`` secrets resolver fn(key) -> str | None

Each seam has a code side (a callable/class the loader imports) and a config side
(the YAML keys that point at it). A scaffold has to get BOTH right, and both drift.

**Where the contract comes from.** Every fact this module can read from a live
object, it reads:

  - config keys come from the pydantic models themselves — ``UdfSchema``,
    ``SecretsConfig``, ``IngressSchema`` — via ``model_fields`` (with aliases),
    so renaming a field breaks generation loudly instead of emitting a key the
    parser will reject;
  - the Assert vocabulary comes from the real ``AssertRuleType`` /
    ``AssertOnFailAction`` enums;
  - the emitted custom-Probe signal is classified by the real
    ``probe_plugins.custom_signal_source()`` before it is handed back, so a
    scaffold that would not resolve at runtime cannot be produced;
  - the secrets resolver's signature is derived from the annotated return type of
    ``aqueduct.secrets.load_resolver_fn`` (``Callable[[str], Any]``);
  - the DataSource stub implements exactly the abstract methods of the installed
    ``pyspark.sql.datasource.DataSource`` — introspected, so a new Spark release
    that adds one shows up in the stub instead of in a user's traceback.

**What is NOT introspectable, and what guards it.** Two callable contracts exist
only as a call site: the Probe signal is invoked as ``fn(df, sig_cfg)`` in
``spark/probe.py`` and the Assert rule as ``fn(df)`` in ``spark/assert_.py``,
whose result is read for ``passed`` / ``message`` / ``quarantine_df``. There is no
Protocol object to read, so those signatures are stated here — and pinned by
``tests/test_cli/test_cli_dev_scaffold.py``, which parses the real call sites and
fails the build if the arity or the result keys move. Do not "simplify" that test
away: it is the only thing standing between these stubs and silent rot.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

KINDS = ("probe", "assert", "udf", "datasource", "secrets")

# The two hand-stated callable contracts (see the module docstring). Held as data
# so the pinning test can compare them against the real call sites.
PROBE_SIGNAL_ARGS = ("df", "sig_cfg")
PROBE_SIGNAL_RESULT_KEYS = ("estimate", "metadata", "passed")
ASSERT_RULE_ARGS = ("df",)
ASSERT_RULE_RESULT_KEYS = ("passed", "message", "quarantine_df")


@dataclass(frozen=True)
class Scaffold:
    """One generated extension stub: the code, and the config that points at it."""

    kind: str
    name: str  # the callable/class name
    module: str  # the python module (file stem) the code lands in
    filename: str
    code: str
    config_snippet: str
    config_target: str  # "blueprint.yml" or "aqueduct.yml"
    next_steps: tuple[str, ...] = ()


# ── contract readers (introspection) ─────────────────────────────────────────


def _field_names(model: Any) -> dict[str, str]:
    """``{field_name: yaml_key}`` for a pydantic model — alias wins, as the parser
    accepts the alias (``EdgeSchema.from_id`` is ``from:`` in YAML)."""
    return {name: (info.alias or name) for name, info in model.model_fields.items()}


def _require(keys: dict[str, str], *wanted: str) -> dict[str, str]:
    """Fail loudly if a key this scaffold writes no longer exists on the model.

    This is the anti-rot mechanism: a renamed/removed schema field breaks
    scaffold generation (and its test) instead of producing YAML the parser
    silently rejects in a user's project.
    """
    missing = [w for w in wanted if w not in keys]
    if missing:
        raise KeyError(
            f"scaffold contract drift: fields {missing} no longer exist on the schema "
            "model this scaffold is generated from — update aqueduct/dev/scaffolds.py"
        )
    return {w: keys[w] for w in wanted}


def _secrets_resolver_signature() -> tuple[str, str]:
    """Derive ``(arg, return)`` for a secrets resolver from the real loader's type."""
    import typing

    from aqueduct.secrets import load_resolver_fn

    hints = typing.get_type_hints(load_resolver_fn)
    ret = hints.get("return")
    args = typing.get_args(ret)  # (Callable[[str], Any] -> ([str], Any))
    arg_types = args[0] if args else [str]
    arg_type = getattr(arg_types[0], "__name__", "str") if arg_types else "str"
    # The value type is `Any` on the loader, but SecretsConfig.resolver documents
    # `(key: str) -> str | None` (None = fall back to os.environ) — the narrower,
    # accurate one, taken from the field description rather than invented here.
    return arg_type, "str | None"


def _datasource_contract() -> list[tuple[str, str]]:
    """``[(method, signature)]`` a batch DataSource must implement — introspected.

    ``pyspark.sql.datasource.DataSource`` declares NO ``__abstractmethods__``: its
    extension points are concrete methods whose bodies raise ``NotImplementedError``.
    So the contract is read that way (source-inspected on the INSTALLED pyspark),
    and the signatures come from ``inspect.signature`` — a Spark release that adds
    a method or changes an argument shows up in the stub instead of in a user's
    traceback. ``stream*`` methods are filtered out: Aqueduct is a batch engine
    (specs.md §11), so a streaming reader would be dead code here.
    """
    import inspect

    from pyspark.sql.datasource import DataSource

    out: list[tuple[str, str]] = []
    for name, fn in inspect.getmembers(DataSource, predicate=inspect.isfunction):
        if name.startswith("_") or "stream" in name.lower():
            continue
        try:
            src = inspect.getsource(fn)
        except OSError:  # pragma: no cover — no source (compiled/stripped install)
            continue
        if "NotImplementedError" in src:
            out.append((name, _short_sig(str(inspect.signature(fn)))))
    return sorted(out)


def _short_sig(sig: str) -> str:
    """Fully-qualified pyspark types -> the names the stub actually imports."""
    return (
        sig.replace("pyspark.sql.types.", "")
        .replace("pyspark.sql.datasource.", "")
        .replace("'", "")
    )


# ── generators ────────────────────────────────────────────────────────────────


def _probe(name: str, module: str) -> Scaffold:
    from aqueduct.executor.probe_plugins import custom_signal_source

    args = ", ".join(PROBE_SIGNAL_ARGS)
    code = f'''"""Custom Probe signal — pointed at by a `type: custom` signal in the Blueprint.

Contract (aqueduct/executor/spark/probe.py calls it as `{name}({args})`):
    {args} -> dict with keys {list(PROBE_SIGNAL_RESULT_KEYS)}

  df       the DataFrame the Probe is attached to
  sig_cfg  the signal's config block from the Blueprint (its own knobs)

Runs on the Spark DRIVER as ordinary Python — it is code, not config, with the
same trust model as a UDF. Keep it cheap: a Probe must not turn a lazy plan into
a full scan (see docs/spark_guide.md on probe cost).
"""

from __future__ import annotations

from typing import Any


def {name}(df: Any, sig_cfg: dict[str, Any]) -> dict[str, Any]:
    # TODO: compute your metric. `estimate` is the value the Regulator and the
    # observability store see; `passed` is an optional boolean gate (None = not a
    # gate); `metadata` is free-form and is persisted alongside the signal.
    threshold = sig_cfg.get("threshold")
    estimate = df.count() if sig_cfg.get("exact") else None

    return {{
        "estimate": estimate,
        "metadata": {{"threshold": threshold}},
        "passed": None,
    }}
'''
    signal = {"type": "custom", "module": module, "entry": name}
    # The real classifier — a scaffold that would not resolve cannot be emitted.
    assert custom_signal_source(signal) == "pointer"

    snippet = f"""# Blueprint — attach the signal to the module you want probed.
modules:
  - id: {name}_probe
    type: Probe
    attach_to: <module_id>          # the module whose DataFrame is probed
    config:
      signals:
        - type: custom
          module: {module}          # importable module, or `{module}.py` next to the blueprint
          entry: {name}             # the callable above
"""
    return Scaffold(
        kind="probe",
        name=name,
        module=module,
        filename=f"{module}.py",
        code=code,
        config_snippet=snippet,
        config_target="blueprint.yml",
        next_steps=(
            "Set `attach_to:` to a real module id.",
            "Run `aqueduct validate blueprint.yml` — the signal shape is checked at compile time.",
        ),
    )


def _assert_(name: str, module: str) -> Scaffold:
    from aqueduct.executor.spark.assert_ import AssertOnFailAction, AssertRuleType

    args = ", ".join(ASSERT_RULE_ARGS)
    code = f'''"""Custom Assert rule — pointed at by a `type: {AssertRuleType.CUSTOM.value}` rule's `fn:`.

Contract (aqueduct/executor/spark/assert_.py calls it as `{name}({args})`):
    {args} -> dict with keys {list(ASSERT_RULE_RESULT_KEYS)}

  passed         False fires the rule's `on_fail:` action
  message        what the user (and the healing LLM) sees when it fires
  quarantine_df  rows to divert when `on_fail: {AssertOnFailAction.QUARANTINE.value}` (None otherwise)

A rule that raises is downgraded to a warning and skipped, so failing loudly here
means returning `passed: False`, not raising.
"""

from __future__ import annotations

from typing import Any


def {name}(df: Any) -> dict[str, Any]:
    # TODO: express the invariant. Keep the Spark actions to a minimum — this runs
    # inside the pipeline, not in a notebook.
    bad = df.filter("amount < 0")
    count = bad.count()

    return {{
        "passed": count == 0,
        "message": f"{{count}} row(s) violate the rule",
        "quarantine_df": bad if count else None,
    }}
'''
    snippet = f"""# Blueprint — an Assert module carrying the custom rule.
modules:
  - id: {name}_gate
    type: Assert
    config:
      rules:
        - id: {name}
          type: {AssertRuleType.CUSTOM.value}
          fn: {module}.{name}       # dotted path; `{module}.py` may sit next to the blueprint
          on_fail: {AssertOnFailAction.WARN.value}   # {" | ".join(a.value for a in AssertOnFailAction)}
"""
    return Scaffold(
        kind="assert",
        name=name,
        module=module,
        filename=f"{module}.py",
        code=code,
        config_snippet=snippet,
        config_target="blueprint.yml",
        next_steps=(
            "Wire the Assert module into the graph with an edge from the module it guards.",
            f"`on_fail: {AssertOnFailAction.QUARANTINE.value}` needs a spillway edge to route the quarantined rows.",
        ),
    )


def _udf(name: str, module: str) -> Scaffold:
    from aqueduct.parser.schema import UdfSchema

    keys = _require(_field_names(UdfSchema), "id", "lang", "module", "entry", "return_type")
    code = f'''"""Python UDF — registered from the Blueprint's `udf_registry:` block.

The Blueprint carries a POINTER (`{keys["module"]}` + `{keys["entry"]}`), never the body: UDF
code is out of scope for self-healing, so a bug in here is a defer-to-human, not
something the agent can patch. Keep the function pure and picklable — it is
shipped to the executors with cloudpickle.
"""

from __future__ import annotations


def {name}(value):
    # TODO: one row-level value in, one out. The declared `{keys["return_type"]}` in the
    # Blueprint must match what this returns.
    if value is None:
        return None
    return str(value).strip().lower()
'''
    snippet = f"""# Blueprint — top-level `udf_registry:`, then call {name}(...) from a Channel's SQL.
udf_registry:
  - {keys["id"]}: {name}
    {keys["lang"]}: python
    {keys["module"]}: {module}      # importable module, or `{module}.py` next to the blueprint
    {keys["entry"]}: {name}         # the callable above
    {keys["return_type"]}: string   # Spark SQL type of the return value
"""
    return Scaffold(
        kind="udf",
        name=name,
        module=module,
        filename=f"{module}.py",
        code=code,
        config_snippet=snippet,
        config_target="blueprint.yml",
        next_steps=(
            f'Call it from a Channel: `sql: "SELECT {name}(col) AS col FROM input"`.',
            f"Keep `{keys['return_type']}` in step with what the function returns.",
        ),
    )


def _datasource(name: str, module: str) -> Scaffold:
    methods = _datasource_contract()
    body = "\n".join(
        f'    def {m}{sig}:\n        raise NotImplementedError("{m}")  # TODO\n'
        for m, sig in methods
    )
    code = f'''"""Custom Python DataSource — used by `format: custom` + `class:` (Spark 4.0+).

The methods below were introspected from the INSTALLED
`pyspark.sql.datasource.DataSource` (its extension points are the methods whose
bodies raise NotImplementedError — it declares no abstractmethods), so this stub
matches the Spark you are actually running against. Implement each one; Aqueduct
imports the class, verifies it subclasses DataSource, registers it with the
session, and then reads or writes through its own `name()`. Streaming methods are
omitted on purpose: Aqueduct is a batch engine.
"""

from __future__ import annotations

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceWriter,
)
from pyspark.sql.types import StructType


class {name}(DataSource):
    @classmethod
    def name(cls) -> str:
        return "{module}"   # the format name Spark registers it under

{body}'''
    snippet = f"""# Blueprint — an Ingress (or Egress) reading through the custom DataSource.
modules:
  - id: {module}_source
    type: Ingress
    config:
      format: custom
      class: {module}.{name}    # module.Class — resolvable next to the blueprint
      options: {{}}              # passed through to your DataSource
"""
    return Scaffold(
        kind="datasource",
        name=name,
        module=module,
        filename=f"{module}.py",
        code=code,
        config_snippet=snippet,
        config_target="blueprint.yml",
        next_steps=(
            "Requires Spark 4.0+ at runtime (the `spark.dataSource` registry).",
            f"Implement: {', '.join(m for m, _ in methods) or '(none)'}.",
        ),
    )


def _secrets(name: str, module: str) -> Scaffold:
    from aqueduct.config import SecretsConfig

    keys = _require(_field_names(SecretsConfig), "provider", "resolver")
    arg_type, ret_type = _secrets_resolver_signature()
    code = f'''"""Custom secrets resolver — `secrets.{keys["provider"]}: custom` + `secrets.{keys["resolver"]}:`.

Contract (aqueduct/secrets.py loads it with `load_resolver_fn`):
    ({arg_type}) -> {ret_type}      # returning None falls back to os.environ

Called on every resolution, never cached, so rotating a secret at the provider
takes effect on the next call. Do not log or return the value anywhere else: the
redaction layer only scrubs what passes through Aqueduct.
"""

from __future__ import annotations


def {name}(key: {arg_type}) -> {ret_type}:
    # TODO: fetch `key` from your vault / KMS / parameter store.
    return None
'''
    snippet = f"""# aqueduct.yml — the engine config, not the Blueprint.
secrets:
  {keys["provider"]}: custom
  {keys["resolver"]}: {module}.{name}   # dotted path to the callable above
"""
    return Scaffold(
        kind="secrets",
        name=name,
        module=module,
        filename=f"{module}.py",
        code=code,
        config_snippet=snippet,
        config_target="aqueduct.yml",
        next_steps=(
            "Reference a secret from a Blueprint with `@aq.secret('NAME')`.",
            "`aqueduct doctor` exercises the resolver without running the pipeline.",
        ),
    )


_GENERATORS = {
    "probe": _probe,
    "assert": _assert_,
    "udf": _udf,
    "datasource": _datasource,
    "secrets": _secrets,
}


def render(kind: str, name: str | None = None, module: str | None = None) -> Scaffold:
    """Generate one extension stub + the config that points at it.

    Raises:
        ValueError: unknown ``kind``.
    """
    if kind not in _GENERATORS:
        raise ValueError(f"unknown scaffold kind {kind!r}; expected one of {list(KINDS)}")
    default_names = {
        "probe": "custom_signal",
        "assert": "custom_rule",
        "udf": "custom_udf",
        "datasource": "CustomDataSource",
        "secrets": "resolve_secret",
    }
    n = name or default_names[kind]
    m = module or ("aq_" + kind if kind != "datasource" else "aq_datasource")
    return _GENERATORS[kind](n, m)


def write(scaffold: Scaffold, out_dir: Path | str = ".", force: bool = False) -> Path:
    """Write the stub's code next to the caller's blueprint. Returns the path.

    The config snippet is deliberately NOT written: it has to be merged into an
    existing blueprint/aqueduct.yml, and silently rewriting a user's file is the
    kind of help nobody asked for.

    Raises:
        FileExistsError: the target exists and ``force`` is False.
    """
    target = Path(out_dir) / scaffold.filename
    if target.exists() and not force:
        raise FileExistsError(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(scaffold.code, encoding="utf-8")
    return target
