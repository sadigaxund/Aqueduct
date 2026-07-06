"""Custom probe signal plugins (Phase 60) — engine-agnostic resolver.

Lives at the executor top level (not under ``spark/``) so it stays
``pyspark``-free and can be imported by BOTH the compiler (``wirer`` — for
compile-time shape validation) and the Spark executor (``probe.py`` — for
runtime resolution). Same precedent as ``executor/path_keys.py``.

A ``type: custom`` probe signal resolves to one of three forms (exactly one):

  - **inline SQL** — ``sql:`` (a value expression) and/or ``passed_when:`` (a
    boolean gate expression). No callable; evaluated directly in ``probe.py``.
  - **module pointer** — ``module:`` + ``entry:`` (mirrors the UDF contract).
  - **entry-point plugin** — ``plugin: <name>`` resolved from the setuptools
    entry-point group ``aqueduct.probe_signals``.

Callable contract::

    fn(df, sig_cfg) -> {"estimate": Any, "metadata": dict, "passed": bool | None}

SECURITY: resolved callables run on the Spark **driver** as ordinary Python —
they are CODE, not config, with the same trust model as UDFs (an importable
module or an installed package, never an inline body in the blueprint YAML). The
blueprint only carries a *pointer*; the body never enters the Manifest, so it is
never surfaced to the healing LLM. Only install probe plugins you trust.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from aqueduct.errors import ConfigError

# setuptools entry-point group for distributing custom probe signals.
AQ_PROBE_ENTRYPOINT_GROUP = "aqueduct.probe_signals"

CustomSignalFn = Callable[..., dict]


def custom_signal_source(sig_cfg: dict[str, Any]) -> str:
    """Classify a ``custom`` signal's resolution form.

    Returns one of ``"sql"`` | ``"pointer"`` | ``"plugin"``.

    Raises:
        ValueError: zero forms, more than one form, or a partial pointer.
    """
    has_sql = bool(sig_cfg.get("sql") or sig_cfg.get("passed_when"))
    has_pointer = bool(sig_cfg.get("module") or sig_cfg.get("entry"))
    has_plugin = bool(sig_cfg.get("plugin"))

    forms = [
        name
        for name, present in (
            ("sql", has_sql),
            ("pointer", has_pointer),
            ("plugin", has_plugin),
        )
        if present
    ]
    if not forms:
        raise ConfigError(
            "custom probe signal requires exactly one source: 'sql'/'passed_when', "
            "'module'+'entry', or 'plugin'"
        )
    if len(forms) > 1:
        raise ConfigError(
            f"custom probe signal has conflicting sources {forms}; specify exactly one of "
            "'sql'/'passed_when', 'module'+'entry', or 'plugin'"
        )
    if forms[0] == "pointer" and not (sig_cfg.get("module") and sig_cfg.get("entry")):
        raise ConfigError(
            "custom probe signal pointer form requires BOTH 'module' and 'entry'"
        )
    return forms[0]


def resolve_callable(sig_cfg: dict[str, Any], base_dir: str | None = None) -> CustomSignalFn:
    """Import the callable for a ``pointer`` or ``plugin`` custom signal.

    ``base_dir`` (Manifest.base_dir) lets ``module: probes`` resolve a
    ``probes.py`` sitting next to the blueprint — see
    ``aqueduct/infra/module_loading.py``. Stays pyspark-free (executor-agnostic
    module, imported outside ``executor/spark/``).

    Raises:
        ValueError: the form is ``sql`` (no callable), the target is missing, or
            it is not callable.
        ImportError: the pointed-at module cannot be imported.
    """
    source = custom_signal_source(sig_cfg)
    if source == "pointer":
        module_name = sig_cfg["module"]
        entry = sig_cfg["entry"]
        from aqueduct.infra.module_loading import load_module

        mod = load_module(module_name, base_dir)
        fn = getattr(mod, entry, None)
        if not callable(fn):
            raise ConfigError(
                f"custom probe: {module_name}:{entry} is not a callable"
            )
        return fn
    if source == "plugin":
        name = sig_cfg["plugin"]
        fn = _load_entry_point(name)
        if fn is None:
            raise ConfigError(
                f"custom probe plugin {name!r} not found in entry-point group "
                f"{AQ_PROBE_ENTRYPOINT_GROUP!r}"
            )
        return fn
    raise ConfigError(
        "resolve_callable() called for an inline-SQL custom signal (no callable)"
    )


def _load_entry_point(name: str) -> CustomSignalFn | None:
    """Look up a probe-signal callable by name in the entry-point group."""
    from importlib.metadata import entry_points

    for ep in entry_points().select(group=AQ_PROBE_ENTRYPOINT_GROUP):
        if ep.name == name:
            return ep.load()
    return None
