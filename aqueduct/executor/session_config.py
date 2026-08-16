"""Engine-config resolution for ``SessionSpec`` — one engine's session config,
resolved from ``AqueductConfig``.

Split out of ``aqueduct/executor/protocol.py`` (Phase 82 remediation) so that
module stays a pure ``ExecutorProtocol`` *contract* declaration — engines
register against it, but these two functions are ordinary session-config
helpers, not part of the contract an engine implements. Keeping them out of
``protocol.py`` keeps that file's public surface (the thing a future
bring-your-own-engine author reads, and the thing
``docs/extending.md``/``AGENTS.md`` track for "has the protocol stopped
changing") limited to the contract itself.

Every ``SessionSpec`` builder resolves through here — the real ``aqueduct
run`` path (``aqueduct/cli/run.py``, single-engine and polyglot) AND the
patch preview sandbox gate (``aqueduct/patch/preview.py::run_sandbox_gate``)
— so a sandbox replay sees the SAME engine config a real run would use.
``patch/`` and ``cli/`` both legitimately import from ``executor/`` (the
4-layer boundary runs Parser -> Compiler -> Executor -> Surveyor; ``patch/``
sits downstream of Surveyor, ``cli/`` sits above all four), so this is the
shared home that respects the layer direction — ``patch/`` importing from
``cli/`` would not.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aqueduct.config import AqueductConfig

__all__ = [
    "cli_engine_config_layers",
    "engine_config_base_layer",
    "engine_config_set_path",
    "resolve_effective_engine_configs",
    "resolve_session_engine_config",
    "session_config_fingerprint",
    "session_secrets_options",
]


def engine_config_base_layer(cfg: AqueductConfig, engine: str) -> dict[str, Any] | None:
    """One engine's ``aqueduct.yml``-level config as a flat dict, or None
    when ``aqueduct.yml``'s schema declares no block for *engine* at all.

    The branch is STRUCTURAL, never a hardcoded engine name: an engine whose
    own config block declares a ``conf`` sub-field (today: Spark) keeps a
    free-form bag of arbitrary native keys and the block IS that bag; an
    engine that declares typed fields instead (today: DuckDB —
    ``memory_limit``/``threads``/``database_path``/``s3_*``) contributes its
    whole dumped sub-model, so a field THAT engine declares flows through to
    its session factory the day it is added. This is the same structural
    question ``aqueduct.patch.config_delta.blueprint_engine_layers`` and
    ``aqueduct.parser.parser._resolve_engine_block_raw`` ask on the
    Blueprint side, asked here against the ``aqueduct.yml`` schema.
    """
    engine_cfg = getattr(cfg.engine, engine, None)
    if engine_cfg is None:
        return None
    if "conf" in type(engine_cfg).model_fields:
        return dict(engine_cfg.conf)
    return engine_cfg.model_dump()


def engine_config_set_path(cfg: AqueductConfig, engine: str, key: str) -> str:
    """The ``-s/--set`` path that addresses one engine-config *key*.

    Same structural branch as ``engine_config_base_layer``: an engine whose
    block carries a free-form ``conf`` bag is addressed through it
    (``engine.spark.conf.spark.sql.shuffle.partitions``), a typed block
    directly (``engine.duckdb.memory_limit``). Used to quote the user's own
    flag back at them when a ``--set`` shadows something.
    """
    engine_cfg = getattr(cfg.engine, engine, None)
    if engine_cfg is not None and "conf" in type(engine_cfg).model_fields:
        return f"engine.{engine}.conf.{key}"
    return f"engine.{engine}.{key}"


def cli_engine_config_layers(
    cfg: AqueductConfig, config_set_nested: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    """``{engine: {key: value}}`` for the engine-config keys ``-s/--set``
    named on this invocation.

    *config_set_nested* is ``aqueduct.overrides.route_overrides``' own
    config-side nesting (``{"engine": {"spark": {"conf": {...}}}}``). Only
    the KEY SET is read from it; every VALUE is read back out of *cfg*,
    which has already had the override validated into it by
    ``apply_to_model``. Taking the value from the validated model rather
    than the raw CLI token is what keeps a typed engine field
    (``--set engine.duckdb.threads=8``) identical in this layer and in the
    ``aqueduct.yml`` layer beneath it, instead of one being the coerced
    Python value and the other whatever the flag parser produced.

    A key naming a field the engine's block does not declare cannot reach
    here: ``route_overrides`` already refuses a path no schema accepts.
    """
    engine_nested = (config_set_nested or {}).get("engine")
    if not isinstance(engine_nested, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for engine in type(cfg.engine).model_fields:
        named = engine_nested.get(engine)
        if not isinstance(named, dict):
            continue
        engine_cfg = getattr(cfg.engine, engine, None)
        if engine_cfg is None:  # pragma: no cover - schema field with no value
            continue
        if "conf" in type(engine_cfg).model_fields:
            named = named.get("conf")
            named = named if isinstance(named, dict) else {}
        resolved = engine_config_base_layer(cfg, engine) or {}
        layer = {key: resolved[key] for key in named if key in resolved}
        if layer:
            out[engine] = layer
    return out


def resolve_session_engine_config(
    cfg: AqueductConfig,
    engine: str,
    manifest: Any,
) -> dict[str, Any]:
    """Build one engine's ``SessionSpec.engine_config`` dict.

    **This function is the ONE statement of engine-config precedence.**
    Three layers, lowest first:

      1. ``aqueduct.yml``'s ``engine.<name>:`` block
         (``engine_config_base_layer``, above).
      2. The Blueprint's own ``engine.<name>:`` block, carried on
         ``Manifest.engine_config`` — this is also where a healed
         ``set_engine_config`` patch writes.
      3. This invocation's ``-s/--set`` overrides
         (``cfg.cli_engine_overrides``).

    Layer 3 is the correction to an inverted precedence: ``--set`` used to
    overlay layer 1 only, so a value a heal had written into the Blueprint
    months earlier beat the flag the user typed thirty seconds ago. A CLI
    flag is the most explicit statement a user can make about one run, and
    it was silently the weakest input in the merge. It wins now, and it is
    safe for it to win precisely because it is per-invocation and never
    written back to any file: it can override a heal for one run, never undo
    one.

    Layers 1 and 2 used to be Spark's own special-cased merge
    (``engine.spark.conf`` + ``manifest.spark_config``), with every other
    engine silently getting ONLY its ``aqueduct.yml`` config and no way for
    a Blueprint to override it. Every registered engine now gets the same
    three layers, with the layer-1 shape resolved structurally rather than
    by engine name (see ``engine_config_base_layer``).

    Every caller that builds a real execution session for an engine — the
    ``aqueduct run`` path (single-engine and polyglot) AND the patch preview
    sandbox gate — MUST resolve ``engine_config`` through this function.
    Building ``SessionSpec.engine_config`` any other way (e.g. hardcoding
    ``manifest.spark_config`` regardless of the target engine, the bug this
    function was originally extracted to fix) silently discards every
    ``engine.<name>.*`` field a non-Spark engine declares, any
    Blueprint-level override for it, and now the user's ``--set`` as well.

    An engine with no ``cfg.engine.<name>:`` block (``engine_config_base_layer``
    returns None — e.g. a third-party BYO engine that is not a field on the
    closed ``AqueductConfig.EngineConfig`` model) is NOT a wall: layer 1 is
    simply empty and layers 2 and 3 still apply. Returning ``{}`` outright
    used to drop the Blueprint and ``--set`` layers for exactly the engines
    that most need a way to be configured without a schema change — a typed
    error was considered instead and rejected for the same reason.
    """
    base = engine_config_base_layer(cfg, engine) or {}
    blueprint_override = manifest.engine_config.get(engine, {})
    cli_override = cfg.cli_engine_overrides.get(engine, {})
    return {**base, **blueprint_override, **cli_override}


class _BlueprintLayerOnly:
    """Minimal ``manifest``-shaped carrier for ``resolve_session_engine_config``.

    That function reads exactly one attribute off its ``manifest`` argument
    (``.engine_config``), so a Blueprint-level override map that has NOT been
    compiled into a ``Manifest`` yet — the patch gates run on a raw Blueprint
    dict, before any compile — can be layered through the SAME resolver a
    real run uses. Deliberately not a public re-implementation of the merge:
    the whole point is that there is one precedence rule and one function
    that knows it.
    """

    __slots__ = ("engine_config",)

    def __init__(self, engine_config: dict[str, dict[str, Any]]) -> None:
        self.engine_config = engine_config


def resolve_effective_engine_configs(
    cfg: AqueductConfig,
    blueprint_engine_config: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Effective session config for EVERY engine ``cfg`` knows about.

    ``blueprint_engine_config`` is the Blueprint-level ``engine:`` block in
    the same shape ``Manifest.engine_config`` carries (engine name -> that
    engine's override dict). Returns engine name -> the config that engine
    would actually run with, each resolved through
    ``resolve_session_engine_config`` so the layering rule is stated once.

    Engine set comes from ``cfg.engine``'s own model fields — every engine
    the installed config schema declares — never from the keys present in
    ``blueprint_engine_config``. Reading the engine list off the Blueprint
    would make the answer depend on which engines the author happened to
    mention, so a write into a block the Blueprint had not previously
    declared would be compared against nothing.
    """
    out: dict[str, dict[str, Any]] = {}
    for engine in type(cfg.engine).model_fields:
        shim = _BlueprintLayerOnly({engine: dict(blueprint_engine_config.get(engine) or {})})
        out[engine] = resolve_session_engine_config(cfg, engine, shim)
    return out


def session_config_fingerprint(cfg: AqueductConfig, engine: str, manifest: Any) -> str:
    """Deterministic fingerprint of the session-determining config for one
    (``cfg``, ``engine``, ``manifest``) triple — the thing a live engine
    session was built FROM, cheap to recompute and compare on every
    execution so a session built from one Manifest is never reused to
    execute a DIFFERENT Manifest (cross-engine remediation).

    Deliberately narrow: it hashes ONLY ``resolve_session_engine_config``'s
    output, not every ``SessionSpec`` field. Within one ``aqueduct run``
    process, ``cfg`` is loaded exactly once (a heal patch rewrites the
    in-memory ``Manifest`` it retries, never ``aqueduct.yml``) and the run's
    target ``engine`` never changes either — so ``master_url``
    (``cfg.engine.<x>.master_url``), ``timezone`` (``cfg.timezone``), and
    ``engine_options``/``session_secrets_options`` (``cfg.secrets.*`` plus
    ``manifest.base_dir``, which stays the patched blueprint's own directory
    across every heal iteration) are all constant for the run's lifetime and
    would only add dead weight to the comparison. ``engine_config`` is the
    ONE part that can differ between two Manifests in the same run: the
    ``set_engine_config`` PatchSpec op is the only op that rewrites
    ``Manifest.engine_config``, and ``resolve_session_engine_config`` is
    exactly the function that folds it in (Blueprint-level override on top
    of the engine's ``aqueduct.yml`` config). Any future session-determining
    field must be added HERE (not layered on as a separate check elsewhere)
    to stay exclusion-safe — see the "classify by what you EXCLUDE" rule in
    AGENTS.md.

    **The ``-s/--set`` layer needs no addition here, by construction.** It
    is layer 3 INSIDE ``resolve_session_engine_config``, so it is already
    part of the hashed output — a config carrying ``--set`` and one without
    it fingerprint differently for the same Manifest, which is exactly the
    invariant ("a session built with ``--set`` is never reused for a
    Manifest resolved without it"). Adding the flag as a separate term here
    would have been the wrong shape twice over: it would double-count a
    value already in the hash, and it would put the second half of the
    precedence rule somewhere other than the resolver. Note the useful
    consequence within one run: a heal that writes a Blueprint value the
    user's ``--set`` shadows produces the SAME fingerprint, so no session
    rebuild is triggered — correctly, because the session's actual config
    did not change.

    Equal ``resolve_session_engine_config()`` output, for the SAME (cfg,
    engine) pair, always produces the same fingerprint — the property the
    session-rebuild-on-mismatch check at the execution funnel depends on to
    stay a no-op when nothing session-relevant changed.
    """
    resolved = resolve_session_engine_config(cfg, engine, manifest)
    canonical = json.dumps(resolved, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def session_secrets_options(cfg: AqueductConfig, manifest: Any) -> dict[str, Any]:
    """Build the ``secrets`` entry of ``SessionSpec.engine_options``.

    The resolved ``secrets:`` block (provider/region/resolver/base_dir),
    passed through so an engine that needs to resolve a secret KEY NAME into
    a VALUE (DuckDB's ``engine.duckdb.s3_key_id_secret`` -> DuckDB's own
    ``CREATE SECRET``) calls the SAME ``aqueduct.secrets.resolve_secret``
    ``@aq.secret()`` uses — never a parallel credential path. An engine that
    has no use for it (Spark) simply ignores the key, per
    ``SessionSpec.engine_options``'s documented "opaque bag, read what you
    understand" contract.
    """
    return {
        "secrets": {
            "provider": cfg.secrets.provider,
            "region": cfg.secrets.region,
            "resolver": cfg.secrets.resolver,
            "base_dir": manifest.base_dir,
        },
    }
