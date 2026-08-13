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
    "resolve_effective_engine_configs",
    "resolve_session_engine_config",
    "session_config_fingerprint",
    "session_secrets_options",
]


def resolve_session_engine_config(
    cfg: AqueductConfig,
    engine: str,
    manifest: Any,
) -> dict[str, Any]:
    """Build one engine's ``SessionSpec.engine_config`` dict.

    Every registered engine gets the SAME precedence: the engine's
    ``aqueduct.yml``-level config, overridden by that engine's entry in the
    Blueprint's ``Manifest.engine_config`` (Blueprint wins on a key
    conflict) — this used to be Spark's own special-cased merge
    (``engine.spark.conf`` + ``manifest.spark_config``) with every other
    engine silently getting ONLY its ``aqueduct.yml`` config and no way for
    a Blueprint to override it. That was the bug (Phase 82 remediation):
    Spark keeps its ``conf``-nested free-form bag (arbitrary ``spark.*``
    keys), every OTHER registered engine (``duckdb``, ...) gets its own
    ``engine.<name>`` sub-model dumped to a flat dict via ``model_dump()``
    — whatever fields THAT engine declares
    (``memory_limit``/``threads``/``database_path``/``s3_*``/... for
    DuckDB) flow through to ``_make_session`` automatically — but BOTH
    branches now layer ``manifest.engine_config.get(engine, {})`` on top,
    Blueprint winning, for every engine alike.

    Every caller that builds a real execution session for an engine — the
    ``aqueduct run`` path (single-engine and polyglot) AND the patch preview
    sandbox gate — MUST resolve ``engine_config`` through this function.
    Building ``SessionSpec.engine_config`` any other way (e.g. hardcoding
    ``manifest.spark_config`` regardless of the target engine, the bug this
    function was originally extracted to fix — Phase 82 remediation)
    silently discards every ``engine.<name>.*`` field a non-Spark engine
    declares, and — before this generalization — silently discarded any
    Blueprint-level override for a non-Spark engine too.
    """
    blueprint_override = manifest.engine_config.get(engine, {})
    if engine == "spark":
        return {**cfg.engine.spark.conf, **blueprint_override}
    engine_cfg = getattr(cfg.engine, engine, None)
    if engine_cfg is None:
        return {}
    return {**engine_cfg.model_dump(), **blueprint_override}


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
