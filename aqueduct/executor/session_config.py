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

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aqueduct.config import AqueductConfig

__all__ = ["resolve_session_engine_config", "session_secrets_options"]


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
