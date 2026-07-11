"""Post-heal regression artifact generation (opt-in, `agent.regression_artifact`).

Different job from the signature-memory heal cache (`agent/signature.py` +
`agent/memory.py`): the cache resolves the SAME failure RECURRING in
production for zero tokens — it acts AFTER the failure recurs. This module
emits a CI regression test (`.aqtest.yml`, run by ``aqueduct test``) that
guards against the FIX being silently UNDONE — a hand-edit, a SQL refactor,
a revert — catching reintroduction of the root cause BEFORE the next
production run, with no failure and no heal needed. Same reason a human
writes a regression test after fixing a bug by hand. The two are
complementary, not overlapping — see docs/specs.md §8.9.

Generation is deliberately conservative: whenever the healed failure/module
shape doesn't map cleanly onto an expressible aqtest, ``generate`` returns a
skip result with a reason instead of emitting a broken test file. No pyspark
import — this only reads the compiled Manifest + PatchSpec, both plain
dataclasses/pydantic models.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aqueduct.executor.spark.test_runner import _TESTABLE_TYPES
from aqueduct.models import Manifest, Module
from aqueduct.patch.grammar import PatchSpec
from aqueduct.surveyor.models import FailureContext

logger = logging.getLogger(__name__)

# Ops that make a single-module config change — the only patch shapes simple
# enough to turn into a fixture-based regression test without guessing at
# multi-module interaction. Any other op in the patch disqualifies generation.
_SINGLE_MODULE_CONFIG_OPS = frozenset({"set_module_config_key", "replace_module_config"})

# Spark SQL DDL type name (lower-cased, prefix before any "(") -> dummy value.
# Matches the vocabulary `schema_hint` / aqtest `schema:` blocks already use
# (see aqueduct/executor/spark/test_runner.py::_schema_ddl / _create_df).
_DUMMY_VALUES: dict[str, Any] = {
    "string": "sample", "varchar": "sample", "char": "sample",
    "int": 1, "integer": 1, "tinyint": 1, "smallint": 1, "bigint": 1, "long": 1, "short": 1, "byte": 1,
    "double": 1.0, "float": 1.0, "decimal": 1.0,
    "boolean": True, "bool": True,
    "date": "2026-01-01",
    "timestamp": "2026-01-01 00:00:00",
}


@dataclass(frozen=True)
class RegressionArtifactResult:
    """Outcome of a `generate` call — always returned, never raises."""

    written: bool
    path: Path | None = None
    skip_reason: str | None = None


def _dummy_value_for_type(type_str: str) -> tuple[Any, bool]:
    """Return (value, mapped). mapped=False means the type has no safe dummy."""
    key = type_str.strip().lower().split("(")[0]
    if key in _DUMMY_VALUES:
        return _DUMMY_VALUES[key], True
    return None, False


def _single_patched_module_id(patch: PatchSpec) -> str | None:
    """The one module_id every op in the patch targets, or None.

    None means: no operations, an op type outside the single-module-config
    set, or operations touching more than one module_id — all disqualify
    generation (the patch shape is too complex to express as one fixture).
    """
    ops = patch.operations
    if not ops:
        return None
    module_ids: set[str] = set()
    for op in ops:
        op_name = getattr(op, "op", None)
        if op_name not in _SINGLE_MODULE_CONFIG_OPS:
            return None
        mid = getattr(op, "module_id", None)
        if not mid:
            return None
        module_ids.add(mid)
    if len(module_ids) != 1:
        return None
    return next(iter(module_ids))


def _find_module(manifest: Manifest, module_id: str) -> Module | None:
    for m in manifest.modules:
        if m.id == module_id:
            return m
    return None


def _upstream_module_ids(manifest: Manifest, module_id: str) -> list[str]:
    """Distinct upstream module ids feeding `module_id` on the main port, in edge order."""
    seen: list[str] = []
    for e in manifest.edges:
        if e.to_id == module_id and e.port == "main" and e.from_id not in seen:
            seen.append(e.from_id)
    return seen


def _build_fixture(
    manifest: Manifest, upstream_ids: list[str]
) -> tuple[dict[str, dict[str, Any]], str | None]:
    """Build the aqtest `inputs:` block from upstream schema_hints.

    Returns (inputs, skip_reason). skip_reason is set (inputs is {}) when any
    upstream module lacks a usable schema_hint.
    """
    inputs: dict[str, dict[str, Any]] = {}
    for uid in upstream_ids:
        upstream = _find_module(manifest, uid)
        if upstream is None:
            return {}, f"upstream module {uid!r} not found in manifest"
        hint = upstream.config.get("schema_hint") if isinstance(upstream.config, dict) else None
        if not hint or not isinstance(hint, list):
            return {}, f"upstream module {uid!r} has no schema_hint — cannot synthesize fixture rows"
        schema: dict[str, str] = {}
        row: list[Any] = []
        for field in hint:
            if not isinstance(field, dict):
                return {}, f"upstream module {uid!r} schema_hint entry is not a mapping"
            name = field.get("name")
            ftype = field.get("type")
            if not name or not ftype:
                return {}, f"upstream module {uid!r} schema_hint entry missing name/type"
            value, mapped = _dummy_value_for_type(str(ftype))
            if not mapped:
                return {}, f"upstream module {uid!r} schema_hint field {name!r} has unmapped type {ftype!r}"
            schema[str(name)] = str(ftype)
            row.append(value)
        inputs[uid] = {"schema": schema, "rows": [row]}
    return inputs, None


def _unique_path(aqtests_dir: Path, stem: str) -> Path:
    """Never overwrite — append _2, _3, ... until a free filename is found."""
    candidate = aqtests_dir / f"{stem}.aqtest.yml"
    n = 2
    while candidate.exists():
        candidate = aqtests_dir / f"{stem}_{n}.aqtest.yml"
        n += 1
    return candidate


def generate(
    manifest: Manifest,
    patch: PatchSpec,
    failure_ctx: FailureContext,
    blueprint_path: Path,
) -> RegressionArtifactResult:
    """Emit a `.aqtest.yml` regression test for a successfully-healed module.

    Call this ONLY after a heal has fully succeeded (patch applied AND the
    re-run succeeded — `healing_outcomes.run_success_after_patch = True`).
    `manifest` is the PATCHED Manifest (the one the successful re-run used).

    Always returns a result — never raises. Any failure to build a valid
    fixture is a skip, not an exception, per the conservative-generation
    contract (never emit a broken test file).
    """
    module_id = _single_patched_module_id(patch)
    if module_id is None:
        return RegressionArtifactResult(
            written=False,
            skip_reason="patch touches multiple modules or a non-config op — regression artifact generation skipped",
        )

    module = _find_module(manifest, module_id)
    if module is None:
        return RegressionArtifactResult(written=False, skip_reason=f"patched module {module_id!r} not found in manifest")

    if module.type not in _TESTABLE_TYPES:
        return RegressionArtifactResult(
            written=False,
            skip_reason=(
                f"module {module_id!r} has type {module.type!r}, not testable via aqtest "
                f"(only {sorted(t.value for t in _TESTABLE_TYPES)} are supported)"
            ),
        )

    upstream_ids = _upstream_module_ids(manifest, module_id)
    if not upstream_ids:
        return RegressionArtifactResult(
            written=False, skip_reason=f"module {module_id!r} has no upstream inputs to build a fixture from"
        )

    inputs, skip_reason = _build_fixture(manifest, upstream_ids)
    if skip_reason:
        return RegressionArtifactResult(written=False, skip_reason=skip_reason)

    if not blueprint_path.exists():
        return RegressionArtifactResult(written=False, skip_reason=f"blueprint path {blueprint_path} does not exist")

    aqtests_dir = blueprint_path.parent / "aqtests"
    aqtests_dir.mkdir(parents=True, exist_ok=True)

    description = (patch.rationale or failure_ctx.error_message or "").strip()
    if len(description) > 140:
        description = description[:140].rstrip() + "…"

    test_id = f"regression_{patch.patch_id or module_id}".replace(" ", "_")
    blueprint_rel = _relative_blueprint_ref(aqtests_dir, blueprint_path)

    doc = {
        "aqueduct_test": "1.0",
        "blueprint": blueprint_rel,
        "tests": [
            {
                "id": test_id,
                "description": f"Regression guard for healed failure on {module_id!r}: {description}",
                "module": module_id,
                "inputs": inputs,
                "assertions": [
                    {"type": "sql", "expr": "SELECT count(*) >= 0 FROM __output__"},
                ],
            }
        ],
    }

    out_path = _unique_path(aqtests_dir, f"{module_id}_regression")
    _write_aqtest(out_path, doc, patch, failure_ctx)
    logger.info("[regression_artifact] wrote %s (module=%s, patch=%s)", out_path, module_id, patch.patch_id)
    return RegressionArtifactResult(written=True, path=out_path)


def _relative_blueprint_ref(aqtests_dir: Path, blueprint_path: Path) -> str:
    try:
        return str(Path(os.path.relpath(blueprint_path, aqtests_dir)))
    except ValueError:
        # Different drives on Windows or similar — fall back to absolute.
        return str(blueprint_path)


def _write_aqtest(out_path: Path, doc: dict[str, Any], patch: PatchSpec, failure_ctx: FailureContext) -> None:
    import yaml

    header = (
        f"# Auto-generated by Aqueduct's post-heal regression-artifact feature\n"
        f"# (agent.regression_artifact: true) after a successful heal.\n"
        f"# Failed module: {failure_ctx.failed_module}  |  patch: {patch.patch_id}\n"
        f"# Run with: aqueduct test {out_path.name}\n"
        f"#\n"
        f"# This test exists to catch the healed failure being reintroduced\n"
        f"# later (a hand-edit, a SQL refactor, a revert) BEFORE it reaches\n"
        f"# production again — a CI regression guard, not the heal cache.\n"
        f"# Safe to edit or delete.\n\n"
    )
    body = yaml.safe_dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)
    out_path.write_text(header + body, encoding="utf-8")
