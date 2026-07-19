"""Phase 30a tier 1 — extended Spark compiler warnings.

Pure static analysis on a compiled Manifest. Zero runtime cost — runs during
`compile()` before the Manifest is returned. Modular: one rule per file in
this package, each exposing a single `check(manifest) -> list[str]` callable.
Add a new rule by dropping a file in here and importing it in `RULES`.

Compiler integration: `compile()` iterates `RULES`, calls each, emits returned
strings via the same `warnings.warn(...)` channel as the existing 8a–8g
diagnostics, so no Surveyor / no observability.db / no log-handler plumbing changes.

To add a rule:
  1. Create `aqueduct/compiler/warnings/<rule_name>.py`.
  2. Export `def check(manifest) -> list[str]:` returning one message per
     finding. Empty list = clean. Never raise.
  3. Append the module to `RULES` below.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import (
    count_col_likely_count_star,
    custom_probe_driver_code,
    file_format_no_repartition,
    jdbc_missing_partition,
    kafka_checkpoint_stale,
    nondeterministic_fanout,
    spillway_port_mismatch,
)

CheckFn = Callable[[Any, str], list[str]]

# Stable per-rule registry. The `id` field is the user-facing key for
# `warnings.suppress` in aqueduct.yml — never rename without a deprecation note.
RULES: list[tuple[str, CheckFn]] = [
    (kafka_checkpoint_stale.RULE_ID,         kafka_checkpoint_stale.check),
    (nondeterministic_fanout.RULE_ID,        nondeterministic_fanout.check),
    (count_col_likely_count_star.RULE_ID,    count_col_likely_count_star.check),
    (file_format_no_repartition.RULE_ID,     file_format_no_repartition.check),
    (jdbc_missing_partition.RULE_ID,         jdbc_missing_partition.check),
    (custom_probe_driver_code.RULE_ID,       custom_probe_driver_code.check),
    (spillway_port_mismatch.RULE_ID,         spillway_port_mismatch.check),
]


def run_all(
    manifest: Any, suppress: set[str] | None = None, engine: str = "spark"
) -> list[tuple[str, str]]:
    """Run every rule, return `[(rule_id, message), ...]`.

    Suppressed rules are skipped entirely (not even invoked). Per-rule
    exceptions are swallowed — diagnostics never block compilation.

    ``engine`` is threaded uniformly into every rule's `check(manifest,
    engine)` call (Phase 78 duckdb-engine work) so a rule whose advice is
    physical to one engine (Spark task partitions, JDBC read parallelism,
    Kafka micro-batches, driver-side custom-probe cost) can gate its own
    emission instead of firing nonsensical advice on another engine.
    Engine-agnostic rules simply ignore the argument.
    """
    suppress = suppress or set()
    out: list[tuple[str, str]] = []
    for rule_id, rule in RULES:
        if rule_id in suppress:
            continue
        try:
            for msg in rule(manifest, engine) or []:
                out.append((rule_id, msg))
        except Exception:
            continue
    return out
