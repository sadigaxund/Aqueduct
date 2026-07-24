"""Junction executor — fan-out: splits one relation into multiple lazy branches.

All three modes are pure filters over a single-node relation (see the Spark
Junction docstring, ``aqueduct/executor/spark/junction.py``, for the shared
mode semantics) — cheap to implement identically on DuckDB, so all three
ship in Stage A: no engine-specific behavior difference from Spark's version
beyond the underlying relation type.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

from aqueduct.errors import AqueductError
from aqueduct.models import Module

VALID_MODES: frozenset[str] = frozenset({"conditional", "broadcast", "partition"})


class JunctionError(AqueductError):
    """Raised when a Junction module cannot be executed."""


def _conditional(
    module_id: str, rel: duckdb.DuckDBPyRelation, branches: list[dict]
) -> dict[str, duckdb.DuckDBPyRelation]:
    if not branches:
        raise JunctionError(f"[{module_id}] 'branches' must not be empty")

    result: dict[str, duckdb.DuckDBPyRelation] = {}
    explicit_conditions: list[str] = []

    for branch in branches:
        bid = branch.get("id")
        if not bid:
            raise JunctionError(f"[{module_id}] every branch must have an 'id'")
        cond = branch.get("condition")
        if not cond:
            raise JunctionError(f"[{module_id}] branch {bid!r} is missing 'condition' for conditional mode")
        if cond != "_else_":
            explicit_conditions.append(cond)

    for branch in branches:
        bid = branch["id"]
        cond = branch["condition"]
        if cond == "_else_":
            if explicit_conditions:
                combined = " OR ".join(f"({c})" for c in explicit_conditions)
                result[bid] = rel.filter(f"NOT ({combined})")
            else:
                result[bid] = rel
        else:
            result[bid] = rel.filter(cond)

    return result


def _broadcast(
    module_id: str, rel: duckdb.DuckDBPyRelation, branches: list[dict]
) -> dict[str, duckdb.DuckDBPyRelation]:
    if not branches:
        raise JunctionError(f"[{module_id}] 'branches' must not be empty")
    result: dict[str, duckdb.DuckDBPyRelation] = {}
    for branch in branches:
        bid = branch.get("id")
        if not bid:
            raise JunctionError(f"[{module_id}] every branch must have an 'id'")
        result[bid] = rel  # same lazy relation, no copy — DuckDB is single-process, nothing to broadcast
    return result


def _partition(
    module_id: str, rel: duckdb.DuckDBPyRelation, cfg: dict, branches: list[dict]
) -> dict[str, duckdb.DuckDBPyRelation]:
    partition_key = cfg.get("partition_key")
    if not partition_key:
        raise JunctionError(f"[{module_id}] 'partition_key' is required for partition mode")
    if not branches:
        raise JunctionError(f"[{module_id}] 'branches' must not be empty")

    result: dict[str, duckdb.DuckDBPyRelation] = {}
    for branch in branches:
        bid = branch.get("id")
        if not bid:
            raise JunctionError(f"[{module_id}] every branch must have an 'id'")
        value = branch.get("value", bid)
        result[bid] = rel.filter(f"{partition_key} = '{value}'")
    return result


def execute_junction(module: Module, rel: duckdb.DuckDBPyRelation) -> dict[str, duckdb.DuckDBPyRelation]:
    """Split rel into branch relations according to module.config.

    Raises:
        JunctionError: Invalid config, unsupported mode, or missing branch id.
    """
    cfg = module.config
    mode = cfg.get("mode")
    branches: list[dict] = cfg.get("branches", [])

    if mode == "conditional":
        return _conditional(module.id, rel, branches)
    if mode == "broadcast":
        return _broadcast(module.id, rel, branches)
    if mode == "partition":
        return _partition(module.id, rel, cfg, branches)
    raise JunctionError(f"[{module.id}] unsupported mode {mode!r}. Supported: conditional, broadcast, partition")


__all__ = ["JunctionError", "execute_junction", "VALID_MODES"]
