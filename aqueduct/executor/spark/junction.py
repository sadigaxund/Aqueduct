"""Junction executor — fan-out: splits one DataFrame into multiple lazy branches.

All modes produce lazy DataFrame references.  No Spark action is triggered
inside this module.  Branches are evaluated when downstream Egress modules
trigger .save().

Supported modes
───────────────
conditional  Each branch receives df.filter(condition).  The special condition
             "_else_" receives rows that matched no other branch.
broadcast    All branches receive the same unmodified DataFrame (zero shuffle).
partition    Each branch receives rows where partition_key = branch.value.

Config shape (YAML / dict)
──────────────────────────
mode: conditional
branches:
  - id: branch_eu
    condition: "region IN ('DE','FR','NL')"
  - id: branch_other
    condition: "_else_"

mode: broadcast
branches:
  - id: branch_a
  - id: branch_b

mode: partition
partition_key: region
branches:
  - id: branch_eu
    value: "EU"          # value to match; falls back to id if omitted
  - id: branch_us
    value: "US"
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from aqueduct.parser.models import Module


class JunctionError(Exception):
    """Raised when a Junction module cannot be executed."""


# ── Mode implementations ──────────────────────────────────────────────────────

def _conditional(
    module_id: str,
    df: DataFrame,
    branches: list[dict],
) -> dict[str, DataFrame]:
    if not branches:
        raise JunctionError(f"[{module_id}] 'branches' must not be empty")

    result: dict[str, DataFrame] = {}
    explicit_conditions: list[str] = []

    # First pass — collect explicit conditions and validate branch ids
    for branch in branches:
        bid = branch.get("id")
        if not bid:
            raise JunctionError(f"[{module_id}] every branch must have an 'id'")
        cond = branch.get("condition")
        if not cond:
            raise JunctionError(
                f"[{module_id}] branch {bid!r} is missing 'condition' for conditional mode"
            )
        if cond != "_else_":
            explicit_conditions.append(cond)

    # Second pass — build filtered DataFrames
    for branch in branches:
        bid = branch["id"]
        cond = branch["condition"]

        if cond == "_else_":
            if explicit_conditions:
                # NOT (c1 OR c2 OR ...)
                combined = " OR ".join(f"({c})" for c in explicit_conditions)
                result[bid] = df.filter(f"NOT ({combined})")
            else:
                result[bid] = df
        else:
            result[bid] = df.filter(cond)

    return result


def _broadcast(
    module_id: str,
    df: DataFrame,
    branches: list[dict],
) -> dict[str, DataFrame]:
    if not branches:
        raise JunctionError(f"[{module_id}] 'branches' must not be empty")

    result: dict[str, DataFrame] = {}
    for branch in branches:
        bid = branch.get("id")
        if not bid:
            raise JunctionError(f"[{module_id}] every branch must have an 'id'")
        result[bid] = df  # same lazy plan, no copy

    return result


def _partition(
    module_id: str,
    df: DataFrame,
    cfg: dict,
    branches: list[dict],
) -> dict[str, DataFrame]:
    partition_key = cfg.get("partition_key")
    if not partition_key:
        raise JunctionError(
            f"[{module_id}] 'partition_key' is required for partition mode"
        )
    if not branches:
        raise JunctionError(f"[{module_id}] 'branches' must not be empty")

    result: dict[str, DataFrame] = {}
    for branch in branches:
        bid = branch.get("id")
        if not bid:
            raise JunctionError(f"[{module_id}] every branch must have an 'id'")
        value = branch.get("value", bid)  # fall back to id as value
        result[bid] = df.filter(f"{partition_key} = '{value}'")

    return result


# ── Public API ────────────────────────────────────────────────────────────────

def execute_junction(module: Module, df: DataFrame) -> dict[str, DataFrame]:
    """Split df into branch DataFrames according to module.config.

    Args:
        module: A Junction Module from the compiled Manifest.
        df:     Single incoming DataFrame on the main port.

    Returns:
        Dict mapping branch_id → lazy filtered DataFrame.
        All values are lazy — no Spark action is triggered.

    Raises:
        JunctionError: Invalid config, unsupported mode, or missing branch id.
    """
    cfg = module.config
    mode = cfg.get("mode")
    branches: list[dict] = cfg.get("branches", [])

    if mode == "conditional":
        return _conditional(module.id, df, branches)
    elif mode == "broadcast":
        return _broadcast(module.id, df, branches)
    elif mode == "partition":
        return _partition(module.id, df, cfg, branches)
    else:
        raise JunctionError(
            f"[{module.id}] unsupported mode {mode!r}. "
            f"Supported: conditional, broadcast, partition"
        )
