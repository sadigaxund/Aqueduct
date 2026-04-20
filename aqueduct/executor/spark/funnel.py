"""Funnel executor — fan-in: merges multiple DataFrames into one.

All modes produce a single lazy DataFrame.  No Spark action is triggered
inside this module.  The result is evaluated when a downstream Egress
module triggers .save().

Supported modes
───────────────
union_all    Stacks all input DataFrames with unionByName.
             Set schema_check: permissive to allow missing columns
             (filled with null); default strict requires identical schemas.
union        Same as union_all, then .distinct() to remove duplicates.
coalesce     Row-aligned merge: adds a synthetic row-id to each DataFrame,
             joins on it, then coalesces overlapping columns (first non-null
             wins left-to-right).  Requires row counts to be equal.
zip          Row-aligned join: adds a synthetic row-id to each DataFrame,
             joins on it.  Column names must be unique across all inputs.

Config shape (YAML / dict)
──────────────────────────
mode: union_all
inputs:
  - ingress_a
  - ingress_b
schema_check: permissive    # optional; default "strict" (union_all / union only)

mode: union
inputs:
  - ingress_a
  - ingress_b

mode: coalesce
inputs:
  - ingress_primary
  - ingress_fallback

mode: zip
inputs:
  - ingress_left
  - ingress_right
"""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from aqueduct.parser.models import Module


class FunnelError(Exception):
    """Raised when a Funnel module cannot be executed."""


# ── helpers ───────────────────────────────────────────────────────────────────

_ROW_ID_COL = "_aq_row_id"


def _resolve_inputs(
    module_id: str,
    cfg: dict,
    upstream_dfs: dict[str, DataFrame],
) -> list[DataFrame]:
    """Validate 'inputs' list and return ordered DataFrames."""
    input_ids: list[str] = cfg.get("inputs", [])
    if not input_ids:
        raise FunnelError(
            f"[{module_id}] 'inputs' must list at least two upstream module IDs"
        )
    if len(input_ids) < 2:
        raise FunnelError(
            f"[{module_id}] 'inputs' must contain at least 2 entries; got {len(input_ids)}"
        )

    dfs: list[DataFrame] = []
    for iid in input_ids:
        df = upstream_dfs.get(iid)
        if df is None:
            raise FunnelError(
                f"[{module_id}] upstream module {iid!r} not found in frame_store. "
                f"Available: {list(upstream_dfs)}"
            )
        dfs.append(df)
    return dfs


# ── Mode implementations ──────────────────────────────────────────────────────

def _union_all(
    module_id: str,
    dfs: list[DataFrame],
    schema_check: str,
) -> DataFrame:
    allow_missing = schema_check == "permissive"
    try:
        return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=allow_missing), dfs)
    except Exception as exc:
        raise FunnelError(
            f"[{module_id}] union_all failed: {exc}"
        ) from exc


def _union(
    module_id: str,
    dfs: list[DataFrame],
    schema_check: str,
) -> DataFrame:
    return _union_all(module_id, dfs, schema_check).distinct()


def _coalesce(
    module_id: str,
    dfs: list[DataFrame],
) -> DataFrame:
    """Row-aligned merge: join on synthetic row-id, coalesce overlapping columns."""
    from pyspark.sql import functions as F

    # Add row-id to each DataFrame
    tagged = [df.withColumn(_ROW_ID_COL, F.monotonically_increasing_id()) for df in dfs]

    # Determine all unique column names (excluding row-id) in order of first appearance
    seen: dict[str, int] = {}  # col_name → index of first df that has it
    for i, df in enumerate(tagged):
        for col in df.columns:
            if col != _ROW_ID_COL and col not in seen:
                seen[col] = i

    # Join all DataFrames on row-id; suffix collisions with _df<n>
    base = tagged[0]
    for i, other in enumerate(tagged[1:], start=1):
        # Rename overlapping columns in 'other' before join
        rename_map: dict[str, str] = {}
        for col in other.columns:
            if col != _ROW_ID_COL and col in [c for c in base.columns if c != _ROW_ID_COL]:
                rename_map[col] = f"{col}_df{i}"

        for old, new in rename_map.items():
            other = other.withColumnRenamed(old, new)

        base = base.join(other, on=_ROW_ID_COL, how="left")

    # Coalesce overlapping columns (first non-null left-to-right)
    select_exprs = []
    # Track which original columns have been coalesced
    coalesced: set[str] = set()

    for col_name in seen:
        if col_name in coalesced:
            continue
        # Collect all aliases for this column across the joined df
        candidates = [col_name] + [
            f"{col_name}_df{i}"
            for i in range(1, len(dfs))
            if f"{col_name}_df{i}" in base.columns
        ]
        if len(candidates) == 1:
            select_exprs.append(F.col(candidates[0]).alias(col_name))
        else:
            select_exprs.append(
                F.coalesce(*[F.col(c) for c in candidates]).alias(col_name)
            )
        coalesced.add(col_name)

    try:
        return base.select(select_exprs)
    except Exception as exc:
        raise FunnelError(f"[{module_id}] coalesce failed: {exc}") from exc


def _zip(
    module_id: str,
    dfs: list[DataFrame],
) -> DataFrame:
    """Row-aligned join: join on synthetic row-id, keep all columns."""
    from pyspark.sql import functions as F

    # Validate no column name conflicts across inputs (excluding row-id)
    all_cols: list[str] = []
    for df in dfs:
        for col in df.columns:
            if col != _ROW_ID_COL:
                if col in all_cols:
                    raise FunnelError(
                        f"[{module_id}] zip mode requires unique column names across "
                        f"all inputs; duplicate column: {col!r}"
                    )
                all_cols.append(col)

    tagged = [df.withColumn(_ROW_ID_COL, F.monotonically_increasing_id()) for df in dfs]

    base = tagged[0]
    for other in tagged[1:]:
        base = base.join(other, on=_ROW_ID_COL, how="left")

    # Drop the synthetic row-id column
    try:
        return base.drop(_ROW_ID_COL)
    except Exception as exc:
        raise FunnelError(f"[{module_id}] zip failed: {exc}") from exc


# ── Public API ────────────────────────────────────────────────────────────────

def execute_funnel(
    module: Module,
    upstream_dfs: dict[str, DataFrame],
) -> DataFrame:
    """Merge upstream DataFrames into one according to module.config.

    Args:
        module:       A Funnel Module from the compiled Manifest.
        upstream_dfs: Dict mapping module_id → lazy DataFrame (all upstreams).

    Returns:
        Single lazy merged DataFrame.  No Spark action triggered.

    Raises:
        FunnelError: Invalid config, unsupported mode, missing inputs, or
                     column conflict (zip mode).
    """
    cfg = module.config
    mode = cfg.get("mode")
    schema_check: str = cfg.get("schema_check", "strict")

    dfs = _resolve_inputs(module.id, cfg, upstream_dfs)

    if mode == "union_all":
        return _union_all(module.id, dfs, schema_check)
    elif mode == "union":
        return _union(module.id, dfs, schema_check)
    elif mode == "coalesce":
        return _coalesce(module.id, dfs)
    elif mode == "zip":
        return _zip(module.id, dfs)
    else:
        raise FunnelError(
            f"[{module.id}] unsupported mode {mode!r}. "
            f"Supported: union_all, union, coalesce, zip"
        )
