"""Egress `format: parquet|json|csv` without `repartition` or `partition_by`.

Spark writes one output file per partition. A pipeline with high task
parallelism (default 200 shuffle partitions) writes 200 tiny files per run,
degrading downstream reads. Note 8d already warns for parquet+append; this
rule covers all modes for parquet/json/csv plus the overwrite case 8d
misses. Skip Delta — Delta uses transaction log + OPTIMIZE for file sizing.
"""

from __future__ import annotations

from typing import Any

RULE_ID = "file_format_no_repartition"

_PROBLEM_FORMATS = {"parquet", "json", "csv"}


def check(manifest: Any) -> list[str]:
    out: list[str] = []
    for m in manifest.modules:
        if m.type != "Egress":
            continue
        cfg = m.config or {}
        fmt = (cfg.get("format") or "").lower()
        if fmt not in _PROBLEM_FORMATS:
            continue
        has_partition = bool(cfg.get("partition_by") or cfg.get("repartition") or cfg.get("coalesce"))
        if has_partition:
            continue
        out.append(
            f"Egress '{m.id}' writes format={fmt!r} without repartition / "
            "coalesce / partition_by. Spark will write one file per task "
            "partition (default 200), producing many tiny files. Add "
            "`coalesce: 1` or `repartition: N` to control output file count "
            "or `partition_by: [col, ...]` for partitioned layout."
        )
    return out
