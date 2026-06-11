"""JDBC Ingress missing partitioning hints → single-partition read.

Without `partitionColumn` + `lowerBound` + `upperBound` (or `predicates`),
Spark's JDBC source pulls every row through a single executor connection.
At >10M rows this becomes the bottleneck for the whole pipeline. All four
options are documented in Spark; the warning surfaces them at compile time
rather than after the first painful production run.
"""

from __future__ import annotations

from typing import Any

RULE_ID = "jdbc_missing_partition"


def check(manifest: Any) -> list[str]:
    out: list[str] = []
    for m in manifest.modules:
        if m.type != "Ingress":
            continue
        cfg = m.config or {}
        if (cfg.get("format") or "").lower() != "jdbc":
            continue
        opts = cfg.get("options") or {}
        has_range = all(k in opts for k in ("partitionColumn", "lowerBound", "upperBound"))
        has_predicates = "predicates" in opts
        if has_range or has_predicates:
            continue
        out.append(
            f"Ingress '{m.id}' uses format=jdbc without partitionColumn / "
            "lowerBound / upperBound (or `predicates`). The entire table will "
            "be read through a single executor connection — this is the most "
            "common bottleneck for JDBC ingestion. Add the four "
            "partitioning options to enable parallel reads. "
            "See docs/spark_guide.md#jdbc-ingress-parallelism."
        )
    return out
