"""DuckDB execution engine (Phase 78 Stage A).

``duckdb`` may be imported ONLY inside this package — mirroring the
``pyspark``-only-inside-``aqueduct/executor/spark/`` rule. The observability/
depot/benchmark STORES also depend on ``duckdb`` (``aqueduct/stores/duckdb_``)
— that is the stores' own dependency (a DuckDB file used as an embedded
metadata database) and is completely unrelated to this package, which is an
*execution engine* for Blueprint data flow (Ingress/Channel/Junction/Funnel/
Egress) using DuckDB's relational API.
"""

from __future__ import annotations
