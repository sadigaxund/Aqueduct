"""DuckDB source schema/sample reader — the DuckDB half of the healing agent's
diagnostic tools (Phase 78 duckdb engine, ExecutorProtocol seam).

Wraps ``aqueduct.executor.duckdb_.ingress.read_ingress`` — the SAME lazy
relation builder Egress consumes — to answer ``get_source_schema`` and
``sample_rows`` (``aqueduct/agent/toolbox.py``) without ever routing a
DuckDB heal through Spark. Mirrors ``aqueduct/executor/spark/ingress.py``'s
``read_source_schema`` precedent: schema reads are metadata-only
(``rel.columns``/``rel.types`` are ``DuckDBPyRelation`` properties, zero
query execution), and row sampling is bounded by a pushed-down ``LIMIT``,
never a full scan.

Only the formats ``read_ingress`` actually implements this stage (parquet,
csv, json — see that module's ``_SUPPORTED_FORMATS``) get a real answer
here. Any other format raises the SAME ``IngressError`` ``read_ingress``
already raises for an unsupported format; the ToolBox's ``call()`` catches
any tool-handler exception and returns it as a redacted tool-result rather
than crashing the heal attempt (see ``ToolBox.call``'s ``except Exception``),
so an unsupported-format module degrades gracefully with a clear reason
rather than silently returning nothing.

Does NOT reimplement anything ``read_ingress`` already does — this module is
purely the "schema/sample" projection over that existing reader, the same
relationship Spark's ``read_source_schema`` has to ``read_ingress``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb

from aqueduct.executor.duckdb_.ingress import read_ingress
from aqueduct.models import Module


def read_source_schema(module: Module, con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Return the live source schema as ``{column: duckdb_type}``.

    Metadata-only: builds the lazy reader (``read_ingress``) and reads
    ``rel.columns``/``rel.types`` — zero query execution, same "read the
    schema without scanning data" contract as the Spark reader.
    """
    rel = read_ingress(module, con)
    return {name: str(dtype) for name, dtype in zip(rel.columns, rel.types, strict=True)}


def sample_source_rows(
    module: Module,
    con: duckdb.DuckDBPyConnection,
    n: int = 10,
    base_dir: str | None = None,
) -> list[dict[str, Any]]:
    """Return up to ``n`` real rows from an Ingress module's source.

    Bounded via a pushed-down ``LIMIT`` on the lazy relation before
    materializing (``.fetchall()``) — never a full scan, mirroring the
    Spark reader's ``limit(n).collect()`` precedent (and the same
    ``block_full_actions``-exempt reasoning: a LIMIT push-down is not the
    fraction-of-whole sampling that gate exists to police).
    """
    rel = read_ingress(module, con, base_dir=base_dir)
    limited = rel.limit(int(n))
    columns = limited.columns
    return [dict(zip(columns, row, strict=True)) for row in limited.fetchall()]


__all__ = ["read_source_schema", "sample_source_rows"]
