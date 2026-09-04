"""DuckDB source schema reader (Phase 78 duckdb engine, ExecutorProtocol seam).

Wraps ``aqueduct.executor.duckdb_.ingress.read_ingress`` — the SAME lazy
relation builder Egress consumes — to answer ``aqueduct drift``'s live-schema
read without ever routing a DuckDB run through Spark. Mirrors
``aqueduct/executor/spark/ingress.py``'s ``read_source_schema`` precedent:
metadata-only (``rel.columns``/``rel.types`` are ``DuckDBPyRelation``
properties, zero query execution).

Only the formats ``read_ingress`` actually implements this stage (parquet,
csv, json — see that module's ``_SUPPORTED_FORMATS``) get a real answer
here. Any other format raises the SAME ``IngressError`` ``read_ingress``
already raises for an unsupported format.

Does NOT reimplement anything ``read_ingress`` already does — this module is
purely the "schema" projection over that existing reader, the same
relationship Spark's ``read_source_schema`` has to ``read_ingress``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

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


__all__ = ["read_source_schema"]
