"""Shared probe-sampling config bundle — engine-agnostic, pyspark-free.

Both engines' ``execute_probe(sampling=...)`` (``executor/spark/probe.py``
and ``executor/duckdb_/probe.py``) accept a ``ProbeSampling`` instance. It
used to live only in the Spark module, which meant ``aqueduct/cli/run_setup.py``
had to import a Spark module just to build the sampling config for EVERY
engine, DuckDB included. Hoisted here per the ``path_keys.py``/``channel_ops.py``
precedent — a single top-level engine-agnostic module the CLI (and either
engine) can import without pulling in the other engine's package.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProbeSampling:
    max_sample_rows: int = 100
    default_sample_fraction: float = 0.1
    # Phase 85 A1 — per-probe write-time cap for the sample_rows signal type,
    # the one signal that persists actual data ROW content (redacted, but
    # still row content) rather than aggregate statistics. Fixed at this
    # default — not configurable via aqueduct.yml.
    sample_rows_keep_last_n: int = 20
