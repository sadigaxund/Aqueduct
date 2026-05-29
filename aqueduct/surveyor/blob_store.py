"""Blob externalisation for fat observability columns (Phase 39).

DuckDB stores large JSON text inline, bloating row width.  This module
moves ``manifest_json``, ``provenance_json``, and ``stack_trace`` out of
the database into compressed ``.json.zst`` files under
``.aqueduct/observability/<blueprint_id>/blobs/<run_id>/``.

The database columns store only the relative blob path.  Existing inline
data continues to work — ``materialize`` detects paths from inline text
and loads transparently.

Postgres is unaffected — TOAST handles large JSON natively.  The blob
store targets DuckDB only.
"""

from __future__ import annotations

import zstandard as zstd
from pathlib import Path

_BLOB_MARKER = "blobs/"


def externalise(value: str, store_dir: Path, run_id: str, name: str) -> str:
    """Write *value* as a compressed blob and return the relative path.

    Args:
        value:     The raw text to externalise (JSON string, stack trace).
        store_dir: Root observability directory (parent of ``blobs/``).
        run_id:    Run UUID — used as the blob subdirectory.
        name:      Human-readable filename stem (e.g. ``manifest``, ``prov``).

    Returns:
        Relative path like ``blobs/<run_id>/manifest.json.zst``.
    """
    if not value:
        return value  # keep empty strings inline
    blob_dir = store_dir / "blobs" / run_id
    blob_dir.mkdir(parents=True, exist_ok=True)
    blob_path = blob_dir / f"{name}.json.zst"
    compressed = zstd.compress(value.encode("utf-8"), level=3)
    blob_path.write_bytes(compressed)
    # Return a relative path anchored to store_dir
    return f"{_BLOB_MARKER}{run_id}/{name}.json.zst"


def materialize(value_or_path: str, store_dir: Path) -> str:
    """Return the raw text, loading from blob if *value_or_path* is a path.

    Args:
        value_or_path: Raw inline text or a relative blob path.
        store_dir:     Root observability directory.

    Returns:
        The original text content (decompressed from blob if necessary).
    """
    if not value_or_path or not value_or_path.startswith(_BLOB_MARKER):
        return value_or_path  # inline data, or empty
    blob_path = store_dir / value_or_path
    try:
        compressed = blob_path.read_bytes()
        return zstd.decompress(compressed).decode("utf-8")
    except (FileNotFoundError, zstd.ZstdError):
        return value_or_path  # fall back to inline (blob missing or corrupt)
