"""Backward-compatible shim — blob logic moved to ``stores/object_store.py``.

Phase 53 unified blob externalisation under the pluggable `ObjectStore`
transport (so s3/gcs/adls backends work, not just the local filesystem). The
two original ``store_dir``-based helpers are retained here so existing call
sites and tests keep working unchanged; both delegate to
`aqueduct.stores.object_store.BlobStore` over a `LocalBackend`.

New code should construct a `BlobStore` directly (see
``aqueduct.stores.object_store.make_blob_store``) rather than import these.
"""

from __future__ import annotations

from pathlib import Path

from aqueduct.stores.object_store import BlobStore, LocalBackend


def externalise(value: str, store_dir: Path, run_id: str, name: str) -> str:
    """Write *value* as a compressed blob under *store_dir*; return the marker.

    Thin wrapper over ``BlobStore(LocalBackend(store_dir)).externalise``."""
    return BlobStore(LocalBackend(store_dir)).externalise(value, run_id, name)


def materialize(value_or_path: str, store_dir: Path) -> str:
    """Return raw text, decompressing when *value_or_path* is a blob marker.

    Thin wrapper over ``BlobStore(LocalBackend(store_dir)).materialize``."""
    return BlobStore(LocalBackend(store_dir)).materialize(value_or_path)
