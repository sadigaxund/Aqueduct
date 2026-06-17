"""Object storage — the driver's blob + patch persistence seam (Phase 53).

Aqueduct's driver writes two families of opaque files that are NOT relational
rows: compressed observability blobs (fat ``manifest_json`` / ``stack_trace`` /
``provenance_json`` columns) and the patch lifecycle (``pending`` / ``applied``
/ ``rejected`` JSON). Historically both went straight to the driver's local
filesystem, which breaks on an ephemeral k8s pod whose cwd vanishes when the
pod dies.

This module introduces one transport (`ObjectStore`) over a pluggable backend
and two semantic stores layered on top:

* `BlobStore`   — zstd-compressed observability blobs (``blobs/`` prefix).
* `PatchStore`  — the human-review patch lifecycle (``patches/`` prefix).

The default `local` backend is byte-identical to the pre-Phase-53 layout, so a
repo/git review workflow is unchanged. The `s3` / `gcs` / `adls` backends are
served by a single ``fsspec`` handle (the ``[object-store]`` extra) so cluster
pods can persist where the artefacts survive.

Design mirrors ``stores/base.py``: a transport ABC (`_Backend`) with concrete
implementations, and prefix-scoped semantic wrappers on top — the same shape as
``_RelationalStore`` → ``ObservabilityStore`` / ``LineageStore``.
"""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

__all__ = [
    "ObjectStore",
    "BlobStore",
    "PatchStore",
    "LocalBackend",
    "FsspecBackend",
    "get_object_backend",
    "BLOB_MARKER",
]

# Kept identical to the pre-Phase-53 marker so existing DuckDB rows that store
# ``blobs/<run_id>/<name>.json.zst`` continue to resolve through BlobStore.
BLOB_MARKER = "blobs/"


# ── Transport backends ──────────────────────────────────────────────────────


class _Backend(ABC):
    """Opaque byte transport. Keys are POSIX-style relative paths (``a/b/c``)."""

    @abstractmethod
    def put(self, key: str, data: bytes) -> None: ...

    @abstractmethod
    def get(self, key: str) -> bytes: ...

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def delete(self, key: str) -> None: ...

    @abstractmethod
    def list(self, prefix: str) -> list[str]:
        """Return keys under *prefix* (non-recursive of dirs is not required;
        callers filter). Missing prefix yields ``[]``."""

    @abstractmethod
    def mtime(self, key: str) -> float:
        """Modification time (epoch seconds) for ordering; ``0.0`` if unknown."""

    @abstractmethod
    def move(self, src: str, dst: str) -> None: ...

    @property
    @abstractmethod
    def location_label(self) -> str: ...


class LocalBackend(_Backend):
    """Local-filesystem backend — the default. No extra dependency.

    ``root`` is the directory all keys are resolved under. For blobs this is the
    observability store dir (so ``blobs/<run_id>/…`` lands exactly where the
    pre-Phase-53 code put it); for patches it is the ``patches/`` parent.
    """

    def __init__(self, root: Path | str) -> None:
        self._root = Path(root)

    def _p(self, key: str) -> Path:
        return self._root / key

    def put(self, key: str, data: bytes) -> None:
        p = self._p(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, p)  # atomic on POSIX

    def get(self, key: str) -> bytes:
        return self._p(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._p(key).exists()

    def delete(self, key: str) -> None:
        p = self._p(key)
        if p.exists():
            p.unlink()

    def list(self, prefix: str) -> list[str]:
        base = self._p(prefix)
        if not base.exists():
            return []
        out: list[str] = []
        for entry in os.scandir(base):
            if entry.is_file():
                out.append(f"{prefix.rstrip('/')}/{entry.name}")
        return out

    def mtime(self, key: str) -> float:
        try:
            return self._p(key).stat().st_mtime
        except OSError:
            return 0.0

    def move(self, src: str, dst: str) -> None:
        s, d = self._p(src), self._p(dst)
        d.parent.mkdir(parents=True, exist_ok=True)
        os.replace(s, d)

    @property
    def location_label(self) -> str:
        return str(self._root)


class FsspecBackend(_Backend):
    """Object-store backend via ``fsspec`` (s3 / gcs / adls / …).

    Lazily imports ``fsspec`` so the base install never needs it. The
    ``[object-store]`` extra pulls ``s3fs`` / ``gcsfs`` / ``adlfs`` as needed —
    one client, every cloud, no feature-named extra.

    ``base_uri`` is the root URI (e.g. ``s3://bucket/aqueduct``); keys are
    appended with ``/``.
    """

    def __init__(self, base_uri: str) -> None:
        try:
            import fsspec  # noqa: F401
        except ImportError as exc:  # pragma: no cover - exercised via extra
            raise ImportError(
                "Object-store backend requires fsspec. Install the extra: "
                "pip install 'aqueduct-core[object-store]'"
            ) from exc
        self._base = base_uri.rstrip("/")
        import fsspec

        # ``url_to_fs`` returns (filesystem, stripped_path); we keep the fs and
        # rebuild full URIs per key so the same handle serves every key.
        self._fs, _ = fsspec.core.url_to_fs(self._base)

    def _uri(self, key: str) -> str:
        return f"{self._base}/{key.lstrip('/')}"

    def put(self, key: str, data: bytes) -> None:
        uri = self._uri(key)
        parent = uri.rsplit("/", 1)[0]
        try:
            self._fs.makedirs(parent, exist_ok=True)
        except (NotImplementedError, OSError):
            pass  # object stores have no real dirs
        with self._fs.open(uri, "wb") as fh:
            fh.write(data)

    def get(self, key: str) -> bytes:
        with self._fs.open(self._uri(key), "rb") as fh:
            return fh.read()

    def exists(self, key: str) -> bool:
        return bool(self._fs.exists(self._uri(key)))

    def delete(self, key: str) -> None:
        uri = self._uri(key)
        if self._fs.exists(uri):
            self._fs.rm_file(uri) if hasattr(self._fs, "rm_file") else self._fs.rm(uri)

    def list(self, prefix: str) -> list[str]:
        base = self._uri(prefix)
        if not self._fs.exists(base):
            return []
        out: list[str] = []
        for item in self._fs.ls(base, detail=True):
            if item.get("type") == "file":
                name = item["name"].rsplit("/", 1)[-1]
                out.append(f"{prefix.rstrip('/')}/{name}")
        return out

    def mtime(self, key: str) -> float:
        try:
            info = self._fs.info(self._uri(key))
        except (FileNotFoundError, OSError):
            return 0.0
        ts = info.get("LastModified") or info.get("mtime") or info.get("last_modified")
        if ts is None:
            return 0.0
        if hasattr(ts, "timestamp"):
            return float(ts.timestamp())
        try:
            return float(ts)
        except (TypeError, ValueError):
            return 0.0

    def move(self, src: str, dst: str) -> None:
        s, d = self._uri(src), self._uri(dst)
        parent = d.rsplit("/", 1)[0]
        try:
            self._fs.makedirs(parent, exist_ok=True)
        except (NotImplementedError, OSError):
            pass
        self._fs.mv(s, d)

    @property
    def location_label(self) -> str:
        return self._base


def get_object_backend(backend: str, location: str) -> _Backend:
    """Construct a transport backend from config (``backend``, ``path``/uri)."""
    if backend == "local":
        return LocalBackend(location)
    if backend in ("s3", "gcs", "adls"):
        return FsspecBackend(location)
    raise ValueError(  # pragma: no cover — guarded at config layer
        f"unsupported object-store backend {backend!r} (expected local|s3|gcs|adls)"
    )


def make_blob_store(backend: str, location: str, local_root: Path | str) -> BlobStore:
    """Build a `BlobStore` from ``stores.blob`` config.

    For ``local`` the backend root defaults to *local_root* (the observability
    store dir) when ``location`` is unset, so ``blobs/`` lands exactly where the
    pre-Phase-53 code put it. For object backends both blobs and patches share
    the same fsspec base, separated by prefix.
    """
    if backend == "local":
        return BlobStore(LocalBackend(location or local_root))
    return BlobStore(get_object_backend(backend, location))


def make_patch_store(backend: str, location: str, local_patches_dir: Path | str) -> PatchStore:
    """Build a `PatchStore` from ``stores.blob`` config.

    `PatchStore` scopes everything under a ``patches/`` prefix. For ``local``
    the backend root is the *parent* of *local_patches_dir*, so
    ``<root>/patches/...`` reproduces the historical directory exactly.
    """
    if backend == "local":
        root = Path(location) if location else Path(local_patches_dir).parent
        return PatchStore(LocalBackend(root))
    return PatchStore(get_object_backend(backend, location))


# ── Transport wrapper ─────────────────────────────────────────────────────────


class ObjectStore:
    """Prefix-scoped byte/text/JSON operations over a `_Backend`.

    Subclasses add per-use-case semantics (compression, lifecycle moves). This
    parent owns *where and how bytes land*; subclasses own *what the bytes mean*.
    """

    def __init__(self, backend: _Backend, prefix: str = "") -> None:
        self._backend = backend
        self._prefix = prefix.strip("/")

    def _key(self, rel: str) -> str:
        rel = rel.lstrip("/")
        return f"{self._prefix}/{rel}" if self._prefix else rel

    # bytes
    def put_bytes(self, rel: str, data: bytes) -> None:
        self._backend.put(self._key(rel), data)

    def get_bytes(self, rel: str) -> bytes:
        return self._backend.get(self._key(rel))

    # text
    def put_text(self, rel: str, text: str) -> None:
        self._backend.put(self._key(rel), text.encode("utf-8"))

    def get_text(self, rel: str) -> str:
        return self._backend.get(self._key(rel)).decode("utf-8")

    # json
    def put_json(self, rel: str, obj: Any, *, indent: int | None = 2) -> None:
        self.put_text(rel, json.dumps(obj, indent=indent))

    def get_json(self, rel: str) -> Any:
        return json.loads(self.get_text(rel))

    def exists(self, rel: str) -> bool:
        return self._backend.exists(self._key(rel))

    def delete(self, rel: str) -> None:
        self._backend.delete(self._key(rel))

    def move(self, src_rel: str, dst_rel: str) -> None:
        self._backend.move(self._key(src_rel), self._key(dst_rel))

    def list_keys(self, rel_prefix: str) -> list[str]:
        """Relative keys (without the store prefix) under *rel_prefix*."""
        full = self._backend.list(self._key(rel_prefix))
        plen = len(self._prefix) + 1 if self._prefix else 0
        return [k[plen:] for k in full]

    def iter_payloads(self, rel_prefix: str) -> list[tuple[str, float, dict]]:
        """``(rel_key, mtime, json_dict)`` for every readable ``*.json``,
        newest first. Mirrors the heal-cache scan contract — malformed files
        are skipped at DEBUG, non-dict payloads dropped."""
        keys = [k for k in self.list_keys(rel_prefix) if k.endswith(".json")]
        scored = [(k, self._backend.mtime(self._key(k))) for k in keys]
        out: list[tuple[str, float, dict]] = []
        for k, mt in sorted(scored, key=lambda t: t[1], reverse=True):
            try:
                payload = json.loads(self.get_text(k))
            except Exception:
                logger.debug("Skipping unreadable object %s", k, exc_info=True)
                continue
            if isinstance(payload, dict):
                out.append((k, mt, payload))
        return out

    @property
    def location_label(self) -> str:
        base = self._backend.location_label
        return f"{base}/{self._prefix}" if self._prefix else base


# ── Blob store ────────────────────────────────────────────────────────────────


class BlobStore(ObjectStore):
    """Zstd externalisation of fat observability columns (``blobs/`` prefix).

    Absorbs the former ``surveyor/blob_store.py`` logic. The DB row stores a
    relative marker (``blobs/<run_id>/<name>.json.zst``); ``materialize`` loads
    and decompresses on read. Empty strings stay inline.
    """

    def __init__(self, backend: _Backend) -> None:
        super().__init__(backend, prefix="")  # marker already carries "blobs/"

    def externalise(self, value: str, run_id: str, name: str) -> str:
        """Compress *value* to ``blobs/<run_id>/<name>.json.zst``; return marker.

        Returns *value* unchanged when empty (kept inline)."""
        if not value:
            return value
        import zstandard as zstd

        rel = f"{BLOB_MARKER}{run_id}/{name}.json.zst"
        self.put_bytes(rel, zstd.compress(value.encode("utf-8"), level=3))
        return rel

    def materialize(self, value_or_marker: str) -> str:
        """Return raw text, decompressing when *value_or_marker* is a blob marker."""
        if not value_or_marker or not value_or_marker.startswith(BLOB_MARKER):
            return value_or_marker
        import zstandard as zstd

        try:
            return zstd.decompress(self.get_bytes(value_or_marker)).decode("utf-8")
        except Exception:
            return value_or_marker  # blob missing/corrupt → fall back to marker


# ── Patch store ───────────────────────────────────────────────────────────────


class PatchStore(ObjectStore):
    """Human-review patch lifecycle (``patches/{pending,applied,rejected}/``).

    Replaces the scattered local-FS writes in ``agent/loop.py`` and
    ``patch/apply.py``. The local backend default is byte-identical to the
    pre-Phase-53 ``patches/`` directory, so the git review workflow is
    unchanged; an object-store backend lets ephemeral cluster pods persist
    patches where they survive.

    JSON is written indented for git-diff friendliness. Lifecycle subdirs are
    plain ``pending`` / ``applied`` / ``rejected`` rel-prefixes.
    """

    PENDING = "pending"
    APPLIED = "applied"
    REJECTED = "rejected"

    def __init__(self, backend: _Backend) -> None:
        super().__init__(backend, prefix="patches")

    def write_pending(self, filename: str, payload: dict) -> str:
        rel = f"{self.PENDING}/{filename}"
        self.put_json(rel, payload, indent=2)
        return rel

    def write_applied(self, filename: str, payload: dict) -> str:
        rel = f"{self.APPLIED}/{filename}"
        self.put_json(rel, payload, indent=2)
        return rel

    def write_rejected(self, filename: str, payload: dict) -> str:
        rel = f"{self.REJECTED}/{filename}"
        self.put_json(rel, payload, indent=2)
        return rel

    def iter_pending(self) -> list[tuple[str, float, dict]]:
        return self.iter_payloads(self.PENDING)

    def iter_applied(self) -> list[tuple[str, float, dict]]:
        return self.iter_payloads(self.APPLIED)

    def find_pending_by_id(self, patch_id: str) -> str | None:
        """Rel-key of a pending patch matching *patch_id*.

        Tries exact ``pending/{patch_id}.json`` then the timestamped
        ``pending/*_{patch_id}.json`` naming; returns the newest match."""
        exact = f"{self.PENDING}/{patch_id}.json"
        if self.exists(exact):
            return exact
        matches = [k for k in self.list_keys(self.PENDING) if k.endswith(f"_{patch_id}.json")]
        if not matches:
            return None
        scored = sorted(matches, key=lambda k: self._backend.mtime(self._key(k)))
        return scored[-1]
