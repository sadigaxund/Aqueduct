"""Unit tests for the Phase 53 object store (stores/object_store.py).

Pure-Python, no Spark, no network — the canonical example of a ``unit`` layer
test. Covers the transport backend, the BlobStore/PatchStore semantics, the
factories, the config model, and the back-compat ``surveyor/blob_store.py`` shim.
"""

from __future__ import annotations

import pytest

from aqueduct.stores.object_store import (
    BLOB_MARKER,
    BlobStore,
    FsspecBackend,
    LocalBackend,
    ObjectStore,
    PatchStore,
    make_blob_store,
    make_patch_store,
)

pytestmark = pytest.mark.unit


# ── LocalBackend transport ────────────────────────────────────────────────────


def test_local_backend_roundtrip_and_lifecycle(tmp_path):
    b = LocalBackend(tmp_path)
    assert b.exists("a/b.json") is False
    b.put("a/b.json", b'{"x": 1}')
    assert b.exists("a/b.json") is True
    assert b.get("a/b.json") == b'{"x": 1}'
    assert b.mtime("a/b.json") > 0
    b.move("a/b.json", "c/d.json")
    assert b.exists("a/b.json") is False
    assert b.exists("c/d.json") is True
    b.delete("c/d.json")
    assert b.exists("c/d.json") is False
    # delete of a missing key is a no-op, never raises
    b.delete("nope.json")


def test_local_backend_list_only_files(tmp_path):
    b = LocalBackend(tmp_path)
    b.put("p/one.json", b"1")
    b.put("p/two.json", b"2")
    (tmp_path / "p" / "subdir").mkdir(parents=True, exist_ok=True)
    keys = sorted(b.list("p"))
    assert keys == ["p/one.json", "p/two.json"]
    assert b.list("missing") == []


def test_local_backend_mtime_missing_is_zero(tmp_path):
    assert LocalBackend(tmp_path).mtime("ghost") == 0.0


# ── ObjectStore wrapper ───────────────────────────────────────────────────────


def test_object_store_prefix_scoping_and_json(tmp_path):
    store = ObjectStore(LocalBackend(tmp_path), prefix="things")
    store.put_json("a.json", {"k": "v"})
    assert (tmp_path / "things" / "a.json").exists()
    assert store.get_json("a.json") == {"k": "v"}
    assert store.exists("a.json")
    assert store.list_keys("") == ["a.json"]  # relative, prefix stripped


def test_iter_payloads_newest_first_and_skips_malformed(tmp_path):
    store = ObjectStore(LocalBackend(tmp_path), prefix="x")
    store.put_json("first.json", {"n": 1})
    store.put_json("second.json", {"n": 2})
    store.put_text("broken.json", "{not json")
    store.put_text("notdict.json", "[1, 2, 3]")
    out = store.iter_payloads("")
    names = [k for k, _, _ in out]
    # malformed + non-dict dropped; only the two dicts survive
    assert set(names) == {"first.json", "second.json"}
    # newest first by mtime
    assert out[0][2]["n"] in (1, 2)


# ── BlobStore ─────────────────────────────────────────────────────────────────


def test_blobstore_externalise_materialize_roundtrip(tmp_path):
    blob = BlobStore(LocalBackend(tmp_path))
    marker = blob.externalise("hello world", "run1", "manifest")
    assert marker == f"{BLOB_MARKER}run1/manifest.json.zst"
    assert (tmp_path / marker).exists()
    assert blob.materialize(marker) == "hello world"


def test_blobstore_empty_stays_inline_and_passthrough(tmp_path):
    blob = BlobStore(LocalBackend(tmp_path))
    assert blob.externalise("", "run1", "manifest") == ""  # empty kept inline
    assert blob.materialize("") == ""
    assert blob.materialize("plain inline text") == "plain inline text"


def test_blobstore_missing_blob_falls_back_to_marker(tmp_path):
    blob = BlobStore(LocalBackend(tmp_path))
    # a marker whose file was never written → return the marker, never raise
    assert blob.materialize(f"{BLOB_MARKER}ghost/x.json.zst") == f"{BLOB_MARKER}ghost/x.json.zst"


def test_blob_marker_is_backcompat_with_legacy_shim(tmp_path):
    """A blob written by the old surveyor.blob_store.externalise reads back
    through the new BlobStore at the same marker (Phase 53 must not orphan
    pre-existing DuckDB rows)."""
    from aqueduct.surveyor.blob_store import externalise as legacy_ext

    marker = legacy_ext("payload-text", tmp_path, "runZ", "stack")
    assert marker == f"{BLOB_MARKER}runZ/stack.json.zst"
    assert BlobStore(LocalBackend(tmp_path)).materialize(marker) == "payload-text"


def test_legacy_shim_roundtrip(tmp_path):
    from aqueduct.surveyor.blob_store import externalise, materialize

    marker = externalise("via-shim", tmp_path, "r", "prov")
    assert materialize(marker, tmp_path) == "via-shim"


# ── PatchStore ────────────────────────────────────────────────────────────────


def test_patchstore_lifecycle_dirs(tmp_path):
    ps = PatchStore(LocalBackend(tmp_path))
    key = ps.write_pending("20260101T000000_p1.json", {"patch_id": "p1"})
    assert key == "pending/20260101T000000_p1.json"
    assert (tmp_path / "patches" / "pending" / "20260101T000000_p1.json").exists()
    ps.write_applied("a.json", {"patch_id": "a"})
    ps.write_rejected("r.json", {"patch_id": "r"})
    assert (tmp_path / "patches" / "applied" / "a.json").exists()
    assert (tmp_path / "patches" / "rejected" / "r.json").exists()


def test_patchstore_find_pending_by_id_exact_and_timestamped(tmp_path):
    ps = PatchStore(LocalBackend(tmp_path))
    ps.write_pending("exact.json", {"patch_id": "exact"})
    ps.write_pending("20260101T000000_ts.json", {"patch_id": "ts"})
    assert ps.find_pending_by_id("exact") == "pending/exact.json"
    assert ps.find_pending_by_id("ts") == "pending/20260101T000000_ts.json"
    assert ps.find_pending_by_id("missing") is None


def test_patchstore_iter_pending_returns_payloads(tmp_path):
    ps = PatchStore(LocalBackend(tmp_path))
    ps.write_pending("p.json", {"patch_id": "p", "_aq_meta": {"x": 1}})
    rows = ps.iter_pending()
    assert [r[2]["patch_id"] for r in rows] == ["p"]


# ── Factories ─────────────────────────────────────────────────────────────────


def test_make_blob_store_local_roots_at_given_dir(tmp_path):
    blob = make_blob_store("local", "", tmp_path)
    marker = blob.externalise("v", "run", "manifest")
    assert (tmp_path / marker).exists()


def test_make_patch_store_local_reproduces_patches_dir(tmp_path):
    patches_dir = tmp_path / "patches"
    ps = make_patch_store("local", "", patches_dir)
    ps.write_pending("x.json", {"patch_id": "x"})
    # <patches_dir.parent>/patches/pending/x.json == <patches_dir>/pending/x.json
    assert (patches_dir / "pending" / "x.json").exists()


def test_make_blob_store_s3_builds_fsspec_backend():
    """An s3 location yields a FsspecBackend (fsspec is installed in CI's
    [object-store] env). With fsspec absent the constructor raises a clear
    ImportError naming the extra."""
    import importlib.util

    if importlib.util.find_spec("fsspec") is None:
        with pytest.raises(ImportError, match="object-store"):
            FsspecBackend("s3://bucket/x")
    else:
        blob = make_blob_store("s3", "s3://bucket/aqueduct", "/unused")
        assert isinstance(blob, BlobStore)


# ── Config ────────────────────────────────────────────────────────────────────


def test_object_store_config_defaults_and_extra_forbid():
    from pydantic import ValidationError

    from aqueduct.config import ObjectStoreConfig, StoresConfig

    cfg = StoresConfig()
    assert cfg.blob.backend == "local"
    assert cfg.blob.path == ""
    with pytest.raises(ValidationError):
        ObjectStoreConfig(backend="local", bogus="x")  # extra="forbid"


def test_blob_config_rejects_unknown_backend():
    from pydantic import ValidationError

    from aqueduct.config import ObjectStoreConfig

    with pytest.raises(ValidationError):
        ObjectStoreConfig(backend="ftp", path="")

# ── FsspecBackend (exercised over fsspec's in-memory filesystem) ──────────────
# fsspec's ``memory://`` driver runs the exact same FsspecBackend code path as
# s3/gcs/adls without needing moto or network — process-local, deterministic.
# Each test uses a unique base since the memory filesystem is process-global.

import importlib.util as _import_util
import uuid as _uuid

# fsspec ships with the [object-store] extra — skip these where it is absent
# (a base install / the minimal CI matrix leg). They run in the object-store env.
_needs_fsspec = pytest.mark.skipif(
    _import_util.find_spec("fsspec") is None,
    reason="fsspec not installed (pip install 'aqueduct-core[object-store]')",
)


def _mem_base(tag: str) -> str:
    return f"memory://{tag}-{_uuid.uuid4().hex}"


@_needs_fsspec
def test_fsspec_backend_roundtrip_and_lifecycle():
    b = FsspecBackend(_mem_base("obj"))
    b.put("d/x.json", b'{"k": 1}')
    assert b.get("d/x.json") == b'{"k": 1}'
    assert b.exists("d/x.json") and not b.exists("d/missing.json")
    assert b.list("d") == ["d/x.json"]
    assert b.mtime("d/x.json") >= 0.0

    b.move("d/x.json", "d/y.json")
    assert not b.exists("d/x.json")
    assert b.get("d/y.json") == b'{"k": 1}'

    b.delete("d/y.json")
    assert not b.exists("d/y.json")
    assert b.list("d") == []


@_needs_fsspec
def test_fsspec_backend_list_empty_prefix_is_empty():
    assert FsspecBackend(_mem_base("empty")).list("nope") == []


@_needs_fsspec
def test_patchstore_over_fsspec_pending_to_applied_move_preserves_body():
    ps = PatchStore(FsspecBackend(_mem_base("ps")))
    body = {"patch_id": "p1", "rationale": "r", "operations": [{"op": "x"}]}
    pending_key = ps.write_pending("p1.json", body)
    assert pending_key == "pending/p1.json"

    ps.move(pending_key, "applied/p1.json")
    assert ps.get_json("applied/p1.json") == body
    assert not ps.exists(pending_key)


@_needs_fsspec
def test_make_patch_store_fsspec_backend_type():
    ps = make_patch_store("s3", "s3://bucket/prefix", local_patches_dir="ignored")
    assert isinstance(ps._backend, FsspecBackend)
