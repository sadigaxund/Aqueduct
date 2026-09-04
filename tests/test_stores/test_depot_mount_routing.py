"""Per-blueprint default depot: where a mount's file lands, and when its keys
are prefixed.

Three mount shapes, three behaviours:

* implicit (no ``path``)  → its own file at
  ``<routing root>/<blueprint_id>/depot.db``, keys stored RAW.
* explicit ``path``       → that one file, keys prefixed ``<blueprint_id>:``.
* ``shared: true``        → that one file, keys RAW — and an explicit ``path``
  is mandatory.
"""

from __future__ import annotations

import duckdb
import pytest
from pydantic import ValidationError

from aqueduct.config import DEFAULT_DEPOT_DB_FILENAME, AqueductConfig, DepotMountConfig
from aqueduct.stores.base import build_depot_mounts
from aqueduct.stores.duckdb_ import DuckDBDepotStore

pytestmark = pytest.mark.unit


def _keys(db_path) -> list[str]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return sorted(r[0] for r in conn.execute("SELECT key FROM depot_kv").fetchall())
    finally:
        conn.close()


def test_implicit_mount_routes_to_its_own_per_blueprint_file(tmp_path):
    """No `path` → `<store dir>/<blueprint_id>/depot.db`, NOT observability.db."""
    mounts = build_depot_mounts(AqueductConfig(), blueprint_id="sales", store_dir_override=tmp_path)
    mounts["default"].kv_put("watermark", "2020-01-01")

    routed = tmp_path / "sales" / DEFAULT_DEPOT_DB_FILENAME
    assert routed.exists()
    assert not (tmp_path / "sales" / "observability.db").exists()
    assert _keys(routed) == ["watermark"]


def test_implicit_mount_skips_key_prefixing(tmp_path):
    """The FILE is per blueprint, so `_NamespacedDepot` must not wrap it."""
    mounts = build_depot_mounts(AqueductConfig(), blueprint_id="sales", store_dir_override=tmp_path)
    assert isinstance(mounts["default"], DuckDBDepotStore)  # not _NamespacedDepot

    mounts["default"].kv_put("watermark", "v1")
    assert _keys(tmp_path / "sales" / DEFAULT_DEPOT_DB_FILENAME) == ["watermark"]


def test_two_blueprints_get_two_files_from_the_same_implicit_mount(tmp_path):
    for bp in ("sales", "orders"):
        mounts = build_depot_mounts(AqueductConfig(), blueprint_id=bp, store_dir_override=tmp_path)
        mounts["default"].kv_put("watermark", bp)

    assert _keys(tmp_path / "sales" / DEFAULT_DEPOT_DB_FILENAME) == ["watermark"]
    assert _keys(tmp_path / "orders" / DEFAULT_DEPOT_DB_FILENAME) == ["watermark"]
    sales = build_depot_mounts(AqueductConfig(), blueprint_id="sales", store_dir_override=tmp_path)
    assert sales["default"].kv_get("watermark") == "sales"


def test_explicit_path_mount_keeps_the_one_file_and_the_key_prefix(tmp_path):
    """An explicit `path` is unchanged: one shared file, prefixed keys."""
    shared = tmp_path / "shared_depot.db"
    cfg = AqueductConfig(
        **{"stores": {"depots": {"default": {"backend": "duckdb", "path": str(shared)}}}}
    )
    build_depot_mounts(cfg, blueprint_id="sales")["default"].kv_put("watermark", "a")
    build_depot_mounts(cfg, blueprint_id="orders")["default"].kv_put("watermark", "b")

    assert _keys(shared) == ["orders:watermark", "sales:watermark"]
    assert not (tmp_path / "sales").exists()


def test_shared_mount_stores_raw_keys_in_its_explicit_file(tmp_path):
    shared = tmp_path / "fleet.db"
    cfg = AqueductConfig(
        **{
            "stores": {
                "depots": {
                    "default": {"backend": "duckdb", "path": str(tmp_path / "d.db")},
                    "fleet": {"backend": "duckdb", "path": str(shared), "shared": True},
                }
            }
        }
    )
    build_depot_mounts(cfg, blueprint_id="sales")["fleet"].kv_put("cursor", "a")
    assert _keys(shared) == ["cursor"]


def test_shared_without_path_is_a_config_error():
    with pytest.raises(ValidationError) as exc:
        DepotMountConfig(shared=True)
    assert "requires an explicit `path`" in str(exc.value)

    with pytest.raises(ValidationError):
        AqueductConfig(**{"stores": {"depots": {"fleet": {"shared": True}}}})


def test_non_duckdb_backend_without_path_is_a_config_error():
    """`path` is a DSN/URL there — nothing this routing can derive."""
    for backend in ("postgres", "redis"):
        with pytest.raises(ValidationError) as exc:
            DepotMountConfig(backend=backend)
        assert "requires an explicit `path`" in str(exc.value)


def test_preview_depots_reads_the_routed_location(tmp_path, monkeypatch):
    """The read-only preview path resolves the same per-blueprint file a run
    writes — otherwise `aqueduct compile` would see an empty depot."""
    from aqueduct.depot.depot import preview_depots

    monkeypatch.chdir(tmp_path)
    build_depot_mounts(AqueductConfig(), blueprint_id="sales")["default"].kv_put(
        "watermark", "2020-01-01"
    )
    assert (tmp_path / ".aqueduct" / "observability" / "sales" / DEFAULT_DEPOT_DB_FILENAME).exists()

    default, mounts = preview_depots(AqueductConfig(), "sales")
    assert default.get("watermark") == "2020-01-01"
    assert mounts["default"].get("watermark") == "2020-01-01"
