"""Watermark crash-consistency tests (see docs/specs.md).

Covers the two-module (append Egress + `format: depot` Egress) shape's crash
window: an intent row (`__intent__:<key>`) recorded before the append starts,
cleared in the SAME transaction as the downstream watermark upsert.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from aqueduct.cli.run_setup import check_watermark_intents
from aqueduct.depot.depot import DEPOT_INTENT_PREFIX, DepotStore, depot_intent_key
from aqueduct.errors import WatermarkIntentPendingError
from aqueduct.parser.models import Module
from aqueduct.stores.base import _NamespacedDepot
from aqueduct.stores.duckdb_ import DuckDBDepotStore

pytestmark = pytest.mark.unit


class _FakeBlueprint:
    """Just enough of `Blueprint` for `check_watermark_intents` (only reads `.modules`)."""

    def __init__(self, modules):
        self.modules = modules


def _egress(module_id: str, config: dict) -> Module:
    return Module(id=module_id, type="Egress", label=module_id, config=config)


def test_depot_intent_key():
    assert depot_intent_key("last_date") == f"{DEPOT_INTENT_PREFIX}last_date"
    assert depot_intent_key("last_date") == "__intent__:last_date"


# ── 1. The crash window, reproduced ─────────────────────────────────────────


def test_crash_window_reproduced(tmp_path: Path):
    depot = DepotStore(tmp_path / "depot.db")

    watermark_key = "orders_last_date"
    intent_payload = json.dumps(
        {
            "run_id": "run-crashed-123",
            "module_id": "append_orders",
            "started_at": "2026-09-01T00:00:00+00:00",
        }
    )
    # Simulate: the append Egress wrote its intent row, then the process died
    # before the downstream depot Egress ever ran (so nothing clears it).
    depot.put(depot_intent_key(watermark_key), intent_payload)

    bp = _FakeBlueprint(
        modules=[
            _egress(
                "append_orders",
                {
                    "format": "parquet",
                    "path": "/tmp/orders",
                    "mode": "append",
                    "watermark_key": watermark_key,
                },
            ),
            _egress("write_watermark", {"format": "depot", "key": watermark_key, "value": "x"}),
        ]
    )

    with pytest.raises(WatermarkIntentPendingError) as exc_info:
        check_watermark_intents(bp, depot)

    message = str(exc_info.value)
    assert watermark_key in message
    assert "run-crashed-123" in message
    assert exc_info.value.key == watermark_key
    assert exc_info.value.run_id == "run-crashed-123"
    # Both remediations must be named.
    assert "clear-intent" in message
    assert message.count("clear-intent") >= 2


def test_no_intent_row_does_not_raise(tmp_path: Path):
    depot = DepotStore(tmp_path / "depot.db")
    bp = _FakeBlueprint(
        modules=[
            _egress(
                "append_orders",
                {
                    "format": "parquet",
                    "path": "/tmp/orders",
                    "mode": "append",
                    "watermark_key": "k",
                },
            ),
        ]
    )
    result = check_watermark_intents(bp, depot)
    assert result is None


def test_no_watermark_key_is_ignored(tmp_path: Path):
    depot = DepotStore(tmp_path / "depot.db")
    bp = _FakeBlueprint(
        modules=[
            _egress("plain_egress", {"format": "parquet", "path": "/tmp/o", "mode": "overwrite"})
        ]
    )
    result = check_watermark_intents(bp, depot)
    assert result is None


# ── 2. A clean run leaves no intent row ─────────────────────────────────────


def test_clean_run_leaves_no_intent_row(tmp_path: Path):
    depot = DepotStore(tmp_path / "depot.db")
    watermark_key = "clean_run_key"

    depot.put(depot_intent_key(watermark_key), json.dumps({"run_id": "r1"}))
    assert depot.get(depot_intent_key(watermark_key)) != ""

    depot.put_and_clear_intent(watermark_key, "2026-09-01")

    assert depot.get(depot_intent_key(watermark_key)) == ""
    assert depot.get(watermark_key) == "2026-09-01"


# ── 3. Atomicity (DuckDB) ────────────────────────────────────────────────────


def test_duckdb_kv_put_and_clear_is_one_transaction(tmp_path: Path):
    backend = DuckDBDepotStore(tmp_path / "depot.db")
    backend.kv_put(depot_intent_key("k"), "pending")

    backend.kv_put_and_clear("k", "final_value", depot_intent_key("k"))

    assert backend.kv_get("k") == "final_value"
    assert backend.kv_get(depot_intent_key("k"), "") == ""


def test_duckdb_kv_put_and_clear_read_only_raises(tmp_path: Path):
    db_path = tmp_path / "depot.db"
    # Create the file/table first via a writable store.
    DuckDBDepotStore(db_path).kv_put("seed", "1")

    from aqueduct.stores.base import StoreConnectionError

    ro = DuckDBDepotStore(db_path, read_only=True)
    with pytest.raises(StoreConnectionError):
        ro.kv_put_and_clear("k", "v", depot_intent_key("k"))


# ── 4. _NamespacedDepot prefixes both keys ──────────────────────────────────


class _RecordingBackend:
    def __init__(self):
        self.calls = []

    def kv_put_and_clear(self, put_key, value, clear_key):
        self.calls.append((put_key, value, clear_key))

    def __getattr__(self, name):
        raise AttributeError(name)


def test_namespaced_depot_prefixes_both_keys():
    inner = _RecordingBackend()
    ns = _NamespacedDepot(inner, "bp1:")

    ns.kv_put_and_clear("wm", "2026-09-01", depot_intent_key("wm"))

    assert inner.calls == [("bp1:wm", "2026-09-01", "bp1:__intent__:wm")]


def test_namespaced_depot_kv_put_and_clear_real_backend(tmp_path: Path):
    backend = DuckDBDepotStore(tmp_path / "depot.db")
    ns = _NamespacedDepot(backend, "bp1:")

    ns.kv_put_and_clear("wm", "2026-09-01", depot_intent_key("wm"))

    # Namespaced view sees the write under its own (unprefixed) name.
    assert ns.kv_get("wm") == "2026-09-01"
    # Raw backend sees it prefixed.
    assert backend.kv_get("bp1:wm") == "2026-09-01"
    assert backend.kv_get("bp1:" + depot_intent_key("wm"), "") == ""
