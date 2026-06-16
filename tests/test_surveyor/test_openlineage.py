"""Phase 55 — OpenLineage emission: facet builder, event builder, emitter POST.

Pure/unit only (no live OpenLineage server — httpx is mocked).
"""
from __future__ import annotations

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

from aqueduct.config import AqueductConfig, LineageConfig
from aqueduct.surveyor.openlineage import (
    OpenLineageEmitter,
    build_column_lineage_facet,
    build_run_event,
    extract_datasets,
    run_uuid,
)


# ── config ───────────────────────────────────────────────────────────────────

def test_lineage_config_defaults_to_disabled():
    cfg = AqueductConfig()
    assert cfg.lineage.openlineage_url is None
    assert cfg.lineage.openlineage_namespace == "aqueduct"


def test_lineage_config_rejects_unknown_key():
    with pytest.raises(Exception):
        LineageConfig(openlineage_urls="typo")  # extra="forbid"


# ── run_uuid ─────────────────────────────────────────────────────────────────

def test_run_uuid_is_deterministic():
    assert run_uuid("run_abc") == run_uuid("run_abc")
    assert run_uuid("run_abc") != run_uuid("run_def")
    # valid UUID string
    import uuid
    uuid.UUID(run_uuid("run_abc"))


# ── columnLineage facet ──────────────────────────────────────────────────────

def test_build_column_lineage_facet_maps_fields_and_dedups():
    rows = [
        {"channel_id": "c", "output_column": "amount", "source_table": "orders", "source_column": "amt"},
        {"channel_id": "c", "output_column": "amount", "source_table": "orders", "source_column": "amt"},
        {"channel_id": "c", "output_column": "region", "source_table": "orders", "source_column": "region"},
    ]
    facet = build_column_lineage_facet(rows, "ns")
    assert set(facet["fields"]) == {"amount", "region"}
    # duplicate row collapses to a single inputField
    assert facet["fields"]["amount"]["inputFields"] == [
        {"namespace": "ns", "name": "orders", "field": "amt"}
    ]
    assert "columnLineage" not in facet  # facet IS the columnLineage body
    assert facet["_schemaURL"].endswith("ColumnLineageDatasetFacet")


def test_build_column_lineage_facet_unknown_fallback():
    rows = [{"channel_id": "c", "output_column": "x", "source_table": "", "source_column": ""}]
    facet = build_column_lineage_facet(rows, "ns")
    assert facet["fields"]["x"]["inputFields"][0]["name"] == "UNKNOWN"
    assert facet["fields"]["x"]["inputFields"][0]["field"] == "UNKNOWN"


def test_build_column_lineage_facet_empty_returns_none():
    assert build_column_lineage_facet([], "ns") is None


# ── datasets ─────────────────────────────────────────────────────────────────

def _manifest():
    return SimpleNamespace(
        blueprint_id="demo.bp",
        modules=(
            SimpleNamespace(id="read", type="Ingress", config={"path": "s3://in/orders"}),
            SimpleNamespace(id="t", type="Channel", config={"op": "sql", "query": "SELECT a FROM read"}),
            SimpleNamespace(id="write", type="Egress", config={"path": "s3://out/daily"}),
        ),
        edges=(SimpleNamespace(from_id="read", to_id="t", port="main"),),
    )


def test_extract_datasets_splits_ingress_and_egress():
    inputs, outputs = extract_datasets(_manifest(), "ns")
    assert inputs == [{"namespace": "ns", "name": "s3://in/orders"}]
    assert outputs == [{"namespace": "ns", "name": "s3://out/daily"}]


# ── run event ────────────────────────────────────────────────────────────────

def test_build_run_event_complete_shape():
    ev = build_run_event(
        "COMPLETE", run_id="run_1", job_namespace="ns", job_name="demo.bp",
        inputs=[], outputs=[{"namespace": "ns", "name": "out"}],
    )
    assert ev["eventType"] == "COMPLETE"
    assert ev["run"]["runId"] == run_uuid("run_1")
    assert ev["job"] == {"namespace": "ns", "name": "demo.bp"}
    assert ev["run"]["facets"] == {}
    assert ev["schemaURL"].endswith("RunEvent")


def test_build_run_event_fail_carries_error_facet():
    ev = build_run_event(
        "FAIL", run_id="run_1", job_namespace="ns", job_name="bp",
        inputs=[], outputs=[], error_message="boom",
    )
    assert ev["run"]["facets"]["errorMessage"]["message"] == "boom"


def test_build_run_event_rejects_invalid_type():
    with pytest.raises(ValueError, match="eventType"):
        build_run_event("NOPE", run_id="r", job_namespace="n", job_name="j",
                        inputs=[], outputs=[])


# ── emitter ──────────────────────────────────────────────────────────────────

def test_emitter_normalises_url():
    m = _manifest()
    assert OpenLineageEmitter("http://h:5000", "ns", m)._url == "http://h:5000/api/v1/lineage"
    assert OpenLineageEmitter("http://h:5000/api/v1/lineage", "ns", m)._url == "http://h:5000/api/v1/lineage"


def test_emitter_posts_event_with_column_lineage_facet():
    m = _manifest()
    emitter = OpenLineageEmitter("http://ol.test", "ns", m)
    with patch("httpx.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200)
        thread = emitter.emit("COMPLETE", run_id="run_1")
        assert isinstance(thread, threading.Thread)
        thread.join(timeout=2)
        mock_post.assert_called_once()
        sent = mock_post.call_args.kwargs["json"]
        assert sent["eventType"] == "COMPLETE"
        assert sent["job"]["name"] == "demo.bp"
        # the SQL Channel produced a columnLineage facet on the output dataset
        assert "columnLineage" in sent["outputs"][0]["facets"]


def test_emitter_server_error_logged_not_raised(capsys):
    emitter = OpenLineageEmitter("http://ol.test", "ns", _manifest())
    with patch("httpx.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=500)
        emitter.emit("START", run_id="run_1").join(timeout=2)
    assert "500" in capsys.readouterr().err
