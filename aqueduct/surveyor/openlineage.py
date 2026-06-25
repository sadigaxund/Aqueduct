"""Phase 55 — OpenLineage emission.

Emits OpenLineage RunEvents — START on run begin, COMPLETE / FAIL on end — to an
OpenLineage-compatible backend (Marquez, DataHub, Atlan) over ``httpx`` in a
daemon thread. Async, non-blocking, best-effort: delivery never blocks the run
and never raises (mirrors ``surveyor/webhook.py``). The blueprint result is
authoritative; a dropped lineage event is logged to stderr and swallowed.

The output datasets carry the column-level **``columnLineage``** facet, built
from the same compile-time lineage rows that populate the ``column_lineage``
table (``compiler.lineage.compute_lineage_rows``) — field-to-field arrows render
in Marquez/DataHub/Atlan. sqlglot resolves ~90% of SparkSQL; unresolved columns
fall back to ``UNKNOWN``.

Config lives in the top-level ``lineage:`` block (``openlineage_url`` /
``openlineage_namespace``) — unrelated to the former ``stores.lineage`` store,
which was removed (column lineage merged into observability). When
``openlineage_url`` is unset, the Surveyor builds no emitter and nothing is
emitted (zero cost).
"""
from __future__ import annotations

import sys
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from aqueduct.models import ModuleType

import httpx

_PRODUCER = "https://github.com/sadigaxund/aqueduct"
_RUN_EVENT_SCHEMA = (
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
)
_COLUMN_LINEAGE_FACET_SCHEMA = (
    "https://openlineage.io/spec/facets/1-2-0/"
    "ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet"
)

# Terminal run states recognised by the OpenLineage spec.
_VALID_EVENT_TYPES = frozenset({"START", "COMPLETE", "FAIL"})

_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_DELIVERY_ATTEMPTS = 2
_DELIVERY_BACKOFF_SECONDS = 2.0


def run_uuid(run_id: str) -> str:
    """Map Aqueduct's run_id to a stable OpenLineage runId (a UUID).

    The spec requires runId to be a UUID; Aqueduct run_ids (``run_2024…_a3f9``)
    are not, so derive a deterministic UUIDv5 — same run_id always yields the
    same runId, so START and COMPLETE/FAIL correlate.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_URL, run_id))


def _dataset_name(module: Any) -> str:
    """Best dataset identifier for an Ingress/Egress module."""
    cfg = getattr(module, "config", {}) or {}
    return str(cfg.get("path") or cfg.get("table") or module.id)


def extract_datasets(manifest: Any, namespace: str) -> tuple[list[dict], list[dict]]:
    """Map Ingress modules → input datasets and Egress modules → output datasets."""
    inputs: list[dict] = []
    outputs: list[dict] = []
    for m in manifest.modules:
        if m.type == ModuleType.Ingress:
            inputs.append({"namespace": namespace, "name": _dataset_name(m)})
        elif m.type == ModuleType.Egress:
            outputs.append({"namespace": namespace, "name": _dataset_name(m)})
    return inputs, outputs


def build_column_lineage_facet(rows: list[dict[str, str]], namespace: str) -> dict | None:
    """Build the OpenLineage ``columnLineage`` facet from lineage rows.

    ``rows`` come from ``compiler.lineage.compute_lineage_rows`` — each is
    ``{channel_id, output_column, source_table, source_column}``. Returns None
    when there is nothing to emit (no SQL Channels), so the facet is omitted
    rather than sent empty.
    """
    fields: dict[str, dict] = {}
    for r in rows:
        out_col = r.get("output_column")
        if not out_col:
            continue
        input_field = {
            "namespace": namespace,
            "name": r.get("source_table") or "UNKNOWN",
            "field": r.get("source_column") or "UNKNOWN",
        }
        entry = fields.setdefault(out_col, {"inputFields": []})
        if input_field not in entry["inputFields"]:
            entry["inputFields"].append(input_field)
    if not fields:
        return None
    return {
        "_producer": _PRODUCER,
        "_schemaURL": _COLUMN_LINEAGE_FACET_SCHEMA,
        "fields": fields,
    }


def build_run_event(
    event_type: str,
    *,
    run_id: str,
    job_namespace: str,
    job_name: str,
    inputs: list[dict],
    outputs: list[dict],
    event_time: str | None = None,
    error_message: str | None = None,
) -> dict:
    """Assemble a spec-compliant OpenLineage RunEvent.

    On ``FAIL`` an ``errorMessage`` run facet carries the failure text. The
    ``columnLineage`` facet, when present, is already attached to each output
    dataset by the caller.
    """
    if event_type not in _VALID_EVENT_TYPES:
        raise ValueError(f"invalid OpenLineage eventType: {event_type!r}")
    run_facets: dict[str, Any] = {}
    if error_message and event_type == "FAIL":
        run_facets["errorMessage"] = {
            "_producer": _PRODUCER,
            "_schemaURL": (
                "https://openlineage.io/spec/facets/1-0-0/"
                "ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet"
            ),
            "message": error_message,
            "programmingLanguage": "PYTHON",
        }
    return {
        "eventType": event_type,
        "eventTime": event_time or datetime.now(tz=timezone.utc).isoformat(),
        "run": {"runId": run_uuid(run_id), "facets": run_facets},
        "job": {"namespace": job_namespace, "name": job_name},
        "inputs": inputs,
        "outputs": outputs,
        "producer": _PRODUCER,
        "schemaURL": _RUN_EVENT_SCHEMA,
    }


class OpenLineageEmitter:
    """Posts OpenLineage RunEvents to a configured receiver in a daemon thread.

    Built by the Surveyor only when ``lineage.openlineage_url`` is set. Holds the
    manifest so START and COMPLETE/FAIL share the same job, datasets, and
    column-lineage facet.
    """

    def __init__(self, url: str, namespace: str, manifest: Any, timeout: float = 10.0) -> None:
        self._url = url.rstrip("/") + "/api/v1/lineage" if not url.rstrip("/").endswith("/api/v1/lineage") else url
        self._namespace = namespace
        self._manifest = manifest
        self._timeout = timeout
        self._started_runs: set[str] = set()  # run_ids that already emitted a START

    def _outputs_with_facet(self) -> tuple[list[dict], list[dict]]:
        from aqueduct.compiler.lineage import compute_lineage_rows
        inputs, outputs = extract_datasets(self._manifest, self._namespace)
        rows = compute_lineage_rows(self._manifest.modules, self._manifest.edges)
        facet = build_column_lineage_facet(rows, self._namespace)
        if facet is not None:
            for ds in outputs:
                ds["facets"] = {"columnLineage": facet}
        return inputs, outputs

    def emit(self, event_type: str, *, run_id: str, error_message: str | None = None,
             event_time: str | None = None) -> threading.Thread | None:
        # Lazy START: a terminal event for a run_id we never START-ed (e.g. a
        # heal re-run, which mints a fresh run_id that bypassed surveyor.start())
        # gets a synthetic START first, so a strict consumer never sees a
        # START-less COMPLETE/FAIL.
        if event_type in ("COMPLETE", "FAIL") and run_id not in self._started_runs:
            self._emit_one("START", run_id=run_id, event_time=event_time)
        return self._emit_one(
            event_type, run_id=run_id, error_message=error_message, event_time=event_time
        )

    def _emit_one(self, event_type: str, *, run_id: str, error_message: str | None = None,
                  event_time: str | None = None) -> threading.Thread | None:
        try:
            inputs, outputs = self._outputs_with_facet()
            event = build_run_event(
                event_type,
                run_id=run_id,
                job_namespace=self._namespace,
                job_name=self._manifest.blueprint_id,
                inputs=inputs,
                outputs=outputs,
                event_time=event_time,
                error_message=error_message,
            )
        except Exception as exc:  # noqa: BLE001 — building an event must never break a run
            print(f"[surveyor] openlineage event build failed: {exc}", file=sys.stderr)
            return None
        if event_type == "START":
            self._started_runs.add(run_id)
        return self._post(event)

    def _post(self, event: dict) -> threading.Thread:
        url, timeout = self._url, self._timeout

        def _send() -> None:
            import time as _time
            for attempt in range(1, _DELIVERY_ATTEMPTS + 1):
                retryable = False
                try:
                    resp = httpx.post(url, json=event, timeout=timeout,
                                      headers={"Content-Type": "application/json"})
                    if resp.status_code < 400:
                        return
                    retryable = resp.status_code in _RETRYABLE_STATUS
                    print(f"[surveyor] openlineage POST {url!r} returned HTTP {resp.status_code}",
                          file=sys.stderr)
                except httpx.RequestError as exc:
                    retryable = True
                    print(f"[surveyor] openlineage POST {url!r} failed: {exc}", file=sys.stderr)
                except Exception as exc:  # noqa: BLE001
                    print(f"[surveyor] openlineage POST {url!r} raised: {exc}", file=sys.stderr)
                    return
                if not retryable or attempt >= _DELIVERY_ATTEMPTS:
                    return
                _time.sleep(_DELIVERY_BACKOFF_SECONDS)

        thread = threading.Thread(target=_send, daemon=True, name="surveyor-openlineage")
        thread.start()
        return thread
