"""Phase 78 — doctor's runtime capability check (aqueduct/doctor/checks_io.py::check_capabilities).

The compile-time gate (aqueduct/compiler/capability_check.py) blocks
UNSUPPORTED leaves but cannot check dependency VERSIONS (compile-time doesn't
know what's installed). This doctor check does — it walks the blueprint's
used capability leaves and validates any ``requires`` constraint against
``importlib.metadata``. Tests monkeypatch the installed version to exercise
both the pass and fail paths without needing a specific pyspark build.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import aqueduct.executor.spark.capabilities  # noqa: F401 — registers "spark"
from aqueduct.doctor.checks_io import check_capabilities

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_CUSTOM_SNIPPET = _REPO / "gallery" / "snippets" / "22_custom_datasource" / "blueprint.yml"
_PLAIN_SNIPPET = _REPO / "gallery" / "snippets" / "01_ingress_csv_options" / "blueprint.yml"


def test_no_version_constrained_capabilities_reports_ok():
    results = check_capabilities(_PLAIN_SNIPPET)
    assert results
    assert all(r.status in ("ok", "skip") for r in results)


def test_version_satisfied_reports_ok(monkeypatch):
    def _fake_version(name):
        return "4.1.2"

    monkeypatch.setattr("importlib.metadata.version", _fake_version)
    results = check_capabilities(_CUSTOM_SNIPPET)
    cap_results = [r for r in results if r.name.startswith("capabilities:")]
    assert cap_results, "expected at least one version-constrained capability result"
    assert all(r.status == "ok" for r in cap_results)


def test_version_unsatisfied_reports_fail(monkeypatch):
    def _fake_version(name):
        return "3.5.8"

    monkeypatch.setattr("importlib.metadata.version", _fake_version)
    results = check_capabilities(_CUSTOM_SNIPPET)
    cap_results = [r for r in results if r.name.startswith("capabilities:")]
    assert cap_results
    assert any(r.status == "fail" for r in cap_results)
    failed = [r for r in cap_results if r.status == "fail"][0]
    assert "3.5.8" in failed.detail
    assert "pyspark" in failed.detail


def test_missing_dependency_skips_gracefully(monkeypatch):
    from importlib.metadata import PackageNotFoundError

    def _raise(name):
        raise PackageNotFoundError(name)

    monkeypatch.setattr("importlib.metadata.version", _raise)
    results = check_capabilities(_CUSTOM_SNIPPET)
    cap_results = [r for r in results if r.name.startswith("capabilities:")]
    assert cap_results
    assert all(r.status == "skip" for r in cap_results)
    assert all("not installed" in r.detail for r in cap_results)


def test_bad_blueprint_path_skips_not_raises(tmp_path):
    bad = tmp_path / "nonexistent.yml"
    results = check_capabilities(bad)
    assert len(results) == 1
    assert results[0].status == "skip"


def test_unknown_engine_skips_not_raises():
    results = check_capabilities(_PLAIN_SNIPPET, engine="nonexistent-engine")
    assert len(results) == 1
    assert results[0].status == "skip"
