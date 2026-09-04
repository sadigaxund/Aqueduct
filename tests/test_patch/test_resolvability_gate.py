"""Phase 88 — Gate 4 (resolvability gate) tests.

Every test that would otherwise reach the network mocks `httpx.get` — no test
in this file may hit the real network.
"""

from __future__ import annotations

import httpx
import pytest

from aqueduct.patch.gate_status import GateStatus
from aqueduct.patch.grammar import PatchSpec
from aqueduct.patch.resolvability_gate import run_resolvability_gate

pytestmark = pytest.mark.unit


def _spec(*requirements: str, patch_id: str = "p1") -> PatchSpec:
    return PatchSpec(
        patch_id=patch_id,
        rationale="need packages",
        operations=[{"op": "declare_dependency", "requirement": req} for req in requirements],
    )


class _FakeResponse:
    def __init__(self, status_code: int, json_data: dict | None = None):
        self.status_code = status_code
        self._json_data = json_data or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("error", request=None, response=self)

    def json(self):
        return self._json_data


def test_not_applicable_when_no_declare_dependency_op():
    spec = PatchSpec(
        patch_id="p1",
        rationale="unrelated fix",
        operations=[{"op": "replace_module_label", "module_id": "m1", "label": "L"}],
    )
    result = run_resolvability_gate(spec)
    assert result.status == GateStatus.NOT_APPLICABLE
    assert result.requirements == []


def test_pass_for_already_installed_package():
    # pydantic is a real, always-installed dependency of this repo — no
    # network call should even happen since requirement_status short-circuits.
    spec = _spec("pydantic")
    result = run_resolvability_gate(spec)
    assert result.status == GateStatus.PASS


def test_warn_when_resolves_on_pypi_but_not_installed(monkeypatch):
    spec = _spec("totally-not-installed-pkg>=1.0", patch_id="patch-abc")

    def fake_get(url, timeout=None):
        return _FakeResponse(200, {"releases": {"1.0": [], "2.0": []}})

    monkeypatch.setattr(httpx, "get", fake_get)
    result = run_resolvability_gate(spec)
    assert result.status == GateStatus.WARN
    assert "resolves on PyPI but is not installed" in result.detail
    assert "pip install 'totally-not-installed-pkg>=1.0'" in result.detail
    assert "aqueduct patch apply patch-abc" in result.detail


def test_fail_no_such_package(monkeypatch):
    spec = _spec("this-package-does-not-exist-xyz")

    def fake_get(url, timeout=None):
        return _FakeResponse(404)

    monkeypatch.setattr(httpx, "get", fake_get)
    result = run_resolvability_gate(spec)
    assert result.status == GateStatus.FAIL
    assert "no such package" in result.detail


def test_fail_no_version_satisfying(monkeypatch):
    spec = _spec("somepkg>=99.0")

    def fake_get(url, timeout=None):
        return _FakeResponse(200, {"releases": {"1.0": [], "2.0": []}})

    monkeypatch.setattr(httpx, "get", fake_get)
    result = run_resolvability_gate(spec)
    assert result.status == GateStatus.FAIL
    assert "no version satisfying" in result.detail


def test_unavailable_on_connection_error(monkeypatch):
    spec = _spec("somepkg-not-installed>=1.0")

    def fake_get(url, timeout=None):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx, "get", fake_get)
    result = run_resolvability_gate(spec)
    assert result.status == GateStatus.UNAVAILABLE
    assert "could not run" in result.detail


def test_unparseable_pypi_version_skipped_not_counted_as_failing(monkeypatch):
    """A version PyPI publishes that the PEP440-lite comparator cannot read
    is SKIPPED, not counted as a failure — mirrors requirement_status's
    unknown_version reasoning."""
    spec = _spec("somepkg>=1.0")

    def fake_get(url, timeout=None):
        return _FakeResponse(
            200, {"releases": {"2024.1a0": [], "1.5": []}}
        )  # first unparseable, second satisfies

    monkeypatch.setattr(httpx, "get", fake_get)
    result = run_resolvability_gate(spec)
    assert result.status == GateStatus.WARN


def test_worst_verdict_wins_across_multiple_ops(monkeypatch):
    """FAIL > UNAVAILABLE > WARN > PASS — a FAIL anywhere makes the whole
    gate FAIL even if other requirements pass."""
    spec = _spec("pydantic", "this-package-does-not-exist-xyz")

    call_count = {"n": 0}

    def fake_get(url, timeout=None):
        call_count["n"] += 1
        return _FakeResponse(404)

    monkeypatch.setattr(httpx, "get", fake_get)
    result = run_resolvability_gate(spec)
    assert result.status == GateStatus.FAIL
    # pydantic is installed (PASS, no network call); the missing package
    # hits the network and fails.
    assert call_count["n"] == 1
    assert len(result.requirements) == 2


def test_worst_verdict_unavailable_beats_warn(monkeypatch):
    spec = _spec("totally-not-installed-pkg>=1.0", "another-missing-pkg>=1.0")

    calls = {"n": 0}

    def fake_get(url, timeout=None):
        calls["n"] += 1
        if calls["n"] == 1:
            return _FakeResponse(200, {"releases": {"1.0": [], "2.0": []}})
        raise httpx.ConnectError("boom")

    monkeypatch.setattr(httpx, "get", fake_get)
    result = run_resolvability_gate(spec)
    assert result.status == GateStatus.UNAVAILABLE
