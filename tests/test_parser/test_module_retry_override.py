"""Per-module `retry:` override (Phase 70).

A module-level `retry:` block inherits blueprint-level `retry_policy:`
values for any field left unset — same per-field inheritance shape as
agent cascade tiers (aqueduct/agent/cascade.py). Parser-only: merge happens
at parse time in aqueduct/parser/parser.py, so Module.retry is already the
FULLY-RESOLVED effective RetryPolicy (or None when the module declares no
`retry:` block at all).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aqueduct.parser.parser import ParseError, parse_dict

pytestmark = pytest.mark.unit


def _bp(modules: list[dict], retry_policy: dict | None = None) -> dict:
    return {
        "aqueduct": "1.0", "id": "bp1", "name": "BP",
        **({"retry_policy": retry_policy} if retry_policy else {}),
        "modules": modules,
        "edges": [{"from": modules[0]["id"], "to": modules[-1]["id"]}] if len(modules) > 1 else [],
    }


def _mod(mid: str, mtype: str = "Channel", retry: dict | None = None) -> dict:
    d = {"id": mid, "label": mid, "type": mtype, "config": {"sql": "SELECT 1"} if mtype == "Channel" else {}}
    if mtype == "Ingress":
        d["config"] = {"format": "csv", "path": "d.csv"}
    if mtype == "Egress":
        d["config"] = {"format": "parquet", "path": "o"}
    if retry is not None:
        d["retry"] = retry
    return d


class TestNoOverride:
    def test_no_retry_block_leaves_module_retry_none(self):
        bp = parse_dict(_bp([_mod("a", "Ingress"), _mod("b", "Egress")]), Path("."))
        assert all(m.retry is None for m in bp.modules)


class TestFieldInheritance:
    def test_module_override_inherits_unset_fields_from_blueprint(self):
        bp = parse_dict(
            _bp(
                [_mod("a", "Ingress", retry={"max_attempts": 2}), _mod("b", "Egress")],
                retry_policy={"max_attempts": 5, "on_exhaustion": "abort", "deadline_seconds": 120},
            ),
            Path("."),
        )
        a = next(m for m in bp.modules if m.id == "a")
        assert a.retry.max_attempts == 2          # module override wins
        assert a.retry.on_exhaustion == "abort"    # inherited from blueprint
        assert a.retry.deadline_seconds == 120     # inherited from blueprint

    def test_only_flagged_module_gets_an_override(self):
        bp = parse_dict(
            _bp([_mod("a", "Ingress", retry={"max_attempts": 3}), _mod("b", "Egress")]),
            Path("."),
        )
        a = next(m for m in bp.modules if m.id == "a")
        b = next(m for m in bp.modules if m.id == "b")
        assert a.retry is not None
        assert b.retry is None

    def test_blueprint_default_retry_policy_used_when_no_explicit_block(self):
        bp = parse_dict(
            _bp([_mod("a", "Ingress", retry={"on_exhaustion": "alert_only"}), _mod("b", "Egress")]),
            Path("."),
        )
        a = next(m for m in bp.modules if m.id == "a")
        assert a.retry.max_attempts == 1  # blueprint's own default, inherited
        assert a.retry.on_exhaustion == "alert_only"

    def test_backoff_overrides_as_whole_block(self):
        bp = parse_dict(
            _bp(
                [_mod("a", "Ingress", retry={"backoff": {"strategy": "fixed", "base_seconds": 5}}), _mod("b", "Egress")],
                retry_policy={"backoff": {"strategy": "linear", "base_seconds": 10, "max_seconds": 300, "jitter": False}},
            ),
            Path("."),
        )
        a = next(m for m in bp.modules if m.id == "a")
        assert a.retry.backoff_strategy == "fixed"
        assert a.retry.backoff_base_seconds == 5
        # max_seconds/jitter not set on the module's backoff block — the
        # whole block replaces, so these fall back to BackoffSchema's OWN
        # defaults (600 / True), not the blueprint's overridden values.
        assert a.retry.backoff_max_seconds == 600
        assert a.retry.jitter is True

    def test_backoff_inherited_whole_block_when_module_omits_it(self):
        bp = parse_dict(
            _bp(
                [_mod("a", "Ingress", retry={"max_attempts": 4}), _mod("b", "Egress")],
                retry_policy={"backoff": {"strategy": "linear", "base_seconds": 10, "max_seconds": 300, "jitter": False}},
            ),
            Path("."),
        )
        a = next(m for m in bp.modules if m.id == "a")
        assert a.retry.backoff_strategy == "linear"
        assert a.retry.backoff_base_seconds == 10
        assert a.retry.backoff_max_seconds == 300
        assert a.retry.jitter is False

    def test_transient_errors_override_replaces_list_not_merges(self):
        bp = parse_dict(
            _bp(
                [_mod("a", "Ingress", retry={"transient_errors": ["timeout"]}), _mod("b", "Egress")],
                retry_policy={"transient_errors": ["connection", "reset"]},
            ),
            Path("."),
        )
        a = next(m for m in bp.modules if m.id == "a")
        assert a.retry.transient_errors == ("timeout",)


class TestSchemaValidation:
    def test_unknown_key_rejected(self):
        with pytest.raises(ParseError):
            parse_dict(
                _bp([_mod("a", "Ingress", retry={"bogus_field": 1}), _mod("b", "Egress")]),
                Path("."),
            )

    def test_max_attempts_below_one_rejected(self):
        with pytest.raises(ParseError):
            parse_dict(
                _bp([_mod("a", "Ingress", retry={"max_attempts": 0}), _mod("b", "Egress")]),
                Path("."),
            )

    def test_invalid_on_exhaustion_value_rejected(self):
        with pytest.raises(ParseError):
            parse_dict(
                _bp([_mod("a", "Ingress", retry={"on_exhaustion": "not_a_real_mode"}), _mod("b", "Egress")]),
                Path("."),
            )


class TestManifestPropagation:
    def test_retry_survives_compile_and_to_dict(self):
        from aqueduct.compiler.compiler import compile as cc
        bp = parse_dict(
            _bp([_mod("a", "Ingress", retry={"max_attempts": 3}), _mod("b", "Egress")]),
            Path("."),
        )
        manifest = cc(bp)
        a = next(m for m in manifest.modules if m.id == "a")
        b = next(m for m in manifest.modules if m.id == "b")
        assert a.retry is not None and a.retry.max_attempts == 3
        assert b.retry is None

        d = manifest.to_dict()
        mod_a = next(m for m in d["modules"] if m["id"] == "a")
        mod_b = next(m for m in d["modules"] if m["id"] == "b")
        assert mod_a["retry"]["max_attempts"] == 3
        assert mod_b["retry"] is None
