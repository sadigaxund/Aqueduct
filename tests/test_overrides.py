from __future__ import annotations

import json
import pickle

import pytest

from aqueduct.overrides import (
    OverrideError,
    _coerce_scalar,
    apply_to_model,
    deep_merge,
    model_accepts_path,
    parse_set_items,
    route_overrides,
    suggest_for_path,
    to_nested,
)
from aqueduct.config import AqueductConfig

pytestmark = pytest.mark.unit


# ── _coerce_scalar ─────────────────────────────────────────────────────────────


class TestCoerceScalar:
    def test_true_becomes_bool(self):
        assert _coerce_scalar("true") is True
        assert _coerce_scalar("True") is True

    def test_false_becomes_bool(self):
        assert _coerce_scalar("false") is False
        assert _coerce_scalar("FALSE") is False

    def test_null_becomes_none(self):
        assert _coerce_scalar("null") is None
        assert _coerce_scalar("none") is None
        assert _coerce_scalar("None") is None

    def test_int_stays_int(self):
        assert _coerce_scalar("5") == 5
        assert _coerce_scalar("-3") == -3

    def test_float_stays_float(self):
        assert _coerce_scalar("3.5") == 3.5
        assert _coerce_scalar("-0.5") == -0.5

    def test_string_fallback(self):
        assert _coerce_scalar("qwen:7b") == "qwen:7b"
        assert _coerce_scalar("hello") == "hello"


# ── parse_set_items ───────────────────────────────────────────────────────────


class TestParseSetItems:
    def test_scalar_parsed_correctly(self):
        items = parse_set_items(["agent.timeout=5"])
        assert len(items) == 1
        assert items[0].path == ("agent", "timeout")
        assert items[0].value == 5
        assert items[0].raw == "agent.timeout=5"

    def test_bool_value(self):
        items = parse_set_items(["danger.allow_multi_patch=true"])
        assert items[0].value is True

    def test_null_value(self):
        items = parse_set_items(["agent.max_heal_attempts_per_hour=null"])
        assert items[0].value is None

    def test_string_value(self):
        items = parse_set_items(["agent.model=claude-opus-4-8"])
        assert items[0].value == "claude-opus-4-8"

    def test_json_syntax_parses_dict(self):
        items = parse_set_items(['agent.budget:={"max_reprompts": 5}'])
        assert items[0].value == {"max_reprompts": 5}

    def test_json_syntax_invalid_raises(self):
        with pytest.raises(OverrideError, match="invalid JSON"):
            parse_set_items(["spark_config:=not-json"])

    def test_rejects_item_without_equals(self):
        with pytest.raises(OverrideError, match="must be PATH=VALUE"):
            parse_set_items(["bad-no-eq"])

    def test_rejects_empty_path(self):
        with pytest.raises(OverrideError, match="must be PATH=VALUE"):
            parse_set_items(["=value"])

    def test_rejects_dot_path_with_empty_segment(self):
        with pytest.raises(OverrideError, match="malformed"):
            parse_set_items(["agent..timeout=5"])

    def test_multiple_items(self):
        items = parse_set_items(["a.b=1", "c.d=true"])
        assert len(items) == 2
        assert items[0].path == ("a", "b")
        assert items[1].path == ("c", "d")


# ── to_nested ──────────────────────────────────────────────────────────────────


class TestToNested:
    def test_single_path(self):
        items = parse_set_items(["deployment.master_url=spark://h:7077"])
        result = to_nested(items)
        assert result == {"deployment": {"master_url": "spark://h:7077"}}

    def test_multiple_paths_merged(self):
        items = parse_set_items(["a.b=1", "a.c=2"])
        result = to_nested(items)
        assert result == {"a": {"b": 1, "c": 2}}

    def test_separate_roots(self):
        items = parse_set_items(["a.x=10", "b.y=20"])
        result = to_nested(items)
        assert result == {"a": {"x": 10}, "b": {"y": 20}}

    def test_deep_nesting(self):
        items = parse_set_items(["stores.observability.backend=postgres"])
        result = to_nested(items)
        assert result == {"stores": {"observability": {"backend": "postgres"}}}


# ── deep_merge ─────────────────────────────────────────────────────────────────


class TestDeepMerge:
    def test_overlay_wins(self):
        result = deep_merge({"a": 1, "b": 2}, {"a": 99})
        assert result == {"a": 99, "b": 2}

    def test_nested_merge(self):
        result = deep_merge({"x": {"y": 1}}, {"x": {"z": 2}})
        assert result == {"x": {"y": 1, "z": 2}}

    def test_no_mutation(self):
        base = {"a": 1}
        result = deep_merge(base, {"b": 2})
        assert base == {"a": 1}
        assert result == {"a": 1, "b": 2}

    def test_empty_overlay(self):
        result = deep_merge({"a": 1}, {})
        assert result == {"a": 1}


# ── apply_to_model ─────────────────────────────────────────────────────────────


class TestApplyToModel:
    def test_simple_override(self):
        cfg = AqueductConfig()
        result = apply_to_model(cfg, {"deployment": {"master_url": "local[2]"}})
        assert result.deployment.master_url == "local[2]"

    def test_nested_override(self):
        cfg = AqueductConfig()
        result = apply_to_model(cfg, {"agent": {"timeout": 300.0}})
        assert result.agent.timeout == 300.0

    def test_empty_overlay_returns_same_instance(self):
        cfg = AqueductConfig()
        result = apply_to_model(cfg, {})
        assert result is cfg

    def test_invalid_value_raises(self):
        cfg = AqueductConfig()
        with pytest.raises(OverrideError, match="invalid config"):
            apply_to_model(cfg, {"deployment": {"master_url": 123}})

    def test_immutable_original(self):
        cfg = AqueductConfig()
        original = cfg.deployment.master_url
        apply_to_model(cfg, {"deployment": {"master_url": "local[4]"}})
        assert cfg.deployment.master_url == original


# ── model_accepts_path ─────────────────────────────────────────────────────────


class TestModelAcceptsPath:
    def test_agent_budget_seconds_accepted(self):
        assert model_accepts_path(AqueductConfig, ("agent", "budget", "max_seconds"))

    def test_deployment_master_accepted(self):
        assert model_accepts_path(AqueductConfig, ("deployment", "master_url"))

    def test_spark_config_open_dict(self):
        assert model_accepts_path(AqueductConfig, ("spark_config", "spark.sql.shuffle.partitions"))

    def test_unknown_path_rejected(self):
        assert not model_accepts_path(AqueductConfig, ("agent", "approval_mode"))

    def test_bogus_top_level_rejected(self):
        assert not model_accepts_path(AqueductConfig, ("nonexistent", "key"))

    def test_accepts_deep_spark_config(self):
        assert model_accepts_path(AqueductConfig, ("spark_config", "any", "depth"))


# ── suggest_for_path ───────────────────────────────────────────────────────────


class TestSuggestForPath:
    def test_suggests_approval_mode_for_typo(self):
        from aqueduct.parser.schema import BlueprintSchema
        hint = suggest_for_path([AqueductConfig, BlueprintSchema], ("agent", "aproval_mode"))
        assert hint is not None
        assert "approval_mode" in hint

    def test_returns_suggestion_for_unknown_top_level(self):
        hint = suggest_for_path([AqueductConfig], ("zorg", "blarf"))
        assert hint is not None
        assert "zorg" in hint
        assert "agent" in hint or "danger" in hint or "deployment" in hint


# ── route_overrides ────────────────────────────────────────────────────────────


class TestRouteOverrides:
    def test_blueprint_path_routed_to_blueprint(self):
        config_nested, blueprint_nested = route_overrides(
            ["agent.approval=auto"], allow_blueprint=True
        )
        assert blueprint_nested == {"agent": {"approval": "auto"}}
        assert config_nested == {}

    def test_config_path_routed_to_config(self):
        config_nested, blueprint_nested = route_overrides(
            ["deployment.master_url=spark://h:7077"], allow_blueprint=True
        )
        assert config_nested == {"deployment": {"master_url": "spark://h:7077"}}
        assert blueprint_nested == {}

    def test_unknown_path_raises(self):
        with pytest.raises(OverrideError, match="no config field at path"):
            route_overrides(["agent.aproval_mode=auto"], allow_blueprint=False)

    def test_unknown_path_with_suggestion(self):
        from aqueduct.parser.schema import BlueprintSchema as BS
        try:
            route_overrides(["agent.aproval_mode=auto"], allow_blueprint=True)
        except OverrideError as exc:
            msg = str(exc)
            assert "aproval_mode" in msg
        else:
            pytest.fail("expected OverrideError")

    def test_allow_blueprint_false_rejects_blueprint_only(self):
        with pytest.raises(OverrideError, match="no config field at path"):
            route_overrides(["agent.approval_mode=auto"], allow_blueprint=False)

    def test_shared_path_wins_blueprint(self):
        cfg, bp = route_overrides(["agent.timeout=5"], allow_blueprint=True)
        assert bp == {"agent": {"timeout": 5}}
        assert cfg == {}
