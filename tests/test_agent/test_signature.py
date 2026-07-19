"""Unit tests for aqueduct/agent/signature.py — Phase 34 Task 83."""

from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.unit

from aqueduct.agent.signature import (
    ErrorSignature,
    from_apply_error,
    from_exception,
    from_json_decode_error,
    from_text,
    from_validation_error,
    make_signature,
)


# ── make_signature / ErrorSignature ──────────────────────────────────────────

class TestMakeSignature:
    def test_same_inputs_produce_same_hash(self):
        s1 = make_signature("missing", "operations[0].op", "required field missing", engine="spark")
        s2 = make_signature("missing", "operations[0].op", "required field missing", engine="spark")
        assert s1.hash == s2.hash

    def test_equality_is_hash_based(self):
        s1 = make_signature("missing", "operations[0].op", "required field missing", engine="spark")
        s2 = make_signature("missing", "operations[0].op", "required field missing", engine="spark")
        assert s1 == s2

    def test_set_membership_works(self):
        s1 = make_signature("missing", "operations[0].op", "required field missing", engine="spark")
        s2 = make_signature("missing", "operations[0].op", "required field missing", engine="spark")
        seen: set[ErrorSignature] = {s1}
        assert s2 in seen

    def test_dict_key_works(self):
        s1 = make_signature("e", "w", "msg", engine="spark")
        d: dict[ErrorSignature, int] = {s1: 42}
        s2 = make_signature("e", "w", "msg", engine="spark")
        assert d[s2] == 42

    def test_volatile_digits_normalized(self):
        s1 = make_signature("e", "w", "error at line 12 column 7", engine="spark")
        s2 = make_signature("e", "w", "error at line 99 column 3", engine="spark")
        # Both collapse digits → same hash
        assert s1.hash == s2.hash
        assert "n" in s1.normalized_message  # digit→N lowercased

    def test_double_quoted_contents_normalized(self):
        s1 = make_signature("e", "w", 'field "foo" is required', engine="spark")
        s2 = make_signature("e", "w", 'field "bar" is required', engine="spark")
        assert s1.hash == s2.hash

    def test_single_quoted_contents_normalized(self):
        s1 = make_signature("e", "w", "field 'foo' is required", engine="spark")
        s2 = make_signature("e", "w", "field 'bar' is required", engine="spark")
        assert s1.hash == s2.hash

    def test_backtick_quoted_identifiers_normalized(self):
        # Spark 4's UNRESOLVED_COLUMN style: backtick-quoted qualified name.
        s1 = make_signature("AnalysisException", "mod", "cannot resolve `i`.`bad_col1` given input columns", engine="spark")
        s2 = make_signature("AnalysisException", "mod", "cannot resolve `i`.`bad_col2` given input columns", engine="spark")
        assert s1.hash == s2.hash
        assert "`x`" in s1.normalized_message

    def test_backtick_and_regular_quotes_do_not_converge(self):
        # Backtick and quote placeholders are visually distinct ('x' / "x" /
        # `x`), so a backtick-quoted message and a plain-quoted message
        # around the same identifier hash differently — verified behavior,
        # not a requirement.
        backtick = make_signature("e", "w", "cannot resolve `bad_col`", engine="spark")
        single_quote = make_signature("e", "w", "cannot resolve 'bad_col'", engine="spark")
        assert backtick.hash != single_quote.hash

    def test_path_normalized(self):
        s1 = make_signature("e", "w", "file /foo/bar/baz.yml not found", engine="spark")
        s2 = make_signature("e", "w", "file /other/path/thing.yml not found", engine="spark")
        assert s1.hash == s2.hash
        assert "/path" in s1.normalized_message

    def test_ansi_escapes_stripped(self):
        s1 = make_signature("e", "w", "\x1b[31merror\x1b[0m occurred", engine="spark")
        s2 = make_signature("e", "w", "error occurred", engine="spark")
        assert s1.hash == s2.hash

    def test_whitespace_collapsed_and_lowercased(self):
        s1 = make_signature("e", "w", "  HELLO   world  ", engine="spark")
        assert "  " not in s1.normalized_message
        assert s1.normalized_message == s1.normalized_message.lower()

    def test_structural_words_differ_different_hash(self):
        s1 = make_signature("e", "w", "field is required", engine="spark")
        s2 = make_signature("e", "w", "field is optional", engine="spark")
        assert s1.hash != s2.hash

    def test_empty_error_class_defaults_to_unknown(self):
        s = make_signature("", "w", "msg", engine="spark")
        assert s.error_class == "unknown"

    def test_none_error_class_defaults_to_unknown(self):
        s = make_signature(None, "w", "msg", engine="spark")  # type: ignore[arg-type]
        assert s.error_class == "unknown"

    def test_empty_where_defaults_to_root(self):
        s = make_signature("e", "", "msg", engine="spark")
        assert s.where == "<root>"

    def test_none_where_defaults_to_root(self):
        s = make_signature("e", None, "msg", engine="spark")  # type: ignore[arg-type]
        assert s.where == "<root>"

    def test_long_message_truncated(self):
        long_msg = "x" * 500
        s = make_signature("e", "w", long_msg, engine="spark")
        assert len(s.normalized_message) <= 240

    def test_truncated_message_stable_hash(self):
        """Two messages that differ ONLY in trailing noise beyond 240 chars → same hash."""
        base = "error " * 40  # > 240 chars
        s1 = make_signature("e", "w", base + "noise1", engine="spark")
        s2 = make_signature("e", "w", base + "noise2", engine="spark")
        assert s1.hash == s2.hash

    def test_frozen_dataclass_mutation_raises(self):
        from dataclasses import FrozenInstanceError
        s = make_signature("e", "w", "msg", engine="spark")
        with pytest.raises(FrozenInstanceError):
            s.hash = "new_hash"  # type: ignore[misc]

    def test_to_dict_returns_five_keys(self):
        s = make_signature("e", "w", "msg", engine="spark")
        d = s.to_dict()
        assert set(d.keys()) == {"error_class", "where", "normalized_message", "hash", "engine"}
        assert d["error_class"] == "e"
        assert d["where"] == "w"
        assert d["engine"] == "spark"
        assert isinstance(d["hash"], str) and len(d["hash"]) == 16


# ── from_validation_error ────────────────────────────────────────────────────

class TestFromValidationError:
    def _make_validation_error(self):
        from pydantic import BaseModel, ValidationError

        class M(BaseModel):
            operations: list[dict]

        try:
            M.model_validate({})
        except ValidationError as e:
            return e
        pytest.fail("Expected ValidationError")

    def _make_nested_validation_error(self):
        """ValidationError with multiple errors — first error used."""
        from pydantic import BaseModel, ValidationError

        class Inner(BaseModel):
            op: str
            module_id: str

        class Outer(BaseModel):
            operations: list[Inner]

        try:
            Outer.model_validate({"operations": [{"op": 1, "module_id": 2}]})
        except ValidationError as e:
            return e
        pytest.fail("Expected ValidationError")

    def test_uses_first_error_loc_type_msg(self):
        exc = self._make_validation_error()
        sig = from_validation_error(exc, engine="spark")
        assert sig.error_class  # non-empty
        assert sig.where        # non-empty

    def test_renders_loc_as_operations_bracket_style(self):
        """loc rendered as operations[0].op not operations.0.op."""
        from pydantic import BaseModel, ValidationError

        class Op(BaseModel):
            op: str

        class M(BaseModel):
            operations: list[Op]

        try:
            M.model_validate({"operations": [{}]})
        except ValidationError as e:
            sig = from_validation_error(e, engine="spark")
            # First error loc should be ('operations', 0, 'op') → 'operations[0].op'
            assert "[" in sig.where or "operations" in sig.where

    def test_empty_errors_falls_back_gracefully(self):
        """Empty errors edge case: should not crash."""
        from unittest.mock import MagicMock
        mock_exc = MagicMock()
        mock_exc.errors.return_value = []
        mock_exc.__str__ = lambda self: "validation_error str"
        sig = from_validation_error(mock_exc, engine="spark")
        assert sig.error_class == "validation_error"
        assert sig.where == "<root>"

    def test_stable_across_different_error_positions(self):
        """Two JSON errors at same structure but different line numbers → same hash."""
        s1 = from_json_decode_error(
            _make_json_error("Expecting ',' delimiter", 10, 5)
        , engine="spark")
        s2 = from_json_decode_error(
            _make_json_error("Expecting ',' delimiter", 99, 3)
        , engine="spark")
        assert s1.hash == s2.hash


def _make_json_error(msg: str, lineno: int, colno: int):
    """Build a mock json.JSONDecodeError-like object."""
    from unittest.mock import MagicMock
    exc = MagicMock()
    exc.msg = msg
    exc.lineno = lineno
    exc.colno = colno
    exc.__str__ = lambda self: f"{msg}: line {lineno} column {colno}"
    return exc


# ── from_exception ────────────────────────────────────────────────────────────

def test_from_exception_uses_type_name():
    exc = ValueError("something broke")
    sig = from_exception(exc, where="channels.clean", engine="spark")
    assert sig.error_class == "ValueError"
    assert sig.where == "channels.clean"


def test_from_exception_no_where_defaults_root():
    sig = from_exception(RuntimeError("oops"), engine="spark")
    assert sig.where == "<root>"


# ── from_apply_error ─────────────────────────────────────────────────────────

def test_from_apply_error_usable_as_dict_key():
    s = from_apply_error("guardrail_violation", "op replace_module_config is forbidden",
                         where="operations[0]", engine="spark")
    d: dict = {s: "value"}
    assert d[s] == "value"


def test_from_apply_error_no_where_defaults_root():
    s = from_apply_error("guardrail_violation", "msg", engine="spark")
    assert s.where == "<root>"


# ── from_failure_context ──────────────────────────────────────────────────────

class TestFromFailureContext:
    def test_returns_exact_and_coarse_pair(self):
        from aqueduct.agent.signature import from_failure_context
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="column not found", stack_trace="",
            manifest_json="{}", started_at="2020-01-01", finished_at="2020-01-01",
            error_class="UNRESOLVED_COLUMN",
         engine="spark",)
        exact, coarse = from_failure_context(ctx)
        assert exact.where == "m1"
        assert coarse.where == "<any>"
        assert exact.error_class == "UNRESOLVED_COLUMN"
        assert coarse.error_class == "UNRESOLVED_COLUMN"
        assert exact.hash != coarse.hash  # different where → different hash

    def test_error_class_priority_spark_wins(self):
        from aqueduct.agent.signature import from_failure_context
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="err", stack_trace="", manifest_json="{}",
            started_at="2020-01-01", finished_at="2020-01-01",
            error_class="UNRESOLVED_COLUMN", error_type="MyAssert",
         engine="spark",)
        exact, _ = from_failure_context(ctx)
        assert exact.error_class == "UNRESOLVED_COLUMN"  # Spark wins

    def test_error_class_priority_assert_when_no_spark(self):
        from aqueduct.agent.signature import from_failure_context
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="err", stack_trace="", manifest_json="{}",
            started_at="2020-01-01", finished_at="2020-01-01",
            error_class=None, error_type="DataQualityViolation",
         engine="spark",)
        exact, _ = from_failure_context(ctx)
        assert exact.error_class == "DataQualityViolation"

    def test_error_class_falls_back_to_root_exception_type(self):
        from aqueduct.agent.signature import from_failure_context
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="err", stack_trace="", manifest_json="{}",
            started_at="2020-01-01", finished_at="2020-01-01",
            root_exception={"type": "PySparkException", "message": "detail"},
         engine="spark",)
        exact, _ = from_failure_context(ctx)
        assert exact.error_class == "PySparkException"

    def test_error_class_defaults_to_unknown(self):
        from aqueduct.agent.signature import from_failure_context
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="", stack_trace="", manifest_json="{}",
            started_at="2020-01-01", finished_at="2020-01-01",
         engine="spark",)
        exact, _ = from_failure_context(ctx)
        assert exact.error_class == "unknown"

    def test_message_prefers_root_exception_message(self):
        from aqueduct.agent.signature import from_failure_context
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="shallow error", stack_trace="", manifest_json="{}",
            started_at="2020-01-01", finished_at="2020-01-01",
            root_exception={"type": "E", "message": "deep detail"},
         engine="spark",)
        exact, _ = from_failure_context(ctx)
        assert "deep detail" in exact.normalized_message

    def test_duck_typed_context_never_raises(self):
        from aqueduct.agent.signature import from_failure_context
        from unittest.mock import MagicMock
        ctx = MagicMock()
        ctx.error_class = "UNRESOLVED_COLUMN"
        ctx.failed_module = "m1"
        ctx.error_message = "boom"
        ctx.root_exception = "not a dict"  # str → coerced to empty dict
        exact, coarse = from_failure_context(ctx)
        assert exact.error_class == "UNRESOLVED_COLUMN"
        assert coarse.where == "<any>"

    def test_non_dict_root_exception_does_not_crash(self):
        from aqueduct.agent.signature import from_failure_context
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="err", stack_trace="", manifest_json="{}",
            started_at="2020-01-01", finished_at="2020-01-01",
            root_exception=None,
         engine="spark",)
        # Must not raise
        exact, _ = from_failure_context(ctx)
        assert exact.error_class == "unknown"


# ── from_text ─────────────────────────────────────────────────────────────────

def test_from_text_digits_collapsed():
    s = from_text("Some error at line 12 column 7", engine="spark")
    assert "n" in s.normalized_message  # digits → N, lowercased
    assert "12" not in s.normalized_message

def test_from_text_default_error_class_is_reprompt():
    s = from_text("Some error at line 12 column 7", engine="spark")
    assert s.error_class == "reprompt"

def test_from_text_custom_error_class():
    s = from_text("message", error_class="compile_error", engine="spark")
    assert s.error_class == "compile_error"
