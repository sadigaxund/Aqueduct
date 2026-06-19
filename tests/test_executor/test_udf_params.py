"""Phase 57 (UDF v2) — parameterized / context-aware UDFs.

Covers the pure factory-resolution helper (`_apply_udf_params`, no Spark) and
the Tier 0 param resolution through parse_dict. Live registration on a real
SparkSession is covered by a stub in tests/test_backlog.py.
"""
from __future__ import annotations

import pytest

from aqueduct.executor.spark.udf import UDFError, _apply_udf_params

pytestmark = pytest.mark.unit


def _make_masker(char="#", keep_last=2):
    def _mask(s):
        return (s[:-keep_last] if s and keep_last else s)
    return _mask


def test_empty_params_returns_fn_unchanged():
    fn = _make_masker
    assert _apply_udf_params("u", "make", "m", fn, {}) is fn


def test_factory_invoked_with_params():
    produced = _apply_udf_params("mask", "make_masker", "m", _make_masker,
                                 {"char": "*", "keep_last": 4})
    assert produced("hello") == "h"  # last 4 stripped


def test_non_callable_factory_with_params_errors():
    with pytest.raises(UDFError, match="not callable"):
        _apply_udf_params("u", "x", "m", "i am a string", {"a": 1})


def test_factory_returning_non_callable_errors():
    with pytest.raises(UDFError, match="must return a callable"):
        _apply_udf_params("u", "bad", "m", lambda **kw: 123, {"a": 1})


def test_factory_raising_is_wrapped():
    def boom(**kw):
        raise ValueError("nope")
    with pytest.raises(UDFError, match="raised: nope"):
        _apply_udf_params("u", "boom", "m", boom, {"a": 1})


def test_register_udfs_invokes_factory_and_registers_product(tmp_path):
    # End-to-end through register_udfs with a MagicMock spark (mirrors the
    # existing UDF tests' approach — no real SparkSession needed to verify the
    # factory is called and its product is what gets registered).
    import sys
    import types
    from unittest.mock import MagicMock

    from aqueduct.executor.spark.udf import register_udfs

    mod = types.ModuleType("aq_test_udf_factory")

    def make_adder(n=0):
        return lambda x: x + n

    mod.make_adder = make_adder
    sys.modules["aq_test_udf_factory"] = mod
    try:
        spark = MagicMock()
        register_udfs(
            ({"id": "add5", "lang": "python", "module": "aq_test_udf_factory",
              "entry": "make_adder", "return_type": "int", "params": {"n": 5}},),
            spark,
        )
        name, fn, rt = spark.udf.register.call_args[0]
        assert name == "add5" and rt == "int"
        assert fn(10) == 15  # the produced callable, parameterized with n=5
    finally:
        del sys.modules["aq_test_udf_factory"]


def test_params_resolve_context_tokens_through_parse():
    from aqueduct.parser.parser import parse_dict

    bp = parse_dict(
        {
            "aqueduct": "1.0",
            "id": "t.udf",
            "name": "T",
            "context": {"mask_char": "*"},
            "modules": [
                {"id": "load", "type": "Ingress", "label": "l",
                 "config": {"format": "parquet", "path": "x"}},
                {"id": "c", "type": "Channel", "label": "c",
                 "config": {"op": "sql", "query": "SELECT mask(name) AS name FROM load",
                            "udfs": ["mask"]}},
            ],
            "edges": [{"from": "load", "to": "c"}],
            "udf_registry": [
                {"id": "mask", "module": "mymod", "entry": "make_masker",
                 "return_type": "string",
                 "params": {"char": "${ctx.mask_char}", "keep_last": 4}},
            ],
        },
        base_dir="/tmp",
    )
    assert bp.udf_registry[0]["params"] == {"char": "*", "keep_last": 4}
