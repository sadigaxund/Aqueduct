"""aqueduct/typehub.py — hub type vocabulary (Phase 80, work package 1 of 4).

Parse / canonicalize / reject / native-namespace / round-trip coverage, plus
bare-`timestamp` — a hard compile-time rejection, no deprecation window (user
decision 2026-07-23).
"""

from __future__ import annotations

import warnings

import pytest

from aqueduct import AqueductWarning
from aqueduct.errors import TypeSpellingError
from aqueduct.typehub import (
    AMBIGUOUS_TYPE_SPELLING_RULE_ID,
    Array,
    BigInt,
    BinaryT,
    Boolean,
    DateT,
    Decimal,
    DoubleT,
    FloatT,
    Int,
    Map,
    NativeType,
    SmallInt,
    StringT,
    Struct,
    StructField,
    TimestampNtz,
    TimestampTz,
    TinyInt,
    parse_type,
    render,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _ensure_warnings_caught():
    warnings.simplefilter("always", AqueductWarning)
    yield


# ── Scalar canonicalization ────────────────────────────────────────────────

@pytest.mark.parametrize(
    "spelling,expected",
    [
        ("boolean", Boolean()),
        ("bool", Boolean()),
        ("BOOL", Boolean()),
        ("tinyint", TinyInt()),
        ("byte", TinyInt()),
        ("smallint", SmallInt()),
        ("short", SmallInt()),
        ("int", Int()),
        ("integer", Int()),
        ("INTEGER", Int()),
        ("bigint", BigInt()),
        ("long", BigInt()),
        ("LONG", BigInt()),
        ("float", FloatT()),
        ("double", DoubleT()),
        ("string", StringT()),
        ("varchar", StringT()),
        ("char", StringT()),
        ("STRING", StringT()),
        ("binary", BinaryT()),
        ("date", DateT()),
        ("timestamp_tz", TimestampTz()),
        ("timestamp_ntz", TimestampNtz()),
    ],
)
def test_scalar_aliases_canonicalize(spelling, expected):
    assert parse_type(spelling) == expected


def test_bare_decimal_defaults_to_10_0():
    assert parse_type("decimal") == Decimal(10, 0)


def test_decimal_with_precision_scale():
    assert parse_type("decimal(10,2)") == Decimal(10, 2)
    assert parse_type("decimal( 38 , 4 )") == Decimal(38, 4)


def test_decimal_invalid_precision_scale_rejected():
    with pytest.raises(TypeSpellingError):
        parse_type("decimal(0,0)")
    with pytest.raises(TypeSpellingError):
        parse_type("decimal(5,10)")


# ── Composite types ─────────────────────────────────────────────────────────

def test_array():
    assert parse_type("array<int>") == Array(Int())


def test_map():
    assert parse_type("map<string,int>") == Map(StringT(), Int())


def test_struct():
    assert parse_type("struct<a:int,b:string>") == Struct(
        (StructField("a", Int()), StructField("b", StringT()))
    )


def test_nested_composite():
    assert parse_type("array<map<string,struct<a:int>>>") == Array(
        Map(StringT(), Struct((StructField("a", Int()),)))
    )


def test_struct_missing_colon_rejected():
    with pytest.raises(TypeSpellingError, match="missing ':type'"):
        parse_type("struct<a>")


def test_struct_empty_rejected():
    with pytest.raises(TypeSpellingError, match="at least one field"):
        parse_type("struct<>")


def test_map_wrong_arity_rejected():
    with pytest.raises(TypeSpellingError, match="exactly two"):
        parse_type("map<string>")


# ── Native namespace ─────────────────────────────────────────────────────────

def test_native_namespace():
    assert parse_type("duckdb:HUGEINT") == NativeType("duckdb", "HUGEINT")
    assert parse_type("spark:interval day to second") == NativeType(
        "spark", "interval day to second"
    )


def test_native_namespace_nested_inside_composite():
    assert parse_type("array<duckdb:HUGEINT>") == Array(NativeType("duckdb", "HUGEINT"))


def test_native_namespace_engine_lowercased():
    assert parse_type("SPARK:foo") == NativeType("spark", "foo")


def test_native_namespace_empty_spelling_rejected():
    with pytest.raises(TypeSpellingError):
        parse_type("spark:")


# ── Rejection / unknown spellings ────────────────────────────────────────────

def test_unknown_spelling_rejected():
    with pytest.raises(TypeSpellingError, match="Unknown type spelling"):
        parse_type("notarealtype")


def test_unknown_spelling_names_native_hatch():
    with pytest.raises(TypeSpellingError, match="native namespace"):
        parse_type("notarealtype")


def test_empty_spelling_rejected():
    with pytest.raises(TypeSpellingError):
        parse_type("")
    with pytest.raises(TypeSpellingError):
        parse_type("   ")


def test_non_string_spelling_rejected():
    with pytest.raises(TypeSpellingError):
        parse_type(123)  # type: ignore[arg-type]


# ── Bare `timestamp` — ambiguous, hard compile-time rejection ───────────────
# No deprecation window (user decision 2026-07-23): Spark's `timestamp` is an
# INSTANT, DuckDB's `TIMESTAMP` is NAIVE wall-clock — there is no safe default
# to silently fall back on, so this raises unconditionally.

def test_bare_timestamp_raises_type_spelling_error():
    with pytest.raises(TypeSpellingError, match="timestamp_tz"):
        parse_type("timestamp")
    with pytest.raises(TypeSpellingError, match="timestamp_ntz"):
        parse_type("timestamp")


def test_bare_timestamp_not_suppressible():
    """There is no warning left to suppress — passing the (now-inert) rule
    id must not resurrect the old parse-with-warning behavior."""
    with pytest.raises(TypeSpellingError, match="timestamp_tz|timestamp_ntz"):
        parse_type("timestamp", suppress={AMBIGUOUS_TYPE_SPELLING_RULE_ID})


def test_timestamp_tz_and_ntz_do_not_warn():
    with warnings.catch_warnings(record=True) as caught:
        parse_type("timestamp_tz")
        parse_type("timestamp_ntz")
    assert not any(AMBIGUOUS_TYPE_SPELLING_RULE_ID in str(w.message) for w in caught)


# ── render() round-trip ──────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "spelling",
    [
        "boolean", "tinyint", "smallint", "int", "bigint", "float", "double",
        "string", "binary", "date", "timestamp_tz", "timestamp_ntz",
        "decimal(10,2)", "array<int>", "map<string,int>",
        "struct<a:int,b:string>", "array<map<string,struct<a:int>>>",
        "duckdb:HUGEINT", "spark:interval day to second",
    ],
)
def test_round_trip_stable(spelling):
    t = parse_type(spelling)
    rendered = render(t)
    assert parse_type(rendered) == t


def test_render_canonicalizes_aliases():
    assert render(parse_type("long")) == "bigint"
    assert render(parse_type("varchar")) == "string"
    assert render(parse_type("integer")) == "int"


def test_render_unrecognized_type_raises():
    class NotAHubType:
        pass

    with pytest.raises(TypeError):
        render(NotAHubType())  # type: ignore[arg-type]
