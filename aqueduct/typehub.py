"""The hub type vocabulary (Phase 80, work package 1 of 4 — "Arrow type-hub").

Today a Blueprint type string (Ingress ``schema_hint``, Channel ``op: cast``,
UDF ``return_type``) is a raw Spark DDL string handed to whichever engine
compiles it. On Spark that costs nothing — Spark's own parser reads it. On
every other engine it is read through a small hand-rolled alias dict (see
``aqueduct/executor/duckdb_/channel.py``, ``duckdb_/ingress.py``,
``spark/ingress.py`` — three independent ~9-entry dicts this module
replaces the NEED for future duplication of) and anything the dict does not
know is handed to that engine's own parser, raw, at runtime. The worst
consequence: Spark's ``timestamp`` is an instant; DuckDB's ``TIMESTAMP`` is a
naive wall-clock value. The same Blueprint, unmodified, silently means a
different value on each engine.

This module is the fix's FIRST piece: one internal type vocabulary every
engine will eventually declare a mapping to/from (work package 3 — NOT this
module's job). The vocabulary deliberately BORROWS Apache Arrow's semantics
for the constructors it defines (Arrow's timestamp-with-tz vs.
timestamp-without-tz distinction is exactly the ``timestamp`` ambiguity above,
already solved upstream) — this is not a pyarrow dependency (no import of
``pyarrow`` anywhere in this file) and not a mirror of Arrow's full type
taxonomy (no unions, no dictionary/run-end encoding, no fixed-size lists) —
just the deliberately-small subset both a columnar engine (DuckDB) and a
Spark-family engine can implement.

This module (work package 1) is engine-agnostic — no ``pyspark`` import, no
``duckdb`` import, top-level (same precedent as
``aqueduct/executor/channel_ops.py``) — and ships three things:

  1. The hub type dataclasses (``Boolean``, ``Int``, ``Decimal``,
     ``Array``, …) plus their canonical lowercase spellings.
  2. ``parse_type(spelling)`` — turns a user-written spelling into a hub
     type (or a ``NativeType`` passthrough marker for an explicit
     ``engine:spelling`` escape hatch). Unambiguous familiar aliases
     (``long`` → ``bigint``, ``integer`` → ``int``, …) canonicalize
     silently. A genuinely unknown spelling is a ``TypeSpellingError``.
  3. ``render(hub_type)`` — the inverse: a hub type back to its canonical
     spelling. ``parse_type(render(parse_type(s)))`` is stable.

What this module does NOT do (later work packages): no engine reads or
writes this vocabulary yet (package 3), no capability-leaf gating on types
(package 2), and the Blueprint grammar itself is unchanged — every
inventoried surface still carries a plain ``str`` (package 1 is validation
only, see ``aqueduct/compiler/compiler.py``'s integration step).
"""

from __future__ import annotations

import difflib
import re
from collections.abc import Iterable
from dataclasses import dataclass

from aqueduct.errors import TypeSpellingError
from aqueduct.warnings import emit as _emit

# ── Rule id (user-facing warning contract — see docs/spark_guide.md) ──────────

AMBIGUOUS_TYPE_SPELLING_RULE_ID = "ambiguous_type_spelling"


# ── Hub type vocabulary ─────────────────────────────────────────────────────
#
# Every class below is a VALUE type (frozen, structurally equal) describing
# comparison/storage semantics — never an engine spelling. Engine spellings
# are a work-package-3 concern (each engine's own hub<->native mapping
# table); this module only states what the hub type MEANS.


@dataclass(frozen=True)
class HubType:
    """Base class for every hub type. Never instantiated directly."""


@dataclass(frozen=True)
class Boolean(HubType):
    """True / False. Arrow ``bool``."""


@dataclass(frozen=True)
class TinyInt(HubType):
    """8-bit signed integer, range -128..127. Arrow ``int8``."""


@dataclass(frozen=True)
class SmallInt(HubType):
    """16-bit signed integer, range -32768..32767. Arrow ``int16``."""


@dataclass(frozen=True)
class Int(HubType):
    """32-bit signed integer. Arrow ``int32``."""


@dataclass(frozen=True)
class BigInt(HubType):
    """64-bit signed integer. Arrow ``int64``."""


@dataclass(frozen=True)
class FloatT(HubType):
    """32-bit IEEE-754 binary floating point. Arrow ``float32``."""


@dataclass(frozen=True)
class DoubleT(HubType):
    """64-bit IEEE-754 binary floating point. Arrow ``float64``."""


@dataclass(frozen=True)
class StringT(HubType):
    """Variable-length UTF-8 text, unbounded. Arrow ``string`` (utf8)."""


@dataclass(frozen=True)
class BinaryT(HubType):
    """Variable-length raw bytes, no encoding implied. Arrow ``binary``."""


@dataclass(frozen=True)
class DateT(HubType):
    """A calendar date (year/month/day) with no time-of-day and no time
    zone — the same date everywhere it is read. Arrow ``date32``.
    """


@dataclass(frozen=True)
class Decimal(HubType):
    """A fixed-point number with exactly ``precision`` total digits and
    ``scale`` digits after the decimal point (Arrow ``decimal128``
    semantics — exact arithmetic, no binary-float rounding).
    """

    precision: int
    scale: int


@dataclass(frozen=True)
class TimestampTz(HubType):
    """An INSTANT in time (a UTC point, independent of any wall clock) —
    Arrow ``timestamp[us, tz=UTC]`` semantics. Two ``timestamp_tz`` values
    from different source time zones compare and sort correctly against
    each other because both are already normalized to the same instant
    line. Maps to Spark's ``timestamp`` and DuckDB's ``TIMESTAMPTZ`` (see
    work package 3 for the actual engine mapping — this class only states
    the VALUE semantics).
    """


@dataclass(frozen=True)
class TimestampNtz(HubType):
    """A NAIVE wall-clock value ("2026-01-01 09:00:00" with no zone
    attached) — Arrow ``timestamp[us]`` (no tz) semantics. Two
    ``timestamp_ntz`` values compare as plain numbers; there is no instant
    they correspond to without an externally-supplied zone. Maps to
    Spark's ``timestamp_ntz`` and DuckDB's ``TIMESTAMP``.
    """


@dataclass(frozen=True)
class Array(HubType):
    """An ordered, variable-length list of ``element``-typed values, one
    element type shared by every item (Arrow ``list<element>``).
    """

    element: "HubType | NativeType"


@dataclass(frozen=True)
class StructField:
    """One named field inside a ``Struct`` — not a type on its own."""

    name: str
    type: "HubType | NativeType"


@dataclass(frozen=True)
class Map(HubType):
    """An unordered association from ``key``-typed to ``value``-typed
    entries, one key/value type pair shared by every entry (Arrow
    ``map<key, value>``). Duplicate keys are engine-defined, not a hub
    concern.
    """

    key: "HubType | NativeType"
    value: "HubType | NativeType"


@dataclass(frozen=True)
class Struct(HubType):
    """An ordered, fixed set of named, independently-typed fields (Arrow
    ``struct<...>``) — a row type nested inside a column.
    """

    fields: "tuple[StructField, ...]"


@dataclass(frozen=True)
class NativeType:
    """An explicit escape hatch: ``<engine>:<native spelling>``, e.g.
    ``duckdb:HUGEINT`` or ``spark:interval day to second``.

    Deliberately NOT a ``HubType`` subclass — a native type carries no hub
    semantics at all, it is a passthrough marker this module validates only
    for SHAPE (non-empty engine token, non-empty spelling). Whether a given
    engine understands the spelling, and whether using it on a Blueprint
    targeting a DIFFERENT engine is even allowed, is capability-leaf gating
    — work package 2, not this module.
    """

    engine: str
    spelling: str


# ── Accepted bare (non-namespaced) spellings ───────────────────────────────
#
# Generous-but-unambiguous: every key here maps to exactly ONE hub type.
# Sourced from the union of the three existing alias dicts this module
# replaces (aqueduct/executor/duckdb_/channel.py::_CAST_TYPE_ALIASES,
# duckdb_/ingress.py::_TYPE_ALIASES, spark/ingress.py::_TYPE_ALIASES) plus a
# small number of additional unambiguous DDL synonyms (``varchar``, ``char``)
# that DuckDB's own vocabulary already uses natively for the same concept.

_SCALAR_ALIASES: dict[str, type[HubType]] = {
    "boolean": Boolean,
    "bool": Boolean,
    "tinyint": TinyInt,
    "byte": TinyInt,
    "smallint": SmallInt,
    "short": SmallInt,
    "int": Int,
    "integer": Int,
    "bigint": BigInt,
    "long": BigInt,
    "float": FloatT,
    "double": DoubleT,
    "string": StringT,
    "varchar": StringT,
    "char": StringT,
    "binary": BinaryT,
    "date": DateT,
    "timestamp_tz": TimestampTz,
    "timestamp_ntz": TimestampNtz,
}

_CANONICAL_SPELLING: dict[type, str] = {
    Boolean: "boolean",
    TinyInt: "tinyint",
    SmallInt: "smallint",
    Int: "int",
    BigInt: "bigint",
    FloatT: "float",
    DoubleT: "double",
    StringT: "string",
    BinaryT: "binary",
    DateT: "date",
    TimestampTz: "timestamp_tz",
    TimestampNtz: "timestamp_ntz",
}

_ARRAY_RE = re.compile(r"^array<(.+)>$", re.IGNORECASE | re.DOTALL)
_MAP_RE = re.compile(r"^map<(.+)>$", re.IGNORECASE | re.DOTALL)
_STRUCT_RE = re.compile(r"^struct<(.*)>$", re.IGNORECASE | re.DOTALL)
_DECIMAL_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)
_NATIVE_RE = re.compile(r"^([a-zA-Z][a-zA-Z0-9_]*):(.+)$", re.DOTALL)


def _split_top_level(s: str, sep: str) -> list[str]:
    """Split ``s`` on ``sep`` characters that sit OUTSIDE any ``<...>``
    nesting — so ``map<string,array<int>>``'s inner content splits into
    ``["string", "array<int>"]``, not three pieces.
    """
    parts: list[str] = []
    depth = 0
    buf: list[str] = []
    for ch in s:
        if ch == "<":
            depth += 1
            buf.append(ch)
        elif ch == ">":
            depth -= 1
            buf.append(ch)
        elif ch == sep and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    parts.append("".join(buf))
    return parts


def _unknown_spelling_message(spelling: str) -> str:
    candidates = sorted(set(_SCALAR_ALIASES) | {"decimal", "timestamp"})
    close = difflib.get_close_matches(spelling.strip().lower(), candidates, n=3)
    hint = f" Did you mean: {', '.join(close)}?" if close else ""
    return (
        f"Unknown type spelling {spelling!r}.{hint} Composite types use "
        "array<T>, map<K,V>, struct<name:type,...>; decimal(p,s) needs an "
        "explicit precision/scale. For an engine-specific type Aqueduct does "
        "not validate, use the native namespace: 'spark:<type>' or "
        "'duckdb:<type>' (e.g. 'duckdb:HUGEINT')."
    )


def _parse_one(spelling: str, *, suppress: Iterable[str] | None) -> "HubType | NativeType":
    s = spelling.strip()
    if not s:
        raise TypeSpellingError("Type spelling must not be empty.")

    m = _ARRAY_RE.match(s)
    if m:
        return Array(_parse_one(m.group(1), suppress=suppress))

    m = _MAP_RE.match(s)
    if m:
        parts = _split_top_level(m.group(1), ",")
        if len(parts) != 2:
            raise TypeSpellingError(
                f"map<...> needs exactly two comma-separated type arguments "
                f"(key,value); got {s!r}."
            )
        key = _parse_one(parts[0], suppress=suppress)
        value = _parse_one(parts[1], suppress=suppress)
        return Map(key, value)

    m = _STRUCT_RE.match(s)
    if m:
        inner = m.group(1)
        if not inner.strip():
            raise TypeSpellingError(f"struct<...> must declare at least one field; got {s!r}.")
        fields: list[StructField] = []
        for chunk in _split_top_level(inner, ","):
            if ":" not in chunk:
                raise TypeSpellingError(
                    f"struct field {chunk.strip()!r} is missing ':type' — expected 'name:type'."
                )
            name, _, ftype = chunk.partition(":")
            name = name.strip()
            if not name:
                raise TypeSpellingError(f"struct field {chunk.strip()!r} has an empty field name.")
            fields.append(StructField(name, _parse_one(ftype, suppress=suppress)))
        return Struct(tuple(fields))

    m = _DECIMAL_RE.match(s)
    if m:
        precision, scale = int(m.group(1)), int(m.group(2))
        if precision < 1 or scale < 0 or scale > precision:
            raise TypeSpellingError(
                f"decimal({precision},{scale}) is not a valid precision/scale "
                "(need 1<=precision, 0<=scale<=precision)."
            )
        return Decimal(precision, scale)

    m = _NATIVE_RE.match(s)
    if m:
        engine = m.group(1).strip().lower()
        native_spelling = m.group(2).strip()
        if not native_spelling:
            raise TypeSpellingError(f"Native type {s!r} is missing a spelling after ':'.")
        return NativeType(engine, native_spelling)

    lower = s.lower()

    if lower == "decimal":
        # Bare `decimal` (no precision/scale) — Spark's own DDL parser
        # defaults this to DECIMAL(10,0); the hub matches that default
        # rather than rejecting it, since the default is well-defined, not
        # ambiguous (see the `timestamp` case below for the actual
        # ambiguous one).
        return Decimal(10, 0)

    if lower == "timestamp":
        # THE ambiguous spelling this package exists to flag: an instant on
        # Spark, a naive wall-clock value on DuckDB. Deprecation window:
        # parse succeeds (canonicalizes to the legacy Spark-compatible
        # meaning, timestamp_tz, so behavior does not change this release)
        # but emits a suppressible warning. FUTURE: this branch becomes
        # `raise TypeSpellingError(...)` in the next release — do not
        # remove the warning without flipping this to a hard error.
        _emit(
            AMBIGUOUS_TYPE_SPELLING_RULE_ID,
            "Bare 'timestamp' is ambiguous: it means an INSTANT "
            "(timestamp_tz — Spark's `timestamp`) on one engine and a NAIVE "
            "wall-clock value (timestamp_ntz — DuckDB's `TIMESTAMP`) on "
            "another. Write `timestamp_tz` or `timestamp_ntz` explicitly, or "
            "name the engine-native spelling directly (e.g. `spark:timestamp`) "
            "if you intend one engine only. Resolves to timestamp_tz (today's "
            "Spark behavior) for now — this becomes a parse ERROR in the next "
            "release.",
            suppress=suppress,
        )
        return TimestampTz()

    cls = _SCALAR_ALIASES.get(lower)
    if cls is not None:
        return cls()

    raise TypeSpellingError(_unknown_spelling_message(s))


def parse_type(spelling: str, *, suppress: Iterable[str] | None = None) -> "HubType | NativeType":
    """Parse a user-written type spelling into a hub type (or a
    ``NativeType`` passthrough marker).

    Familiar unambiguous spellings canonicalize silently (``long`` and
    ``bigint`` both parse to ``BigInt()``). ``timestamp`` (bare) is the one
    SEMANTICALLY ambiguous spelling accepted this release — it parses but
    emits a suppressible ``ambiguous_type_spelling`` warning (routed through
    ``aqueduct.warnings.emit``; pass ``suppress`` to plug into a caller's own
    resolved suppress set, e.g. a compile pass's Blueprint + engine union).
    Any other unknown BARE spelling is a ``TypeSpellingError``. A namespaced
    spelling (``spark:...`` / ``duckdb:...`` / ``<engine>:...``) always
    parses to a ``NativeType`` passthrough marker, validated for shape only.

    Args:
        spelling: The raw type string as written in a Blueprint (e.g.
            ``"bigint"``, ``"array<int>"``, ``"decimal(10,2)"``,
            ``"duckdb:HUGEINT"``).
        suppress: Rule ids to silence for the ``ambiguous_type_spelling``
            warning path (forwarded to ``aqueduct.warnings.emit`` verbatim).
            ``None`` falls back to the process-global suppress default.

    Returns:
        A ``HubType`` instance, or a ``NativeType`` for a namespaced
        spelling.

    Raises:
        TypeSpellingError: ``spelling`` is empty, not a string, or does not
            match any hub constructor / native-namespace shape.
    """
    if not isinstance(spelling, str):
        raise TypeSpellingError(
            f"Type spelling must be a string, got {type(spelling).__name__}: {spelling!r}."
        )
    if not spelling.strip():
        raise TypeSpellingError("Type spelling must not be empty.")
    return _parse_one(spelling, suppress=suppress)


def constructor_names() -> frozenset[str]:
    """Every hub type CONSTRUCTOR's canonical leaf-name suffix — one entry per
    constructor, never per instantiation (``decimal(10,2)`` and
    ``decimal(5,1)`` are the same ``"decimal"`` entry; ``array<int>`` and
    ``array<string>`` are the same ``"array"`` entry).

    This is the enumeration hook work package 2 (capability leaves,
    ``aqueduct/executor/capability_leaves.py::type_leaves()``) reads instead
    of hand-duplicating the constructor list — derived from the SAME
    ``_CANONICAL_SPELLING`` table ``render()`` uses for scalars, plus the
    composite/parametrized constructors that table does not cover (they take
    an argument so they have no single canonical spelling of their own:
    ``decimal``, ``array``, ``map``, ``struct``). ``NativeType`` is
    deliberately excluded — it is not a hub constructor, it is the
    passthrough escape hatch (see ``type.native.<engine>`` leaves, derived
    separately from the registered engine list).
    """
    return frozenset(_CANONICAL_SPELLING.values()) | {"decimal", "array", "map", "struct"}


def render(t: "HubType | NativeType") -> str:
    """Render a hub type (or ``NativeType``) back to its canonical
    spelling. Round-trip stable: ``parse_type(render(parse_type(s)))``
    equals ``parse_type(s)`` for any valid ``s``.
    """
    if isinstance(t, NativeType):
        return f"{t.engine}:{t.spelling}"
    if isinstance(t, Decimal):
        return f"decimal({t.precision},{t.scale})"
    if isinstance(t, Array):
        return f"array<{render(t.element)}>"
    if isinstance(t, Map):
        return f"map<{render(t.key)},{render(t.value)}>"
    if isinstance(t, Struct):
        return "struct<" + ",".join(f"{f.name}:{render(f.type)}" for f in t.fields) + ">"
    spelling = _CANONICAL_SPELLING.get(type(t))
    if spelling is None:
        raise TypeError(f"render() received an unrecognized hub type: {t!r}")
    return spelling


__all__ = [
    "AMBIGUOUS_TYPE_SPELLING_RULE_ID",
    "Array",
    "BigInt",
    "BinaryT",
    "Boolean",
    "DateT",
    "Decimal",
    "DoubleT",
    "FloatT",
    "HubType",
    "Int",
    "Map",
    "NativeType",
    "SmallInt",
    "StringT",
    "Struct",
    "StructField",
    "TimestampNtz",
    "TimestampTz",
    "TinyInt",
    "constructor_names",
    "parse_type",
    "render",
]
