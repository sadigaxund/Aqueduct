"""DuckDB's hub<->native type mapping (Phase 80 work package 3 — "Arrow type-hub").

Replaces ``duckdb_/channel.py``'s old ``_CAST_TYPE_ALIASES`` and
``duckdb_/ingress.py``'s old ``_TYPE_ALIASES`` (9-entry, scalar-only dicts,
both deleted by this package) with a real mapping from the full hub
vocabulary (``aqueduct/typehub.py``) to DuckDB's own SQL type spelling —
including the composite constructors (``array<T>``, ``map<K,V>``,
``struct<name:type,...>``) the old dicts never touched at all, which is
exactly the documented weak spot this package closes: ``array<int>`` as a
cast/schema_hint type used to reach DuckDB's parser raw and fail (DuckDB
wants ``INTEGER[]``, not ``array<int>``), despite the ``type.array`` leaf
already being declared ``supported`` (see ``capabilities.yml``'s comment on
this exact gap). Pure string logic — no ``duckdb`` import — so this module
needs no lazy-import discipline the way ``execute`` does, even though
``duckdb`` is a base dependency of this project.

Verified empirically against a real DuckDB connection (nested
array/map/struct combinations included — ``INTEGER[][]``,
``MAP(INTEGER, INTEGER)[]``, ``MAP(VARCHAR, STRUCT(y INTEGER))``, ...) that
every rendered spelling below is a real, executable ``CAST`` target.
"""

from __future__ import annotations

from aqueduct import typehub as hub

# hub scalar constructor -> DuckDB's canonical CAST spelling. Every entry
# verified as a real DuckDB CAST target (see module docstring).
_SCALAR: dict[type, str] = {
    hub.Boolean: "BOOLEAN",
    hub.TinyInt: "TINYINT",
    hub.SmallInt: "SMALLINT",
    hub.Int: "INTEGER",
    hub.BigInt: "BIGINT",
    hub.FloatT: "FLOAT",
    hub.DoubleT: "DOUBLE",
    hub.StringT: "VARCHAR",
    hub.BinaryT: "BLOB",
    hub.DateT: "DATE",
}


def render_duckdb_type(t: "hub.HubType | hub.NativeType") -> str:
    """Render one parsed hub type (or ``NativeType``) to DuckDB's SQL spelling.

    Registered as DuckDB's ``ExecutorProtocol.render_type``
    (``aqueduct/executor/duckdb_/engine.py``) — call through
    ``aqueduct.executor.protocol.render_native_type("duckdb", spelling)``
    rather than this function directly; that seam also handles the
    native-namespace / parse-failure branches this function does not.
    """
    if isinstance(t, hub.NativeType):
        # A caller reaching this branch already confirmed t.engine ==
        # "duckdb" (render_native_type's job); this function only ever sees
        # a value already destined for this engine.
        return t.spelling
    if isinstance(t, hub.TimestampTz):
        return "TIMESTAMPTZ"
    if isinstance(t, hub.TimestampNtz):
        return "TIMESTAMP"
    if isinstance(t, hub.Decimal):
        return f"DECIMAL({t.precision},{t.scale})"
    if isinstance(t, hub.Duration):
        # Integer-backed by design (see typehub.Duration's docstring) — the
        # unit is Aqueduct's own metadata, never consulted by DuckDB's cast
        # machinery, which only ever sees a plain 64-bit integer.
        return "BIGINT"
    if isinstance(t, hub.Array):
        return f"{render_duckdb_type(t.element)}[]"
    if isinstance(t, hub.Map):
        return f"MAP({render_duckdb_type(t.key)}, {render_duckdb_type(t.value)})"
    if isinstance(t, hub.Struct):
        fields = ", ".join(f"{f.name} {render_duckdb_type(f.type)}" for f in t.fields)
        return f"STRUCT({fields})"
    spelling = _SCALAR.get(type(t))
    if spelling is None:
        raise TypeError(f"render_duckdb_type: unrecognized hub type {t!r}")
    return spelling


def normalize_type_spelling(spelling: str) -> str:
    """Render a raw Blueprint type spelling (Channel ``op: cast`` column type
    or Ingress ``schema_hint`` field type) to DuckDB's own SQL spelling,
    preserving the old alias-dicts' raw-passthrough fallback for a spelling
    the hub does not recognize at all (DuckDB-only DDL the hub deliberately
    does not model, e.g. ``HUGEINT`` written bare instead of via the
    ``duckdb:`` native namespace, or ``BLOB``/``STRUCT(x INTEGER)`` written
    as DuckDB's own native spelling directly) — DuckDB's own parser remains
    the authority on whether ITS OWN native spelling is valid, same as the
    deleted dicts' fallback behavior.

    Raises:
        EnginePluginError: ``spelling`` is a native escape hatch naming a
            DIFFERENT engine (defensive — see
            ``aqueduct.executor.protocol.render_native_type``; the
            compile-time ``type.native.*`` gate should already have refused
            this on any gated path).
    """
    from aqueduct.executor.protocol import render_native_type
    from aqueduct.typehub import TypeSpellingError

    stripped = str(spelling).strip()
    try:
        return render_native_type("duckdb", stripped)
    except TypeSpellingError:
        return stripped


# hub scalar constructor -> DuckDB spelling, reversed. Deliberately reuses
# `_SCALAR` (never a second hand-maintained table) — only the entries needed
# for `widens_to()`'s numeric families matter in practice (TINYINT/SMALLINT/
# INTEGER/BIGINT/FLOAT/DOUBLE); the rest (BOOLEAN, VARCHAR, BLOB, DATE) are
# harmless to include and simply never match a widening family.
_REVERSE_SCALAR: dict[str, type] = {v: k for k, v in _SCALAR.items()}


def parse_duckdb_scalar_type(native_spelling: str) -> "hub.HubType | None":
    """Best-effort reverse mapping from a CONCRETE DuckDB scalar type
    spelling (as returned by ``str(rel.types[i])``, e.g. ``"BIGINT"``,
    ``"VARCHAR"``) back to a hub type.

    Deliberately narrow — backs ONLY the ``schema_hint``/``schema_match``
    numeric-widening check (see ``typehub.widens_to``), never general type
    parsing. Composite spellings (``INTEGER[]``, ``STRUCT(x INTEGER)``,
    ``MAP(...)``), parametrized ones (``DECIMAL(18,4)``), and ``TIMESTAMP``/
    ``TIMESTAMPTZ`` are all deliberately OUT of scope and return ``None`` —
    ``TIMESTAMP`` in particular must never be routed back through
    ``parse_type`` (bare ``timestamp`` is the one spelling the hub treats as
    a hard ambiguity error; see ``duckdb_/ingress.py``'s
    ``_normalize_actual_type`` docstring for why re-parsing a genuinely
    concrete DuckDB column type through that path would be wrong, not just
    unsupported). A caller receiving ``None`` falls back to the existing
    literal-spelling comparison unchanged.
    """
    cls = _REVERSE_SCALAR.get(str(native_spelling).strip().upper())
    return cls() if cls is not None else None


def schema_type_matches(hint_spelling: str, actual_native_spelling: str) -> bool:
    """True when a Blueprint-authored ``schema_hint``/``schema_match``
    ``type:`` spelling is satisfied by a column's ACTUAL DuckDB native type
    spelling (as returned by ``str(rel.types[i])``).

    Two checks, in order:

    1. **Exact** — ``hint_spelling`` rendered through the hub to DuckDB's own
       spelling equals ``actual_native_spelling`` (case-insensitive). This is
       the pre-existing behavior, unchanged, and covers everything: an exact
       hub-type match, a DuckDB-only native spelling round-tripping through
       ``normalize_type_spelling``'s raw-passthrough fallback, and
       ``TIMESTAMP``/``TIMESTAMPTZ`` (never re-parsed on the actual side).
    2. **Widened** — only reached when (1) fails. Both sides are parsed to
       hub types (the hint via ``typehub.parse_type``, the actual via the
       narrow ``parse_duckdb_scalar_type`` reverse table above) and accepted
       if ``typehub.widens_to(hint_hub, actual_hub)`` — the actual column is
       the SAME fixed-width numeric family and at least as wide as the
       hint. This is what makes ``quantity: integer`` (Spark's own inferred
       type for that column) validate against DuckDB's CSV sniffer, which
       only ever infers ``BIGINT`` for whole numbers regardless of value
       range — see ``typehub.widens_to``'s docstring for the full
       reasoning. Either parse failing (an unrecognized hint spelling, or an
       actual spelling outside the narrow reverse table) means no widening
       applies — this call returns ``False``, and the caller raises its
       normal mismatch error.

    Never touches a value — this is a type-name comparison, not a cast.

    Raises:
        EnginePluginError: ``hint_spelling`` is a native escape hatch naming
            a DIFFERENT engine (propagated from ``normalize_type_spelling``,
            not caught here — each call site wraps it in its own error type
            with its own context, same as before this function existed).
    """
    from aqueduct import typehub as hub

    normalized_actual = str(actual_native_spelling).strip().lower()
    if normalize_type_spelling(str(hint_spelling)).strip().lower() == normalized_actual:
        return True

    try:
        hint_hub = hub.parse_type(str(hint_spelling))
    except hub.TypeSpellingError:
        return False
    actual_hub = parse_duckdb_scalar_type(actual_native_spelling)
    if actual_hub is None:
        return False
    return hub.widens_to(hint_hub, actual_hub)


__all__ = [
    "render_duckdb_type",
    "normalize_type_spelling",
    "parse_duckdb_scalar_type",
    "schema_type_matches",
]
