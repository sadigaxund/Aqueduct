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


__all__ = ["render_duckdb_type", "normalize_type_spelling"]
