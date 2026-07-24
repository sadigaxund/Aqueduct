"""Spark's hub<->native type mapping (Phase 80 work package 3 — "Arrow type-hub").

Replaces ``spark/ingress.py``'s old ``_TYPE_ALIASES`` 5-entry dict (deleted by
this package) with a real mapping from the full hub vocabulary
(``aqueduct/typehub.py``) to Spark's own DDL type-string spelling, the same
spelling Spark's ``_parse_datatype_string`` / ``F.col(...).cast(...)`` already
accept. Pure string logic — no ``pyspark`` import — so this module (and the
``ExecutorProtocol`` it is registered on, see ``spark/engine.py``) needs no
lazy-import discipline the way ``execute`` does.

Mostly identity: the hub's own canonical scalar spellings (``bigint``,
``int``, ``string``, ``binary``, ``date``, ``boolean``, ``tinyint``,
``smallint``, ``float``, ``double``) and its composite/parametrized
constructor syntax (``array<T>``, ``map<K,V>``, ``struct<name:type,...>``,
``decimal(p,s)``) already match Spark's DDL character-for-character — Spark's
own parser reads the hub's ``render()`` output unmodified for every one of
those. The ONE load-bearing exception is the timestamp pair: the hub's
``timestamp_tz``/``timestamp_ntz`` are not themselves valid Spark DDL tokens
(Spark spells the instant type plain ``timestamp`` and the naive type
``timestamp_ntz``) — see ``typehub.TimestampTz``/``TimestampNtz`` docstrings
for why the hub uses its own names instead of overloading Spark's `timestamp`
for both meanings.
"""

from __future__ import annotations

from aqueduct import typehub as hub


def render_spark_type(t: "hub.HubType | hub.NativeType") -> str:
    """Render one parsed hub type (or ``NativeType``) to Spark's DDL spelling.

    Registered as Spark's ``ExecutorProtocol.render_type``
    (``aqueduct/executor/spark/engine.py``) — call through
    ``aqueduct.executor.protocol.render_native_type("spark", spelling)``
    rather than this function directly; that seam also handles the
    native-namespace / parse-failure branches this function does not.
    """
    if isinstance(t, hub.NativeType):
        # A caller reaching this branch already confirmed t.engine == "spark"
        # (render_native_type's job); this function only ever sees a value
        # already destined for this engine.
        return t.spelling
    if isinstance(t, hub.TimestampTz):
        return "timestamp"
    if isinstance(t, hub.TimestampNtz):
        return "timestamp_ntz"
    if isinstance(t, hub.Decimal):
        return f"decimal({t.precision},{t.scale})"
    if isinstance(t, hub.Array):
        return f"array<{render_spark_type(t.element)}>"
    if isinstance(t, hub.Map):
        return f"map<{render_spark_type(t.key)},{render_spark_type(t.value)}>"
    if isinstance(t, hub.Struct):
        return "struct<" + ",".join(f"{f.name}:{render_spark_type(f.type)}" for f in t.fields) + ">"
    # Every remaining scalar's hub canonical spelling IS valid Spark DDL
    # verbatim (see module docstring) — no override table needed.
    return hub.render(t)


def normalize_type_spelling(spelling: str) -> str:
    """Render a raw Blueprint type spelling (Channel ``op: cast`` column type
    or UDF ``return_type``) to Spark's own DDL spelling, preserving the old
    alias-dict's raw-passthrough fallback for a spelling the hub does not
    recognize at all (Spark-only DDL the hub deliberately does not model —
    ``interval day to second``, ``variant``, a Java class-name-shaped
    ``return_type`` like ``"StringType()"``) — Spark's own parser remains the
    authority on whether ITS OWN native spelling is valid, same as the
    deleted dict's fallback behavior.

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
        return render_native_type("spark", stripped)
    except TypeSpellingError:
        return stripped


__all__ = ["render_spark_type", "normalize_type_spelling"]
