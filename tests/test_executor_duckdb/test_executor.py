"""DuckDB engine executor tests (Phase 78 Stage A).

Engine-side mechanics for the handlers Stage A implements: Ingress
(read_parquet/read_csv), Channel (sql/join/filter/select/deduplicate),
Junction (all three modes), Funnel (union_all/union), Egress (COPY TO
parquet/csv). Not a duplicate of the Spark suite — DuckDB-specific mechanics
(COPY TO, sqlglot transpile, relation lifecycle) tested here, engine-agnostic
contract behavior stays in the parametrized-over-engines suites where they
already exist.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aqueduct.executor.duckdb_.channel import ChannelError, execute_channel
from aqueduct.executor.duckdb_.egress import EgressError, _copy_options, write_egress
from aqueduct.executor.duckdb_.error_extraction import extract_duckdb_error
from aqueduct.executor.duckdb_.executor import ExecuteError, execute
from aqueduct.executor.duckdb_.funnel import FunnelError, execute_funnel
from aqueduct.executor.duckdb_.ingress import IngressError, read_ingress
from aqueduct.executor.duckdb_.junction import JunctionError, execute_junction
from aqueduct.executor.models import ExecutionStatus
from aqueduct.executor.protocol import get_protocol
from aqueduct.models import Edge, Manifest, Module

pytestmark = pytest.mark.duckdb


def _module(id_, type_, config, **kw):
    return Module(id=id_, type=type_, label=id_, config=config, **kw)


def _write_parquet(con, tmp_path, name, rows_sql):
    path = str(tmp_path / f"{name}.parquet")
    con.sql(f"COPY ({rows_sql}) TO '{path}' (FORMAT PARQUET)")
    return path


# ── Registration ─────────────────────────────────────────────────────────


def test_duckdb_registered_via_entry_point():
    proto = get_protocol("duckdb")
    assert proto.engine == "duckdb"
    assert proto.execute is not None
    assert proto.extract_error is not None
    assert proto.prompt_rules.persona


# ── Ingress ──────────────────────────────────────────────────────────────


def test_read_ingress_parquet(duckdb_con, tmp_path):
    path = _write_parquet(
        duckdb_con, tmp_path, "src", "SELECT 1 AS a, 'x' AS b UNION ALL SELECT 2, 'y'"
    )
    module = _module("ing", "Ingress", {"format": "parquet", "path": path})
    rel = read_ingress(module, duckdb_con)
    assert sorted(rel.columns) == ["a", "b"]
    assert rel.fetchall() and len(rel.fetchall()) == 2


def test_read_ingress_csv(duckdb_con, tmp_path):
    csv_path = tmp_path / "src.csv"
    csv_path.write_text("a,b\n1,x\n2,y\n")
    module = _module("ing", "Ingress", {"format": "csv", "path": str(csv_path)})
    rel = read_ingress(module, duckdb_con)
    assert sorted(rel.columns) == ["a", "b"]


def test_read_ingress_json(duckdb_con, tmp_path):
    json_path = tmp_path / "src.json"
    json_path.write_text('{"a": 1, "b": "x"}\n{"a": 2, "b": "y"}\n')
    module = _module("ing", "Ingress", {"format": "json", "path": str(json_path)})
    rel = read_ingress(module, duckdb_con)
    assert sorted(rel.columns) == ["a", "b"]
    assert len(rel.fetchall()) == 2


def test_read_ingress_unsupported_format_raises(duckdb_con):
    module = _module("ing", "Ingress", {"format": "jdbc", "path": "whatever"})
    with pytest.raises(IngressError, match="not implemented"):
        read_ingress(module, duckdb_con)


def test_read_ingress_schema_hint_mismatch(duckdb_con, tmp_path):
    path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT 1 AS a")
    module = _module(
        "ing",
        "Ingress",
        {"format": "parquet", "path": path, "schema_hint": {"a": "varchar"}},
    )
    with pytest.raises(IngressError, match="type mismatch"):
        read_ingress(module, duckdb_con)


# ── Phase 80 work package 3: schema_hint now understands the hub vocabulary ─
#
# `schema_hint` field types used to be normalised through `_TYPE_ALIASES`
# (deleted, 9-entry scalar-only dict) — a HUB spelling like `array<int>` or
# `timestamp_ntz` never matched it and never matched DuckDB's own
# `str(dtype)` representation either, so it always raised "type mismatch"
# regardless of whether the live column actually had that type. Both sides
# now render through the same hub-aware `_normalize_type`.
def test_read_ingress_schema_hint_hub_array_matches(duckdb_con, tmp_path):
    path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT [1,2,3] AS a")
    module = _module(
        "ing",
        "Ingress",
        {"format": "parquet", "path": path, "schema_hint": {"a": "array<int>"}},
    )
    rel = read_ingress(module, duckdb_con)
    assert rel.fetchall() == [([1, 2, 3],)]


def test_read_ingress_schema_hint_hub_decimal_matches(duckdb_con, tmp_path):
    path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT CAST(1.5 AS DECIMAL(10,2)) AS a")
    module = _module(
        "ing",
        "Ingress",
        {"format": "parquet", "path": path, "schema_hint": {"a": "decimal(10,2)"}},
    )
    rel = read_ingress(module, duckdb_con)  # no IngressError
    # The hint passed the mismatch check AND the live column is genuinely
    # DECIMAL(10,2) — not merely "didn't raise" (a hint check that always
    # passed would look identical without this).
    assert str(rel.types[rel.columns.index("a")]) == "DECIMAL(10,2)"
    assert rel.fetchall() == [(1.5,)]


def test_read_ingress_schema_hint_hub_timestamp_ntz_matches(duckdb_con, tmp_path):
    path = _write_parquet(
        duckdb_con, tmp_path, "src", "SELECT CAST('2020-01-01' AS TIMESTAMP) AS a"
    )
    module = _module(
        "ing",
        "Ingress",
        {"format": "parquet", "path": path, "schema_hint": {"a": "timestamp_ntz"}},
    )
    rel = read_ingress(module, duckdb_con)  # no IngressError
    assert str(rel.types[rel.columns.index("a")]) == "TIMESTAMP"
    import datetime

    assert rel.fetchall() == [(datetime.datetime(2020, 1, 1),)]


def test_read_ingress_schema_hint_hub_array_mismatch_still_raises(duckdb_con, tmp_path):
    """A genuine mismatch (int column, string-array hint) must still raise —
    hub-aware normalization must not make the check permissive."""
    path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT [1,2,3] AS a")
    module = _module(
        "ing",
        "Ingress",
        {"format": "parquet", "path": path, "schema_hint": {"a": "array<string>"}},
    )
    with pytest.raises(IngressError, match="type mismatch"):
        read_ingress(module, duckdb_con)


# ── Pass G2 — numeric-family widening ───────────────────────────────────────
#
# The real, measured defect: DuckDB's CSV sniffer always infers BIGINT for a
# whole-number column, regardless of value range, while Spark's own CSV
# inference picks IntegerType for small values — an author who writes
# `quantity: integer` (matching what Spark infers) previously got a
# `schema_hint type mismatch` on the DuckDB lane even though the column is
# genuinely integer-valued on both engines. 30_ingress_schema_hints hits this
# exact case.


def test_read_ingress_schema_hint_integer_widens_to_bigint_actual(duckdb_con, tmp_path):
    path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT CAST(150 AS BIGINT) AS quantity")
    module = _module(
        "ing",
        "Ingress",
        {"format": "parquet", "path": path, "schema_hint": {"quantity": "integer"}},
    )
    rel = read_ingress(module, duckdb_con)  # no IngressError
    assert str(rel.types[rel.columns.index("quantity")]) == "BIGINT"
    assert rel.fetchall() == [(150,)]


def test_read_ingress_schema_hint_bigint_hint_does_not_widen_from_integer_actual(
    duckdb_con, tmp_path
):
    """The reverse direction (hint WIDER than the actual column) must still
    raise — widening only ever accepts an actual type at least as wide as
    the hint, never the other way around."""
    path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT CAST(150 AS INTEGER) AS quantity")
    module = _module(
        "ing",
        "Ingress",
        {"format": "parquet", "path": path, "schema_hint": {"quantity": "bigint"}},
    )
    with pytest.raises(IngressError, match="type mismatch"):
        read_ingress(module, duckdb_con)


# ── Channel ──────────────────────────────────────────────────────────────


def test_channel_filter(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1),(2),(3)) t(a)")
    module = _module("ch", "Channel", {"op": "filter", "condition": "a > 1"})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert sorted(r[0] for r in out.fetchall()) == [2, 3]


def test_channel_select(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a, 2 AS b")
    module = _module("ch", "Channel", {"op": "select", "columns": ["a"]})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert out.columns == ["a"]


def test_channel_deduplicate_no_key(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1),(1),(2)) t(a)")
    module = _module("ch", "Channel", {"op": "deduplicate"})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert sorted(r[0] for r in out.fetchall()) == [1, 2]


def test_channel_deduplicate_with_key_and_order(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1, 10), (1, 20), (2, 5)) t(k, v)")
    module = _module("ch", "Channel", {"op": "deduplicate", "key": "k", "order_by": "v DESC"})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    rows = sorted(out.fetchall())
    assert rows == [(1, 20), (2, 5)]


def test_channel_sql_transpiled_from_spark_dialect(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("ch", "Channel", {"op": "sql", "query": "SELECT a + 1 AS b FROM up"})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert out.fetchall() == [(2,)]


def test_channel_join(duckdb_con):
    # Module IDs deliberately avoid SQL reserved words (e.g. "left"/"right",
    # which DuckDB reserves for JOIN syntax even in expression position) —
    # a real Blueprint author picks module IDs, and a reserved-word ID is an
    # edge case shared by any SQL-backed engine, not something Stage A needs
    # to paper over. See aqueduct/executor/duckdb_/channel.py::_build_join_query
    # for the (real, tested) quoting this engine does for its own generated
    # FROM/JOIN table references.
    customers = duckdb_con.sql("SELECT 1 AS id, 'a' AS name")
    orders = duckdb_con.sql("SELECT 1 AS id, 100 AS amount")
    module = _module(
        "ch",
        "Channel",
        {
            "op": "join",
            "left": "customers",
            "right": "orders",
            "join_type": "inner",
            "condition": "customers.id = orders.id",
        },
    )
    out = execute_channel(module, {"customers": customers, "orders": orders}, duckdb_con)
    assert out.fetchall() == [(1, "a", 1, 100)]


def test_channel_unsupported_op_is_honest_error(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("ch", "Channel", {"op": "repartition", "num_partitions": 4})
    with pytest.raises(ChannelError, match="not implemented"):
        execute_channel(module, {"up": rel}, duckdb_con)


def test_channel_cast(duckdb_con):
    rel = duckdb_con.sql("SELECT '1' AS a, 'x' AS b")
    module = _module("ch", "Channel", {"op": "cast", "columns": {"a": "int"}})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert out.columns == ["a", "b"]
    assert out.fetchall() == [(1, "x")]


# ── Phase 80 work package 2: type-leaf verdict->test links ────────────────
#
# Each case exercises the DuckDB engine's own cast capability behind one
# `type.<constructor>` capability leaf (capability_leaves.py::type_leaves()),
# through the real `channel.op.cast` handler (`_execute_cast` in
# duckdb_/channel.py) — a real cast through the engine, not a raw duckdb.sql
# smoke check. Target spellings here are DuckDB's OWN native DDL, written
# directly — proving the "spelling the hub doesn't recognize falls through to
# DuckDB's own parser raw" fallback (`normalize_type_spelling`'s
# TypeSpellingError branch, `aqueduct/executor/duckdb_/type_render.py`). The
# HUB-vocabulary spellings (`array<int>`, `decimal(10,2)`, `timestamp_ntz`,
# ...) are the separate `_HUB_VOCABULARY_CAST_CASES` block below — Phase 80
# work package 3 is what makes THOSE work end-to-end (they used to reach
# DuckDB's parser unmodified and fail; see typehub.py's module docstring for
# the `array<int>` example).
_HUB_TYPE_CAST_CASES = [
    ("type.boolean", "SELECT true AS a", "BOOLEAN"),
    ("type.tinyint", "SELECT 1 AS a", "TINYINT"),
    ("type.smallint", "SELECT 1 AS a", "SMALLINT"),
    ("type.int", "SELECT 1 AS a", "INTEGER"),
    ("type.bigint", "SELECT 1 AS a", "BIGINT"),
    ("type.float", "SELECT 1.0 AS a", "FLOAT"),
    ("type.double", "SELECT 1.0 AS a", "DOUBLE"),
    ("type.string", "SELECT 1 AS a", "VARCHAR"),
    ("type.binary", "SELECT 'a' AS a", "BLOB"),
    ("type.date", "SELECT '2020-01-01' AS a", "DATE"),
    ("type.decimal", "SELECT 1.5 AS a", "DECIMAL(10,2)"),
    ("type.timestamp_tz", "SELECT '2020-01-01' AS a", "TIMESTAMPTZ"),
    ("type.timestamp_ntz", "SELECT '2020-01-01' AS a", "TIMESTAMP"),
    ("type.array", "SELECT [1,2,3] AS a", "INTEGER[]"),
    ("type.map", "SELECT MAP([1],[2]) AS a", "MAP(INTEGER,INTEGER)"),
    ("type.struct", "SELECT {'x':1} AS a", "STRUCT(x INTEGER)"),
]


# DuckDB's own `str(DuckDBPyType)` rendering for a couple of these diverges
# textually from the CAST target spelling we wrote (same type, different
# self-description) — map the exceptions here rather than asserting a
# coincidental string match.
#
# Bare "TIMESTAMP" (the type.timestamp_ntz row's target spelling here) used
# to be hub-recognized as the ambiguous bare-timestamp spelling and silently
# resolved to timestamp_tz (a real bug: the row's own leaf name promises
# timestamp_NTZ but the live column came back TIMESTAMPTZ — see git history
# for the now-removed override this entry used to carry). Bare `timestamp`
# is a hard TypeSpellingError now (no deprecation window), which
# `normalize_type_spelling`'s TypeSpellingError-catches-and-falls-back-raw
# path (`aqueduct/executor/duckdb_/type_render.py`) turns into exactly the
# "hand this DuckDB-native DDL to DuckDB's own parser, unmodified" case every
# OTHER row in `_HUB_TYPE_CAST_CASES` already exercises — so "TIMESTAMP"
# needs no override: it now correctly produces a genuine naive TIMESTAMP
# column, matching the leaf it is meant to test.
_DUCKDB_TYPE_STR_OVERRIDES = {
    "TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE",
    "MAP(INTEGER,INTEGER)": "MAP(INTEGER, INTEGER)",
}


@pytest.mark.parametrize(
    "leaf,source_sql,target_type", _HUB_TYPE_CAST_CASES, ids=[c[0] for c in _HUB_TYPE_CAST_CASES]
)
def test_channel_cast_hub_type_constructors(duckdb_con, leaf, source_sql, target_type):
    rel = duckdb_con.sql(source_sql)
    module = _module("ch", "Channel", {"op": "cast", "columns": {"a": target_type}})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    out.fetchall()  # force real execution, not just relation-graph construction
    expected = _DUCKDB_TYPE_STR_OVERRIDES.get(target_type, target_type)
    assert str(out.types[out.columns.index("a")]) == expected


def test_channel_cast_native_namespace_duckdb(duckdb_con):
    """`type.native.duckdb` — the `duckdb:<spelling>` escape hatch is a real
    DuckDB-only type the hub vocabulary deliberately does not model
    (``HUGEINT``, a 128-bit integer with no Spark/Arrow equivalent in this
    vocabulary — see typehub.py's NativeType docstring example); proves the
    escape hatch is not merely accepted at parse time but a real, executable
    cast target on the engine it names."""
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("ch", "Channel", {"op": "cast", "columns": {"a": "HUGEINT"}})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert out.fetchall() == [(1,)]


# ── Phase 80 work package 3: hub vocabulary now WORKS end-to-end on DuckDB ──
#
# These are the exact "silently mishandled" cases work package 2's own test
# docstring called out: a HUB spelling (not DuckDB's native DDL) used as an
# op=cast target. Before this package, `array<int>` reached DuckDB's SQL
# parser completely unmodified and failed (DuckDB wants `INTEGER[]`) despite
# `type.array` already being declared `supported` — a declaration/behavior
# mismatch. `duckdb_/channel.py::_normalize_cast_type` now parses the
# spelling through the hub and renders it via
# `aqueduct.executor.duckdb_.type_render.render_duckdb_type` before the cast.
_HUB_VOCABULARY_CAST_CASES = [
    ("type.boolean", "SELECT true AS a", "boolean"),
    ("type.bigint", "SELECT 1 AS a", "bigint"),
    ("type.string", "SELECT 1 AS a", "string"),
    ("type.binary", "SELECT 'a' AS a", "binary"),
    ("type.decimal", "SELECT 1.5 AS a", "decimal(10,2)"),
    ("type.timestamp_tz", "SELECT '2020-01-01' AS a", "timestamp_tz"),
    ("type.timestamp_ntz", "SELECT '2020-01-01' AS a", "timestamp_ntz"),
    # Phase 81/82 — duration(unit) is integer-backed (typehub.Duration): no
    # DuckDB-native "duration" DDL exists to put in _HUB_TYPE_CAST_CASES
    # above, so this hub-vocabulary round-trip is the only real proof this
    # constructor casts on DuckDB at all.
    ("type.duration", "SELECT 1 AS a", "duration(us)"),
    ("type.array", "SELECT [1,2,3] AS a", "array<int>"),
    ("type.map", "SELECT MAP([1],[2]) AS a", "map<string,int>"),
    ("type.struct", "SELECT {'x':1} AS a", "struct<x:int>"),
    ("type.array_nested", "SELECT [MAP([1],[2])] AS a", "array<map<string,int>>"),
]


# Expected `str(DuckDBPyType)` for the resulting column after casting through
# each hub-vocabulary spelling — i.e. what render_duckdb_type (type_render.py)
# is documented to produce for that hub construct. Proves the hub spelling
# didn't just parse without error but resolved to the SAME native type the
# equivalent DuckDB-native-DDL case (_HUB_TYPE_CAST_CASES) produces.
_HUB_VOCABULARY_EXPECTED_TYPE_STR = {
    "boolean": "BOOLEAN",
    "bigint": "BIGINT",
    "string": "VARCHAR",
    "binary": "BLOB",
    "decimal(10,2)": "DECIMAL(10,2)",
    "timestamp_tz": "TIMESTAMP WITH TIME ZONE",
    "timestamp_ntz": "TIMESTAMP",
    "duration(us)": "BIGINT",
    "array<int>": "INTEGER[]",
    "map<string,int>": "MAP(VARCHAR, INTEGER)",
    "struct<x:int>": "STRUCT(x INTEGER)",
    "array<map<string,int>>": "MAP(VARCHAR, INTEGER)[]",
}


@pytest.mark.parametrize(
    "leaf,source_sql,target_type",
    _HUB_VOCABULARY_CAST_CASES,
    ids=[c[0] for c in _HUB_VOCABULARY_CAST_CASES],
)
def test_channel_cast_hub_vocabulary_spellings(duckdb_con, leaf, source_sql, target_type):
    rel = duckdb_con.sql(source_sql)
    module = _module("ch", "Channel", {"op": "cast", "columns": {"a": target_type}})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    out.fetchall()  # force real execution — proves the hub spelling is a real, valid CAST target
    assert str(out.types[out.columns.index("a")]) == _HUB_VOCABULARY_EXPECTED_TYPE_STR[target_type]


# ── Phase 80 work package 3: old _CAST_TYPE_ALIASES spellings still work ────
#
# `_CAST_TYPE_ALIASES` (the deleted 9-entry dict) mapped a small set of
# Spark-vocabulary aliases to DuckDB DDL. All nine are hub-recognized
# spellings too, so `render_native_type` reproduces the exact same mapping —
# this is the alias-dict-deletion regression the package's own report must
# demonstrate.
# target spelling -> the exact DuckDB DDL the deleted 9-entry
# `_CAST_TYPE_ALIASES` dict used to map it to (the regression this test
# guards against reproducing the mapping via render_native_type instead).
_DELETED_ALIAS_DICT_CASES = [
    ("string", "SELECT 1 AS a", "VARCHAR"),
    ("long", "SELECT 1 AS a", "BIGINT"),
    ("int", "SELECT 1 AS a", "INTEGER"),
    ("integer", "SELECT 1 AS a", "INTEGER"),
    ("short", "SELECT 1 AS a", "SMALLINT"),
    ("byte", "SELECT 1 AS a", "TINYINT"),
    ("bool", "SELECT 1 AS a", "BOOLEAN"),
    ("double", "SELECT 1.0 AS a", "DOUBLE"),
    ("float", "SELECT 1.0 AS a", "FLOAT"),
]


@pytest.mark.parametrize(
    "target_type,source_sql,expected_type_str",
    _DELETED_ALIAS_DICT_CASES,
    ids=[c[0] for c in _DELETED_ALIAS_DICT_CASES],
)
def test_channel_cast_deleted_alias_dict_spellings_still_work(
    duckdb_con, target_type, source_sql, expected_type_str
):
    rel = duckdb_con.sql(source_sql)
    module = _module("ch", "Channel", {"op": "cast", "columns": {"a": target_type}})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    out.fetchall()
    assert str(out.types[out.columns.index("a")]) == expected_type_str


def test_channel_cast_explicit_native_namespace_duckdb_passthrough(duckdb_con):
    """The EXPLICIT `duckdb:<spelling>` native-namespace syntax (distinct from
    the bare-native-DDL fallback `test_channel_cast_native_namespace_duckdb`
    above exercises) renders through `render_native_type`'s NativeType
    same-engine branch: `.spelling` verbatim, unmapped."""
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("ch", "Channel", {"op": "cast", "columns": {"a": "duckdb:HUGEINT"}})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert out.fetchall() == [(1,)]


def test_channel_cast_foreign_native_namespace_is_a_defensive_error(duckdb_con):
    """A `spark:<spelling>` cast target reaching the DuckDB engine directly
    (an ungated call — the real compile-time `type.native.spark` gate on the
    duckdb engine is `unsupported`, see capabilities.yml) must fail loudly,
    not silently forward Spark's native spelling to DuckDB's parser."""
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("ch", "Channel", {"op": "cast", "columns": {"a": "spark:variant"}})
    with pytest.raises(ChannelError, match="DIFFERENT engine"):
        execute_channel(module, {"up": rel}, duckdb_con)


def test_channel_rename(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a, 2 AS b")
    module = _module("ch", "Channel", {"op": "rename", "columns": {"a": "id"}})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert out.columns == ["id", "b"]
    assert out.fetchall() == [(1, 2)]


def test_channel_sort(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (3),(1),(2)) t(a)")
    module = _module("ch", "Channel", {"op": "sort", "order_by": "a DESC"})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert [r[0] for r in out.fetchall()] == [3, 2, 1]


def test_channel_union(duckdb_con):
    a = duckdb_con.sql("SELECT 1 AS x")
    b = duckdb_con.sql("SELECT 2 AS x")
    module = _module("ch", "Channel", {"op": "union"})
    out = execute_channel(module, {"a": a, "b": b}, duckdb_con)
    assert sorted(r[0] for r in out.fetchall()) == [1, 2]


# ── Junction ─────────────────────────────────────────────────────────────


def test_junction_conditional(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1),(2),(3)) t(a)")
    module = _module(
        "j",
        "Junction",
        {
            "mode": "conditional",
            "branches": [
                {"id": "hi", "condition": "a > 1"},
                {"id": "lo", "condition": "_else_"},
            ],
        },
    )
    branches = execute_junction(module, rel)
    assert sorted(r[0] for r in branches["hi"].fetchall()) == [2, 3]
    assert sorted(r[0] for r in branches["lo"].fetchall()) == [1]


def test_junction_broadcast(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("j", "Junction", {"mode": "broadcast", "branches": [{"id": "x"}, {"id": "y"}]})
    branches = execute_junction(module, rel)
    assert branches["x"].fetchall() == branches["y"].fetchall() == [(1,)]


def test_junction_partition(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES ('EU'),('US'),('EU')) t(region)")
    module = _module(
        "j",
        "Junction",
        {
            "mode": "partition",
            "partition_key": "region",
            "branches": [{"id": "eu", "value": "EU"}, {"id": "us", "value": "US"}],
        },
    )
    branches = execute_junction(module, rel)
    assert len(branches["eu"].fetchall()) == 2
    assert len(branches["us"].fetchall()) == 1


def test_junction_unknown_mode_raises(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("j", "Junction", {"mode": "bogus", "branches": []})
    with pytest.raises(JunctionError):
        execute_junction(module, rel)


# ── Funnel ───────────────────────────────────────────────────────────────


def test_funnel_union_all(duckdb_con):
    a = duckdb_con.sql("SELECT 1 AS x")
    b = duckdb_con.sql("SELECT 2 AS x")
    module = _module("f", "Funnel", {"mode": "union_all", "inputs": ["a", "b"]})
    out = execute_funnel(module, {"a": a, "b": b}, duckdb_con)
    assert sorted(r[0] for r in out.fetchall()) == [1, 2]


def test_funnel_union_dedupes(duckdb_con):
    a = duckdb_con.sql("SELECT 1 AS x")
    b = duckdb_con.sql("SELECT 1 AS x")
    module = _module("f", "Funnel", {"mode": "union", "inputs": ["a", "b"]})
    out = execute_funnel(module, {"a": a, "b": b}, duckdb_con)
    assert out.fetchall() == [(1,)]


def test_funnel_unsupported_mode_is_honest_error(duckdb_con):
    a = duckdb_con.sql("SELECT 1 AS x")
    b = duckdb_con.sql("SELECT 2 AS x")
    module = _module("f", "Funnel", {"mode": "bogus", "inputs": ["a", "b"]})
    with pytest.raises(FunnelError, match="not implemented"):
        execute_funnel(module, {"a": a, "b": b}, duckdb_con)


def test_funnel_zip_row_aligned_join(duckdb_con):
    a = duckdb_con.sql("SELECT * FROM (VALUES (1,'x'),(2,'y')) t(id, name)")
    b = duckdb_con.sql("SELECT * FROM (VALUES (10),(20)) t(amount)")
    module = _module("f", "Funnel", {"mode": "zip", "inputs": ["a", "b"]})
    out = execute_funnel(module, {"a": a, "b": b}, duckdb_con)
    assert sorted(out.columns) == sorted(["id", "name", "amount"])
    rows = {r[0]: r for r in out.fetchall()}
    assert len(rows) == 2


def test_funnel_coalesce_folds_overlapping_columns(duckdb_con):
    a = duckdb_con.sql("SELECT * FROM (VALUES (1, NULL), (2, 5)) t(id, val)")
    b = duckdb_con.sql("SELECT * FROM (VALUES (100), (200)) t(val)")
    module = _module("f", "Funnel", {"mode": "coalesce", "inputs": ["a", "b"]})
    out = execute_funnel(module, {"a": a, "b": b}, duckdb_con)
    assert sorted(out.columns) == ["id", "val"]
    rows = sorted(out.fetchall())
    # first row: a.val is NULL -> falls back to b.val (100); second: a.val=5 wins
    assert rows == [(1, 100), (2, 5)]


def test_funnel_zip_duplicate_column_names_rejected(duckdb_con):
    a = duckdb_con.sql("SELECT 1 AS x")
    b = duckdb_con.sql("SELECT 2 AS x")
    module = _module("f", "Funnel", {"mode": "zip", "inputs": ["a", "b"]})
    with pytest.raises(FunnelError, match="unique column names"):
        execute_funnel(module, {"a": a, "b": b}, duckdb_con)


# ── Egress ───────────────────────────────────────────────────────────────


def test_write_egress_parquet_overwrite(duckdb_con, tmp_path):
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    module = _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"})
    write_egress(rel, module, duckdb_con)
    assert duckdb_con.read_parquet(out_path).fetchall() == [(1,)]


def test_write_egress_json_overwrite(duckdb_con, tmp_path):
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = str(tmp_path / "out.json")
    module = _module("eg", "Egress", {"format": "json", "path": out_path, "mode": "overwrite"})
    write_egress(rel, module, duckdb_con)
    assert duckdb_con.read_json(out_path).fetchall() == [(1,)]


def test_write_egress_json_append_to_existing_target_unions_rows(duckdb_con, tmp_path):
    """Regression (F-9): the append branch's reader used to be picked with
    `"read_parquet" if fmt == "parquet" else "read_csv"` — a JSON append
    would silently read the existing JSON file back AS CSV, a wrong-answer
    bug rather than a crash. Assert the union actually contains both rows,
    which only happens if the existing file is read back as JSON."""
    out_path = str(tmp_path / "out.json")
    first = duckdb_con.sql("SELECT 1 AS a")
    write_egress(
        first,
        _module("eg", "Egress", {"format": "json", "path": out_path, "mode": "overwrite"}),
        duckdb_con,
    )

    second = duckdb_con.sql("SELECT 2 AS a")
    append_module = _module("eg", "Egress", {"format": "json", "path": out_path, "mode": "append"})
    write_egress(second, append_module, duckdb_con)

    assert sorted(r[0] for r in duckdb_con.read_json(out_path).fetchall()) == [1, 2]


def test_write_egress_csv_explicit_header_option_not_duplicated(duckdb_con, tmp_path):
    """Regression (gallery snippet 11_spillway_channel): a csv Egress with
    BOTH the default `header:` (implicit True) and an explicit
    `options: {header: ...}` must not emit `HEADER` twice in the COPY
    statement — DuckDB rejects a duplicate option name outright."""
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = str(tmp_path / "out.csv")
    module = _module(
        "eg",
        "Egress",
        {"format": "csv", "path": out_path, "mode": "overwrite", "options": {"header": "true"}},
    )
    write_egress(rel, module, duckdb_con)  # must not raise a duplicate-option Parser Error
    assert duckdb_con.read_csv(out_path).fetchall() == [(1,)]


def test_write_egress_error_mode_existing_target_raises(duckdb_con, tmp_path):
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = tmp_path / "out.parquet"
    out_path.write_bytes(b"not-really-parquet")
    module = _module("eg", "Egress", {"format": "parquet", "path": str(out_path), "mode": "error"})
    with pytest.raises(EgressError, match="already exists"):
        write_egress(rel, module, duckdb_con)


def test_write_egress_errorifexists_mode_existing_target_raises(duckdb_con, tmp_path):
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = tmp_path / "out.parquet"
    out_path.write_bytes(b"not-really-parquet")
    module = _module(
        "eg", "Egress", {"format": "parquet", "path": str(out_path), "mode": "errorifexists"}
    )
    with pytest.raises(EgressError, match="already exists"):
        write_egress(rel, module, duckdb_con)


def test_write_egress_ignore_mode_existing_target_skips_write(duckdb_con, tmp_path):
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = tmp_path / "out.parquet"
    out_path.write_bytes(b"original-bytes")
    module = _module("eg", "Egress", {"format": "parquet", "path": str(out_path), "mode": "ignore"})
    write_egress(rel, module, duckdb_con)  # must not raise, must not touch the file
    assert out_path.read_bytes() == b"original-bytes"


def test_write_egress_append_to_new_target_writes_once(duckdb_con, tmp_path):
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    module = _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "append"})
    write_egress(rel, module, duckdb_con)
    assert duckdb_con.read_parquet(out_path).fetchall() == [(1,)]


def test_copy_options_coalesce_adds_per_thread_output_false(duckdb_con):
    """Regression (Pass G1): `coalesce` was a documented, capability-declared
    `supported` Egress field with zero readers anywhere on this engine
    either. `coalesce` truthy (no partition_by) must now explicitly pin
    single-file output via PER_THREAD_OUTPUT false rather than relying on
    DuckDB's undocumented default."""
    opts = _copy_options("parquet", {"coalesce": True}, partition_by=None)
    assert "PER_THREAD_OUTPUT false" in opts


def test_copy_options_coalesce_falsy_omits_per_thread_output(duckdb_con):
    opts = _copy_options("parquet", {"coalesce": False}, partition_by=None)
    assert "PER_THREAD_OUTPUT" not in opts
    opts_unset = _copy_options("parquet", {}, partition_by=None)
    assert "PER_THREAD_OUTPUT" not in opts_unset


def test_copy_options_coalesce_not_duplicated_with_explicit_option(duckdb_con):
    """Same dedup convention as `header:` — user-supplied `options:` wins,
    checked case-insensitively, so COPY never receives PER_THREAD_OUTPUT
    twice."""
    opts = _copy_options(
        "parquet",
        {"coalesce": True, "options": {"per_thread_output": "true"}},
        partition_by=None,
    )
    assert opts.count("PER_THREAD_OUTPUT") == 1
    assert "PER_THREAD_OUTPUT 'true'" in opts


def test_copy_options_coalesce_inert_when_partition_by_set(duckdb_con):
    """PER_THREAD_OUTPUT cannot combine with PARTITION_BY on this engine
    (raises NotImplementedException) — coalesce must not add it in that
    branch; DuckDB already writes one file per partition value by default."""
    opts = _copy_options("parquet", {"coalesce": True}, partition_by=["grp"])
    assert "PER_THREAD_OUTPUT" not in opts
    assert "PARTITION_BY (grp)" in opts


def test_write_egress_coalesce_true_writes_single_file_correctly(duckdb_con, tmp_path):
    """End-to-end: coalesce: true still writes correct data as a single
    file (real behavior, not just an option-string unit check)."""
    rel = duckdb_con.sql("SELECT * FROM range(20) t(a)")
    out_path = str(tmp_path / "coalesced.parquet")
    module = _module(
        "eg",
        "Egress",
        {"format": "parquet", "path": out_path, "mode": "overwrite", "coalesce": True},
    )
    write_egress(rel, module, duckdb_con)
    assert duckdb_con.read_parquet(out_path).aggregate("COUNT(*) AS c").fetchone()[0] == 20
    assert Path(out_path).is_file()


def test_write_egress_repartition_has_no_effect_on_duckdb(duckdb_con, tmp_path):
    """`repartition` stays honestly unsupported on this engine (no
    shuffle/partition-count lever exists) — setting it must not raise and
    must not change the written data."""
    rel = duckdb_con.sql("SELECT * FROM range(10) t(a)")
    out_path = str(tmp_path / "repart.parquet")
    module = _module(
        "eg",
        "Egress",
        {"format": "parquet", "path": out_path, "mode": "overwrite", "repartition": 4},
    )
    write_egress(rel, module, duckdb_con)
    assert duckdb_con.read_parquet(out_path).aggregate("COUNT(*) AS c").fetchone()[0] == 10


def test_write_egress_append_to_existing_target_unions_rows(duckdb_con, tmp_path):
    out_path = str(tmp_path / "out.parquet")
    first = duckdb_con.sql("SELECT 1 AS a")
    module = _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"})
    write_egress(first, module, duckdb_con)

    second = duckdb_con.sql("SELECT 2 AS a")
    append_module = _module(
        "eg", "Egress", {"format": "parquet", "path": out_path, "mode": "append"}
    )
    write_egress(second, append_module, duckdb_con)

    assert sorted(r[0] for r in duckdb_con.read_parquet(out_path).fetchall()) == [1, 2]


# ── Egress: on_new_columns (Pass F) ─────────────────────────────────────────


def test_write_egress_on_new_columns_fail_raises_when_new_column_added(duckdb_con, tmp_path):
    out_path = str(tmp_path / "out.parquet")
    first = duckdb_con.sql("SELECT 1 AS a")
    write_egress(
        first,
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
        duckdb_con,
    )

    second = duckdb_con.sql("SELECT 2 AS a, 'x' AS b")
    module = _module(
        "eg",
        "Egress",
        {
            "format": "parquet",
            "path": out_path,
            "mode": "overwrite",
            "on_new_columns": "fail",
        },
    )
    with pytest.raises(EgressError, match="on_new_columns=fail"):
        write_egress(second, module, duckdb_con)
    # Prevention semantics: the original file must be untouched after the raise.
    assert duckdb_con.read_parquet(out_path).fetchall() == [(1,)]


def test_write_egress_on_new_columns_allow_absorbs_silently(duckdb_con, tmp_path):
    out_path = str(tmp_path / "out.parquet")
    first = duckdb_con.sql("SELECT 1 AS a")
    write_egress(
        first,
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
        duckdb_con,
    )

    second = duckdb_con.sql("SELECT 2 AS a, 'x' AS b")
    module = _module(
        "eg",
        "Egress",
        {
            "format": "parquet",
            "path": out_path,
            "mode": "overwrite",
            "on_new_columns": "allow",
        },
    )
    write_egress(second, module, duckdb_con)  # must not raise
    assert duckdb_con.read_parquet(out_path).fetchall() == [(2, "x")]


def test_write_egress_on_new_columns_alert_warns_and_absorbs(duckdb_con, tmp_path, caplog):
    out_path = str(tmp_path / "out.parquet")
    first = duckdb_con.sql("SELECT 1 AS a")
    write_egress(
        first,
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
        duckdb_con,
    )

    second = duckdb_con.sql("SELECT 2 AS a, 'x' AS b")
    module = _module(
        "eg",
        "Egress",
        {
            "format": "parquet",
            "path": out_path,
            "mode": "overwrite",
            "on_new_columns": "alert",
        },
    )
    with caplog.at_level("WARNING"):
        write_egress(second, module, duckdb_con)
    assert any("runtime_egress_new_columns" in r.message for r in caplog.records)
    assert duckdb_con.read_parquet(out_path).fetchall() == [(2, "x")]


def test_write_egress_on_new_columns_no_new_columns_is_noop(duckdb_con, tmp_path):
    out_path = str(tmp_path / "out.parquet")
    first = duckdb_con.sql("SELECT 1 AS a")
    write_egress(
        first,
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
        duckdb_con,
    )

    second = duckdb_con.sql("SELECT 2 AS a")  # same columns
    module = _module(
        "eg",
        "Egress",
        {
            "format": "parquet",
            "path": out_path,
            "mode": "overwrite",
            "on_new_columns": "fail",
        },
    )
    write_egress(second, module, duckdb_con)  # must not raise — no drift
    assert duckdb_con.read_parquet(out_path).fetchall() == [(2,)]


def test_write_egress_on_new_columns_first_write_is_noop(duckdb_con, tmp_path):
    """No existing target — nothing to drift against, same as Spark's version."""
    out_path = str(tmp_path / "out.parquet")
    rel = duckdb_con.sql("SELECT 1 AS a, 'x' AS b")
    module = _module(
        "eg",
        "Egress",
        {
            "format": "parquet",
            "path": out_path,
            "mode": "overwrite",
            "on_new_columns": "fail",
        },
    )
    write_egress(rel, module, duckdb_con)  # must not raise
    assert duckdb_con.read_parquet(out_path).fetchall() == [(1, "x")]


# ── Egress: format=depot (Pass E item 1) ────────────────────────────────────
# Mirrors tests/test_executor/test_executor_egress.py's depot coverage exactly
# — same MockDepot shape, same four cases — proving the DuckDB dispatch branch
# behaves identically to Spark's: a plain depot.put(key, value) Python call,
# never routed through DuckDB's own relation/SQL layer.


class MockDepot:
    def __init__(self):
        self.puts = {}

    def put(self, key, value):
        self.puts[key] = value


def test_write_egress_format_depot_no_depot(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("eg", "Egress", {"format": "depot", "key": "k1", "value": "v1"})
    with pytest.raises(EgressError, match="no DepotStore is wired"):
        write_egress(rel, module, duckdb_con, depot=None)


def test_write_egress_format_depot_missing_key(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("eg", "Egress", {"format": "depot", "value": "v1"})
    with pytest.raises(EgressError, match="requires 'key'"):
        write_egress(rel, module, duckdb_con, depot=MockDepot())


def test_write_egress_format_depot_value(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")
    depot = MockDepot()
    module = _module("eg", "Egress", {"format": "depot", "key": "k1", "value": "v1"})
    write_egress(rel, module, duckdb_con, depot=depot)
    assert depot.puts["k1"] == "v1"


def test_write_egress_format_depot_value_expr(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (0), (1), (2), (3), (4)) AS t(id)")
    depot = MockDepot()
    module = _module("eg", "Egress", {"format": "depot", "key": "k1", "value_expr": "max(id)"})
    write_egress(rel, module, duckdb_con, depot=depot)
    assert depot.puts["k1"] == "4"


# ── Error extraction ─────────────────────────────────────────────────────


def test_extract_duckdb_error_binder_exception(duckdb_con):
    duckdb_con.execute("CREATE TABLE t(a INT)")
    try:
        duckdb_con.execute("SELECT b FROM t")
    except Exception as exc:
        fields = extract_duckdb_error(exc)
    assert fields is not None
    assert fields["error_class"] == "BinderException"
    assert fields["object_name"] == "b"
    assert "a" in fields["suggested_columns"]


def test_extract_duckdb_error_none_input_returns_none():
    assert extract_duckdb_error(None) is None


# ── Full pipeline (Ingress -> Channel -> Egress) ────────────────────────


def test_full_pipeline_ingress_channel_egress(duckdb_con, tmp_path):
    src_path = _write_parquet(
        duckdb_con, tmp_path, "src", "SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) t(id, name)"
    )
    out_path = str(tmp_path / "out.parquet")

    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module("ch", "Channel", {"op": "filter", "condition": "id > 1"}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="ch", port="main"),
        Edge(from_id="ch", to_id="eg", port="main"),
    )
    manifest = Manifest(
        blueprint_id="test_bp",
        context={},
        modules=modules,
        edges=edges,
        engine_config={},
    )

    result = execute(manifest, duckdb_con, run_id="r1")
    assert result.status == ExecutionStatus.SUCCESS
    assert {r.module_id: r.status for r in result.module_results} == {
        "ing": "success",
        "ch": "success",
        "eg": "success",
    }
    assert sorted(r[0] for r in duckdb_con.read_parquet(out_path).fetchall()) == [2, 3]


def test_unsupported_module_type_raises_execute_error(duckdb_con):
    # Pass F: Probe is now dispatched (module.type.Probe: supported — see
    # duckdb_/probe.py), so it no longer serves as the "unsupported type"
    # example here. Arcade is the one authorable module type genuinely never
    # dispatched by this executor — it is compiled away into flat modules by
    # aqueduct/compiler/expander.py before a real Manifest ever reaches
    # execute(); constructing one directly (bypassing that expansion, as this
    # unit test does) is exactly the "module type this executor cannot run"
    # case _SUPPORTED_TYPES exists to catch as defense in depth.
    modules = (_module("a", "Arcade", {}),)
    manifest = Manifest(blueprint_id="bp", context={}, modules=modules, edges=(), engine_config={})
    with pytest.raises(ExecuteError, match="not supported"):
        execute(manifest, duckdb_con, run_id="r2")


# ── module.type.{Junction,Funnel,Regulator} driven through execute() ──────
# (B2 — these were already `supported` in capabilities.yml; proving the
# WHOLE module type, not just its handler function, actually runs through
# the executor's dispatch loop.)


def test_module_type_junction_driven_through_execute(duckdb_con, tmp_path):
    src_path = _write_parquet(
        duckdb_con, tmp_path, "src", "SELECT * FROM (VALUES (1),(2),(3)) t(a)"
    )
    out_hi = str(tmp_path / "hi.parquet")
    out_lo = str(tmp_path / "lo.parquet")
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module(
            "j",
            "Junction",
            {
                "mode": "conditional",
                "branches": [
                    {"id": "hi", "condition": "a > 1"},
                    {"id": "lo", "condition": "_else_"},
                ],
            },
        ),
        _module("eg_hi", "Egress", {"format": "parquet", "path": out_hi, "mode": "overwrite"}),
        _module("eg_lo", "Egress", {"format": "parquet", "path": out_lo, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="j", port="main"),
        Edge(from_id="j", to_id="eg_hi", port="hi"),
        Edge(from_id="j", to_id="eg_lo", port="lo"),
    )
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )
    result = execute(manifest, duckdb_con, run_id="r_junction")
    assert result.status == ExecutionStatus.SUCCESS
    assert sorted(r[0] for r in duckdb_con.read_parquet(out_hi).fetchall()) == [2, 3]
    assert sorted(r[0] for r in duckdb_con.read_parquet(out_lo).fetchall()) == [1]


def test_module_type_funnel_driven_through_execute(duckdb_con, tmp_path):
    src_a = _write_parquet(duckdb_con, tmp_path, "a", "SELECT 1 AS x")
    src_b = _write_parquet(duckdb_con, tmp_path, "b", "SELECT 2 AS x")
    out_path = str(tmp_path / "out.parquet")
    modules = (
        _module("ing_a", "Ingress", {"format": "parquet", "path": src_a}),
        _module("ing_b", "Ingress", {"format": "parquet", "path": src_b}),
        _module("f", "Funnel", {"mode": "union_all", "inputs": ["ing_a", "ing_b"]}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing_a", to_id="f", port="main"),
        Edge(from_id="ing_b", to_id="f", port="main"),
        Edge(from_id="f", to_id="eg", port="main"),
    )
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )
    result = execute(manifest, duckdb_con, run_id="r_funnel")
    assert result.status == ExecutionStatus.SUCCESS
    assert sorted(r[0] for r in duckdb_con.read_parquet(out_path).fetchall()) == [1, 2]


def test_execute_drops_channel_and_funnel_temp_tables(duckdb_con, tmp_path):
    """Channel `op: sql` and Funnel both materialize into uniquely-named
    ``CREATE TEMP TABLE``s (``__aq_ch_*`` / ``__aq_fn_*`` — see the
    docstrings in ``duckdb_/channel.py::_run_sql`` and
    ``duckdb_/funnel.py``'s union/coalesce/zip path for why materializing is
    required). Those tables used to never be dropped, so a long multi-module
    run accumulated one per Channel/Funnel execution on the connection for
    the run's whole lifetime. A single ``execute()`` run through a
    Channel(sql) -> Funnel(union_all) -> Egress pipeline must leave no
    ``__aq_ch_``/``__aq_fn_`` temp table behind on the connection once it
    returns.
    """
    src_a = _write_parquet(duckdb_con, tmp_path, "a", "SELECT 1 AS x")
    src_b = _write_parquet(duckdb_con, tmp_path, "b", "SELECT 2 AS x")
    out_path = str(tmp_path / "out.parquet")
    modules = (
        _module("ing_a", "Ingress", {"format": "parquet", "path": src_a}),
        _module("ing_b", "Ingress", {"format": "parquet", "path": src_b}),
        _module("ch", "Channel", {"op": "sql", "query": "SELECT x + 10 AS x FROM ing_a"}),
        _module("fn", "Funnel", {"mode": "union_all", "inputs": ["ch", "ing_b"]}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing_a", to_id="ch", port="main"),
        Edge(from_id="ch", to_id="fn", port="main"),
        Edge(from_id="ing_b", to_id="fn", port="main"),
        Edge(from_id="fn", to_id="eg", port="main"),
    )
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )
    result = execute(manifest, duckdb_con, run_id="r_temp_cleanup")
    assert result.status == ExecutionStatus.SUCCESS
    assert sorted(r[0] for r in duckdb_con.read_parquet(out_path).fetchall()) == [2, 11]

    rows = duckdb_con.execute(
        "SELECT table_name FROM information_schema.tables " "WHERE table_type = 'LOCAL TEMPORARY'"
    ).fetchall()
    leftover = [name for (name,) in rows if name.startswith(("__aq_ch_", "__aq_fn_"))]
    assert leftover == []


def test_module_type_regulator_driven_through_execute_gate_open(duckdb_con, tmp_path):
    """No surveyor supplied -> gate defaults open, Regulator passes data through."""
    src_path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module("reg", "Regulator", {}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="reg", port="main"),
        Edge(from_id="reg", to_id="eg", port="main"),
    )
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )
    result = execute(manifest, duckdb_con, run_id="r_reg_open")
    assert result.status == ExecutionStatus.SUCCESS
    assert {r.module_id: r.status for r in result.module_results}["reg"] == "success"
    assert duckdb_con.read_parquet(out_path).fetchall() == [(1,)]


def test_module_type_regulator_driven_through_execute_gate_closed_skips(duckdb_con, tmp_path):
    """A closed gate (on_block=skip, the default) marks the Regulator SKIPPED
    and gates downstream Egress off without failing the run."""
    from unittest.mock import MagicMock

    src_path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module("reg", "Regulator", {}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="reg", port="main"),
        Edge(from_id="reg", to_id="eg", port="main"),
    )
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )
    surveyor = MagicMock()
    surveyor.evaluate_regulator.return_value = False
    result = execute(manifest, duckdb_con, run_id="r_reg_closed", surveyor=surveyor)
    assert result.status == ExecutionStatus.SUCCESS
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["reg"] == "skipped"
    assert statuses["eg"] == "skipped"
    assert not Path(out_path).exists()


# ── feature.spillway / feature.checkpoint driven through execute() ────────


def test_feature_spillway_driven_through_execute(duckdb_con, tmp_path):
    src_path = _write_parquet(
        duckdb_con, tmp_path, "src", "SELECT * FROM (VALUES (1),(-1),(2)) t(a)"
    )
    main_out = str(tmp_path / "main.parquet")
    spill_out = str(tmp_path / "spill.parquet")
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module(
            "ch", "Channel", {"op": "filter", "condition": "1=1", "spillway_condition": "a < 0"}
        ),
        _module("eg_main", "Egress", {"format": "parquet", "path": main_out, "mode": "overwrite"}),
        _module(
            "eg_spill", "Egress", {"format": "parquet", "path": spill_out, "mode": "overwrite"}
        ),
    )
    edges = (
        Edge(from_id="ing", to_id="ch", port="main"),
        Edge(from_id="ch", to_id="eg_main", port="main"),
        Edge(from_id="ch", to_id="eg_spill", port="spillway"),
    )
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )
    result = execute(manifest, duckdb_con, run_id="r_spillway")
    assert result.status == ExecutionStatus.SUCCESS
    assert sorted(r[0] for r in duckdb_con.read_parquet(main_out).fetchall()) == [1, 2]
    assert sorted(r[0] for r in duckdb_con.read_parquet(spill_out).fetchall()) == [-1]


def test_typed_spillway_edge_from_channel_does_not_raise_binder_error(duckdb_con, tmp_path):
    """A typed spillway edge (``error_types``) filters on ``_aq_error_type``
    (executor.py's Egress/Funnel branches). Before this fix, a Channel's
    spillway_condition branch produced a relation with NO error columns at
    all (unlike Spark's, which stamps 4 via withColumn), so a typed edge
    fed from a Channel hit a DuckDB Binder error referencing a column that
    never existed — a blueprint that runs fine on Spark. This proves the
    DuckDB Channel spillway branch now stamps the same columns and the
    typed filter resolves them without error, keeping only the
    matching-type rows."""
    src_path = _write_parquet(
        duckdb_con, tmp_path, "src", "SELECT * FROM (VALUES (1),(-1),(2)) t(a)"
    )
    spill_out = str(tmp_path / "spill.parquet")
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module(
            "ch", "Channel", {"op": "filter", "condition": "1=1", "spillway_condition": "a < 0"}
        ),
        _module(
            "eg_spill", "Egress", {"format": "parquet", "path": spill_out, "mode": "overwrite"}
        ),
    )
    edges = (
        Edge(from_id="ing", to_id="ch", port="main"),
        Edge(
            from_id="ch",
            to_id="eg_spill",
            port="spillway",
            error_types=("SpillwayCondition",),
        ),
    )
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )
    result = execute(manifest, duckdb_con, run_id="r_typed_spillway")
    assert result.status == ExecutionStatus.SUCCESS
    assert sorted(r[0] for r in duckdb_con.read_parquet(spill_out).fetchall()) == [-1]


def test_feature_checkpoint_driven_through_execute(duckdb_con, tmp_path):
    src_path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    checkpoint_root = tmp_path / "checkpoints"
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}, checkpoint=True),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (Edge(from_id="ing", to_id="eg", port="main"),)
    manifest = Manifest(
        blueprint_id="bp",
        context={},
        modules=modules,
        edges=edges,
        engine_config={},
        checkpoint=True,
    )

    result = execute(manifest, duckdb_con, run_id="r_ckpt", checkpoint_root=checkpoint_root)
    assert result.status == ExecutionStatus.SUCCESS
    done_marker = checkpoint_root / "r_ckpt" / "ing" / "_aq_done"
    assert done_marker.exists()
    data_ckpt = checkpoint_root / "r_ckpt" / "ing" / "data" / "part-0.parquet"
    assert data_ckpt.exists()

    # Resume: Ingress reloads from the checkpoint file instead of re-reading src.
    resumed = execute(
        manifest,
        duckdb_con,
        run_id="r_ckpt_2",
        checkpoint_root=checkpoint_root,
        resume_run_id="r_ckpt",
    )
    assert resumed.status == ExecutionStatus.SUCCESS
    assert {r.module_id: r.status for r in resumed.module_results}["ing"] == "success"
    assert duckdb_con.read_parquet(out_path).fetchall() == [(1,)]


def test_resume_mismatched_manifest_warns_and_continues(duckdb_con, tmp_path):
    """DuckDB writes ``_manifest_hash`` into the checkpoint dir on every
    checkpointed run but, before this fix, never read it back on
    ``--resume`` — a Manifest that had changed since the checkpointed run
    was silently reused with no signal at all (unlike Spark, which at
    least warns). Tampering with the stored hash and resuming must now
    surface a suppressible ``runtime_resume_hash_changed`` AqueductWarning
    while still completing the run (permissive-resume semantics, matching
    Spark: warn, then proceed — never refuse).

    Pre-fix, this test failed: no read of ``_manifest_hash`` existed at
    all on the resume path, so ``execute()`` raised no warning of any kind
    and ``caught`` was empty.
    """
    import warnings as _w

    src_path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    checkpoint_root = tmp_path / "checkpoints"
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}, checkpoint=True),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (Edge(from_id="ing", to_id="eg", port="main"),)
    manifest = Manifest(
        blueprint_id="test.hash_mismatch",
        context={},
        modules=modules,
        edges=edges,
        engine_config={},
        checkpoint=True,
    )
    r1 = execute(manifest, duckdb_con, run_id="r_hash1", checkpoint_root=checkpoint_root)
    assert r1.status == ExecutionStatus.SUCCESS

    # Tamper with the stored hash to force a mismatch.
    hash_file = checkpoint_root / "r_hash1" / "_manifest_hash"
    hash_file.write_text("000000000000", encoding="utf-8")

    out_path2 = str(tmp_path / "out2.parquet")
    modules2 = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}, checkpoint=True),
        _module("eg", "Egress", {"format": "parquet", "path": out_path2, "mode": "overwrite"}),
    )
    manifest2 = Manifest(
        blueprint_id="test.hash_mismatch",
        context={},
        modules=modules2,
        edges=edges,
        engine_config={},
        checkpoint=True,
    )
    with _w.catch_warnings(record=True) as caught:
        _w.simplefilter("always")
        r2 = execute(
            manifest2,
            duckdb_con,
            run_id="r_hash2",
            checkpoint_root=checkpoint_root,
            resume_run_id="r_hash1",
        )
    assert r2.status == ExecutionStatus.SUCCESS
    assert any(
        "runtime_resume_hash_changed" in str(w.message) and "changed" in str(w.message)
        for w in caught
    )


def test_resume_mismatched_manifest_warning_is_suppressible(duckdb_con, tmp_path):
    """The documented suppression workflow (``warnings.suppress`` /
    ``--suppress-warning`` → ``warnings_suppress=`` on ``execute()``) must
    silence ``runtime_resume_hash_changed`` — proving the fix routes
    through ``aqueduct.warnings.emit()`` rather than a bare ``logger.warning``
    that the suppress list can never reach."""
    import warnings as _w

    src_path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    checkpoint_root = tmp_path / "checkpoints"
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}, checkpoint=True),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (Edge(from_id="ing", to_id="eg", port="main"),)
    manifest = Manifest(
        blueprint_id="test.hash_mismatch_suppressed",
        context={},
        modules=modules,
        edges=edges,
        engine_config={},
        checkpoint=True,
    )
    r1 = execute(manifest, duckdb_con, run_id="r_sup1", checkpoint_root=checkpoint_root)
    assert r1.status == ExecutionStatus.SUCCESS

    hash_file = checkpoint_root / "r_sup1" / "_manifest_hash"
    hash_file.write_text("000000000000", encoding="utf-8")

    out_path2 = str(tmp_path / "out2.parquet")
    modules2 = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}, checkpoint=True),
        _module("eg", "Egress", {"format": "parquet", "path": out_path2, "mode": "overwrite"}),
    )
    manifest2 = Manifest(
        blueprint_id="test.hash_mismatch_suppressed",
        context={},
        modules=modules2,
        edges=edges,
        engine_config={},
        checkpoint=True,
    )
    with _w.catch_warnings(record=True) as caught:
        _w.simplefilter("always")
        r2 = execute(
            manifest2,
            duckdb_con,
            run_id="r_sup2",
            checkpoint_root=checkpoint_root,
            resume_run_id="r_sup1",
            warnings_suppress={"runtime_resume_hash_changed"},
        )
    assert r2.status == ExecutionStatus.SUCCESS
    assert not any("runtime_resume_hash_changed" in str(w.message) for w in caught)


# ── Engine-invariant proof: retry_policy / module retry driven through the
# duckdb executor's own _with_retry, via a real execute() run ─────────────


def test_module_retry_driven_through_execute(duckdb_con, tmp_path, monkeypatch):
    """A Channel module fails twice then succeeds; module.retry (max_attempts=3,
    no backoff) must make the run succeed on the 3rd attempt — proving
    RetryPolicy/backoff aren't a rubber-stamp 'engine-invariant' verdict but
    are actually exercised by the duckdb executor's dispatch loop."""
    import aqueduct.executor.duckdb_.executor as executor_mod
    from aqueduct.executor.duckdb_.channel import ChannelError
    from aqueduct.parser.models import RetryPolicy

    calls = {"n": 0}
    real_execute_channel = executor_mod.execute_channel

    def flaky_execute_channel(module, upstream, con):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ChannelError("transient failure")
        return real_execute_channel(module, upstream, con)

    monkeypatch.setattr(executor_mod, "execute_channel", flaky_execute_channel)

    retry_policy = RetryPolicy(
        max_attempts=3,
        backoff_strategy="fixed",
        backoff_base_seconds=0,
        backoff_max_seconds=0,
        jitter=False,
        on_exhaustion="abort",
    )
    modules = (
        _module("ing", "Ingress", {"format": "csv", "path": "unused"}),
        _module("ch", "Channel", {"op": "filter", "condition": "1=1"}, retry=retry_policy),
    )
    edges = (Edge(from_id="ing", to_id="ch", port="main"),)
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )

    # Swap Ingress for a direct relation registration to avoid a real file read.
    monkeypatch.setattr(
        executor_mod, "read_ingress", lambda module, con, base_dir=None: con.sql("SELECT 1 AS a")
    )

    result = execute(manifest, duckdb_con, run_id="r_retry")
    assert result.status == ExecutionStatus.SUCCESS
    assert calls["n"] == 3
    assert {r.module_id: r.status for r in result.module_results} == {
        "ing": "success",
        "ch": "success",
    }


def test_module_retry_exhausted_fails_run(duckdb_con, monkeypatch):
    """Retry exhaustion (on_exhaustion=abort, always-failing handler) must
    still fail the run after the declared max_attempts — the honest converse
    of the success-path retry test above."""
    import aqueduct.executor.duckdb_.executor as executor_mod
    from aqueduct.executor.duckdb_.channel import ChannelError
    from aqueduct.parser.models import RetryPolicy

    calls = {"n": 0}

    def always_fails(module, upstream, con):
        calls["n"] += 1
        raise ChannelError("permanent failure")

    monkeypatch.setattr(executor_mod, "execute_channel", always_fails)
    monkeypatch.setattr(
        executor_mod, "read_ingress", lambda module, con, base_dir=None: con.sql("SELECT 1 AS a")
    )

    retry_policy = RetryPolicy(
        max_attempts=2,
        backoff_strategy="fixed",
        backoff_base_seconds=0,
        backoff_max_seconds=0,
        jitter=False,
        on_exhaustion="abort",
    )
    modules = (
        _module("ing", "Ingress", {"format": "csv", "path": "unused"}),
        _module("ch", "Channel", {"op": "filter", "condition": "1=1"}, retry=retry_policy),
    )
    edges = (Edge(from_id="ing", to_id="ch", port="main"),)
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )

    result = execute(manifest, duckdb_con, run_id="r_retry_exhausted")
    assert result.status == ExecutionStatus.ERROR
    assert calls["n"] == 2


# ── module.type.Arcade — Arcades expand at compile time, so the executor
# never dispatches one; the proof lives at the compile+run boundary. ──────


def test_arcade_compiles_and_runs_on_duckdb(tmp_path):
    from aqueduct.compiler.compiler import compile as aq_compile
    from aqueduct.parser.parser import parse

    arcade_file = tmp_path / "sub.yml"
    arcade_file.write_text(
        "aqueduct: '1.0'\nid: arcade.sub\nname: Sub\n"
        "modules:\n"
        "  - id: ch\n    type: Channel\n    label: C\n"
        "    config: {op: filter, condition: 'a > 1'}\n"
        "edges: []\n",
        encoding="utf-8",
    )
    parent_file = tmp_path / "parent.yml"
    src_path = tmp_path / "src.csv"
    src_path.write_text("a\n1\n2\n3\n", encoding="utf-8")
    out_path = tmp_path / "out.parquet"
    parent_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
        "modules:\n"
        f"  - id: ing\n    type: Ingress\n    label: I\n"
        f"    config: {{format: csv, path: '{src_path}'}}\n"
        "  - id: arc\n    type: Arcade\n    label: A\n    ref: 'sub.yml'\n"
        f"  - id: eg\n    type: Egress\n    label: E\n"
        f"    config: {{format: parquet, path: '{out_path}', mode: overwrite}}\n"
        "edges:\n  - from: ing\n    to: arc\n  - from: arc\n    to: eg\n",
        encoding="utf-8",
    )
    bp = parse(parent_file)
    manifest = aq_compile(bp, blueprint_path=parent_file, engine="duckdb")
    module_ids = {m.id for m in manifest.modules}
    assert "arc__ch" in module_ids

    import duckdb as duckdb_mod

    con = duckdb_mod.connect(":memory:")
    try:
        result = execute(manifest, con, run_id="r_arcade")
    finally:
        con.close()
    assert result.status == ExecutionStatus.SUCCESS
    assert sorted(
        r[0]
        for r in duckdb_mod.connect(":memory:")
        .sql(f"SELECT * FROM read_parquet('{out_path}')")
        .fetchall()
    ) == [2, 3]


# ── timezone: universal key (Phase 81/82) ────────────────────────────────────


def test_duckdb_make_session_applies_universal_timezone():
    """``SessionSpec.timezone`` (aqueduct.yml's top-level ``timezone:``) is
    applied via ``SET TimeZone`` — DuckDB has no ``engine.duckdb.*`` conf knob
    (see ``aqueduct.config.DuckDBEngineConfig``), so there is no engine-native
    override to defer to, unlike Spark."""
    from aqueduct.executor.protocol import SessionSpec

    protocol = get_protocol("duckdb")
    conn = protocol.make_session(SessionSpec(blueprint_id="tz-test", timezone="America/New_York"))
    try:
        assert conn.execute("SELECT current_setting('TimeZone')").fetchone() == (
            "America/New_York",
        )
    finally:
        protocol.close_session(conn)


def test_duckdb_make_session_no_timezone_is_a_no_op():
    from aqueduct.executor.protocol import SessionSpec

    protocol = get_protocol("duckdb")
    conn = protocol.make_session(SessionSpec(blueprint_id="tz-unset"))
    try:
        # No error, no forced write — whatever DuckDB's own default is stays.
        assert conn.execute("SELECT current_setting('TimeZone')").fetchone() is not None
    finally:
        protocol.close_session(conn)


# ── feature.table_addressing (Pass G2) — catalog table: addressing ─────────
#
# DuckDB genuinely has a catalog (memory.main, system.*, plus whatever ATTACH
# adds) — see duckdb_/ingress.py::_read_table's docstring for the full
# defaulting rule. These tests exercise the real implementation against a
# real DuckDBPyConnection, both engine-facing directions (Ingress read,
# Egress write) plus register_as_table's external-file registration.


def test_ingress_table_reads_existing_catalog_table(duckdb_con):
    duckdb_con.sql("CREATE TABLE orders AS SELECT 1 AS id, 'a' AS name")
    module = _module("ing", "Ingress", {"table": "orders"})
    rel = read_ingress(module, duckdb_con)
    assert rel.columns == ["id", "name"]
    assert rel.fetchall() == [(1, "a")]


def test_ingress_table_unresolvable_name_raises_ingress_error(duckdb_con):
    module = _module("ing", "Ingress", {"table": "does_not_exist"})
    with pytest.raises(IngressError, match="not found"):
        read_ingress(module, duckdb_con)


def test_ingress_table_and_path_mutually_exclusive(duckdb_con):
    module = _module("ing", "Ingress", {"table": "orders", "path": "x.csv"})
    with pytest.raises(IngressError, match="mutually exclusive"):
        read_ingress(module, duckdb_con)


def test_ingress_table_applies_schema_hint_and_partition_filters(duckdb_con):
    duckdb_con.sql("CREATE TABLE orders AS SELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 200")
    module = _module(
        "ing",
        "Ingress",
        {"table": "orders", "partition_filters": "amount > 100", "schema_hint": {"id": "int"}},
    )
    rel = read_ingress(module, duckdb_con)
    assert rel.fetchall() == [(2, 200)]


def test_egress_table_overwrite_creates_and_replaces(duckdb_con):
    rel1 = duckdb_con.sql("SELECT 1 AS id")
    write_egress(rel1, _module("e1", "Egress", {"table": "t1", "mode": "overwrite"}), duckdb_con)
    assert duckdb_con.table("t1").fetchall() == [(1,)]

    rel2 = duckdb_con.sql("SELECT 2 AS id")
    write_egress(rel2, _module("e2", "Egress", {"table": "t1", "mode": "overwrite"}), duckdb_con)
    assert duckdb_con.table("t1").fetchall() == [(2,)]


def test_egress_table_error_mode_raises_when_table_exists(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS id")
    write_egress(rel, _module("e1", "Egress", {"table": "t2", "mode": "overwrite"}), duckdb_con)
    with pytest.raises(EgressError, match="already exists"):
        write_egress(rel, _module("e2", "Egress", {"table": "t2", "mode": "error"}), duckdb_con)


def test_egress_table_ignore_mode_skips_when_table_exists(duckdb_con):
    rel1 = duckdb_con.sql("SELECT 1 AS id")
    write_egress(rel1, _module("e1", "Egress", {"table": "t3", "mode": "overwrite"}), duckdb_con)
    rel2 = duckdb_con.sql("SELECT 2 AS id")
    write_egress(rel2, _module("e2", "Egress", {"table": "t3", "mode": "ignore"}), duckdb_con)
    assert duckdb_con.table("t3").fetchall() == [(1,)]  # unchanged


def test_egress_table_append_creates_then_inserts(duckdb_con):
    rel1 = duckdb_con.sql("SELECT 1 AS id")
    write_egress(rel1, _module("e1", "Egress", {"table": "t4", "mode": "append"}), duckdb_con)
    rel2 = duckdb_con.sql("SELECT 2 AS id")
    write_egress(rel2, _module("e2", "Egress", {"table": "t4", "mode": "append"}), duckdb_con)
    assert sorted(duckdb_con.table("t4").fetchall()) == [(1,), (2,)]


def test_egress_table_and_path_mutually_exclusive(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS id")
    module = _module("e1", "Egress", {"table": "t5", "path": "x.parquet", "format": "parquet"})
    with pytest.raises(EgressError, match="mutually exclusive"):
        write_egress(rel, module, duckdb_con)


def test_egress_register_as_table_readable_back_by_ingress(duckdb_con, tmp_path):
    path = str(tmp_path / "out.parquet")
    rel = duckdb_con.sql("SELECT 1 AS id, 'x' AS name")
    write_egress(
        rel,
        _module(
            "e1",
            "Egress",
            {
                "format": "parquet",
                "path": path,
                "mode": "overwrite",
                "register_as_table": "registered_v",
            },
        ),
        duckdb_con,
    )
    readback = read_ingress(_module("ing", "Ingress", {"table": "registered_v"}), duckdb_con)
    assert readback.fetchall() == [(1, "x")]


def test_egress_register_as_table_reflects_current_file_contents(duckdb_con, tmp_path):
    """The registered name is a live VIEW over the file, not a snapshot copy —
    re-writing the same path changes what a later read-by-name sees."""
    path = str(tmp_path / "out.parquet")
    write_egress(
        duckdb_con.sql("SELECT 1 AS id"),
        _module(
            "e1",
            "Egress",
            {"format": "parquet", "path": path, "mode": "overwrite", "register_as_table": "v"},
        ),
        duckdb_con,
    )
    write_egress(
        duckdb_con.sql("SELECT 2 AS id"),
        _module(
            "e2",
            "Egress",
            {"format": "parquet", "path": path, "mode": "overwrite", "register_as_table": "v"},
        ),
        duckdb_con,
    )
    assert duckdb_con.table("v").fetchall() == [(2,)]


def test_egress_table_ignored_when_register_as_table_also_set(duckdb_con, caplog):
    """`table:` writes already register the name; `register_as_table` is
    ignored with a warning (never a silent drop) — mirrors Spark's
    `runtime_egress_register_as_table_ignored` behaviour exactly."""
    rel = duckdb_con.sql("SELECT 1 AS id")
    module = _module(
        "e1", "Egress", {"table": "t6", "mode": "overwrite", "register_as_table": "unused_name"}
    )
    write_egress(rel, module, duckdb_con)
    assert duckdb_con.table("t6").fetchall() == [(1,)]
    with pytest.raises(Exception):
        duckdb_con.table("unused_name")


# ── Junction branch port → Channel (the 2.2.2 include-list bug) ────────────
# `_incoming_main` used to be an include-list (`e.port == "main"`), so every
# module type except Egress/Handoff rejected a Junction branch-port edge with
# "has no main-port incoming edges" — even though docs/specs.md's port table
# has always said a `<branch_id>` port is consumed by "Any downstream module".
# It is now an exclude-list (any data edge that is not `signal`/`spillway`);
# see `aqueduct/executor/edge_ports.py`.


def test_junction_branch_feeds_channel_fan_shape(duckdb_con, tmp_path):
    """Ingress -> Junction (2 branches) -> a Channel each -> an Egress each."""
    src_path = _write_parquet(
        duckdb_con, tmp_path, "src", "SELECT * FROM (VALUES (1),(2),(3),(4)) t(a)"
    )
    out_hi = str(tmp_path / "hi.parquet")
    out_lo = str(tmp_path / "lo.parquet")
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module(
            "j",
            "Junction",
            {
                "mode": "conditional",
                "branches": [
                    {"id": "hi", "condition": "a > 2"},
                    {"id": "lo", "condition": "_else_"},
                ],
            },
        ),
        # Each Channel reads its branch by the branch's frame key
        # (`<junction_id>.<branch_id>`), which is what the SQL text names.
        _module("ch_hi", "Channel", {"op": "sql", "query": 'SELECT a * 10 AS a FROM "j.hi"'}),
        _module("ch_lo", "Channel", {"op": "sql", "query": 'SELECT a * 100 AS a FROM "j.lo"'}),
        _module("eg_hi", "Egress", {"format": "parquet", "path": out_hi, "mode": "overwrite"}),
        _module("eg_lo", "Egress", {"format": "parquet", "path": out_lo, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="j", port="main"),
        Edge(from_id="j", to_id="ch_hi", port="hi"),
        Edge(from_id="j", to_id="ch_lo", port="lo"),
        Edge(from_id="ch_hi", to_id="eg_hi", port="main"),
        Edge(from_id="ch_lo", to_id="eg_lo", port="main"),
    )
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )
    result = execute(manifest, duckdb_con, run_id="r_junction_fan")

    assert result.status == ExecutionStatus.SUCCESS, [
        (r.module_id, r.status, r.error) for r in result.module_results
    ]
    assert sorted(r[0] for r in duckdb_con.read_parquet(out_hi).fetchall()) == [30, 40]
    assert sorted(r[0] for r in duckdb_con.read_parquet(out_lo).fetchall()) == [100, 200]


def test_junction_branch_into_channel_is_not_a_missing_main_port(duckdb_con, tmp_path):
    """Regression: the Channel must not report "no main-port incoming edges"."""
    src_path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module("j", "Junction", {"mode": "broadcast", "branches": [{"id": "only"}]}),
        _module("ch", "Channel", {"op": "sql", "query": 'SELECT a FROM "j.only"'}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="j", port="main"),
        Edge(from_id="j", to_id="ch", port="only"),
        Edge(from_id="ch", to_id="eg", port="main"),
    )
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )
    result = execute(manifest, duckdb_con, run_id="r_junction_branch_channel")

    errors = [r.error for r in result.module_results if r.error]
    assert not any("has no main-port incoming edges" in (e or "") for e in errors), errors
    assert result.status == ExecutionStatus.SUCCESS, errors
