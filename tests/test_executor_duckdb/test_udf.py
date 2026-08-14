"""DuckDB engine UDF registration tests (Pass E item 2).

Mirrors tests/test_executor/test_udf.py's coverage shape, adapted to a real
DuckDB connection (Spark's suite mocks the SparkSession; a real
`conn.create_function` call is cheap enough here that mocking buys nothing
and would hide the null-handling/row-at-a-time semantics this module exists
to prove).
"""

from __future__ import annotations

import pytest

from aqueduct.executor.duckdb_.udf import UDFError, register_udfs

pytestmark = pytest.mark.duckdb


def test_unsupported_lang_raises(duckdb_con):
    with pytest.raises(UDFError, match="language 'fortran' is not supported"):
        register_udfs(({"id": "my_udf", "lang": "fortran"},), duckdb_con)


def test_java_lang_raises_clean_error(duckdb_con):
    """java/scala UDFs are honestly unsupported — DuckDB is not on the JVM."""
    with pytest.raises(UDFError, match="not supported on the DuckDB engine"):
        register_udfs(({"id": "my_udf", "lang": "java", "jar": "x.jar"},), duckdb_con)
    with pytest.raises(UDFError, match="not supported on the DuckDB engine"):
        register_udfs(({"id": "my_udf", "lang": "scala", "jar": "x.jar"},), duckdb_con)


def test_missing_module_raises(duckdb_con):
    with pytest.raises(UDFError, match="'module' is required"):
        register_udfs(({"id": "my_udf", "lang": "python"},), duckdb_con)


def test_nonexistent_module_raises(duckdb_con):
    with pytest.raises(UDFError, match="cannot import module"):
        register_udfs(({"id": "my_udf", "lang": "python", "module": "no.such.module"},), duckdb_con)


def test_missing_entry_raises(duckdb_con):
    with pytest.raises(UDFError, match="function 'nonexistent_fn' not found"):
        register_udfs(
            ({"id": "my_udf", "lang": "python", "module": "json", "entry": "nonexistent_fn"},),
            duckdb_con,
        )


def test_successful_registration_and_call(duckdb_con):
    """math.sqrt is a real importable callable with a single positional
    parameter — register + actually call it. (json.loads was deliberately
    avoided: its many optional keyword-only parameters make DuckDB's
    signature-introspected function require all of them, an artifact of that
    particular stdlib function's signature, not of this module.)"""
    register_udfs(
        (
            {
                "id": "sqrt_udf",
                "lang": "python",
                "module": "math",
                "entry": "sqrt",
                "return_type": "double",
            },
        ),
        duckdb_con,
    )
    result = duckdb_con.sql("SELECT sqrt_udf(9.0) AS r").fetchone()
    assert result[0] == 3.0


def test_entry_defaults_to_udf_id(duckdb_con):
    register_udfs(
        ({"id": "sqrt", "lang": "python", "module": "math", "return_type": "double"},),
        duckdb_con,
    )
    result = duckdb_con.sql("SELECT sqrt(9.0) AS r").fetchone()
    assert result[0] == 3.0


def test_row_at_a_time_not_batched(duckdb_con):
    """Proves the NATIVE (default) execution path really is row-at-a-time —
    the exact claim backing this leaf's capability hint, not an assumption."""
    calls = []

    def counting_fn(x):
        calls.append(x)
        return x * 2

    import sys
    import types

    mod = types.ModuleType("_aq_test_udf_mod")
    mod.counting_fn = counting_fn
    sys.modules["_aq_test_udf_mod"] = mod
    try:
        register_udfs(
            (
                {
                    "id": "double_it",
                    "lang": "python",
                    "module": "_aq_test_udf_mod",
                    "entry": "counting_fn",
                    "return_type": "bigint",
                },
            ),
            duckdb_con,
        )
        rows = duckdb_con.sql(
            "SELECT double_it(x) AS r FROM (VALUES (1), (2), (3), (4), (5)) t(x) ORDER BY x"
        ).fetchall()
    finally:
        del sys.modules["_aq_test_udf_mod"]

    assert [r[0] for r in rows] == [2, 4, 6, 8, 10]
    assert sorted(calls) == [1, 2, 3, 4, 5]  # one call per input row, not one call per batch


def test_null_argument_still_calls_function(duckdb_con):
    """DuckDB's DEFAULT null_handling skips the call entirely on a NULL
    argument (measured — see module docstring). Spark's Python UDFs always
    invoke the function. This proves the ``null_handling='special'`` override
    actually restores that — the exact contract gallery snippet
    05_channel_sql_udf's mask_email(email: str | None) relies on."""
    import sys
    import types

    calls = []

    def mask_or_none(x):
        calls.append(x)
        return "" if x is None else f"masked:{x}"

    mod = types.ModuleType("_aq_test_udf_null_mod")
    mod.mask_or_none = mask_or_none
    sys.modules["_aq_test_udf_null_mod"] = mod
    try:
        register_udfs(
            (
                {
                    "id": "mask_or_none",
                    "lang": "python",
                    "module": "_aq_test_udf_null_mod",
                    "return_type": "string",
                },
            ),
            duckdb_con,
        )
        rows = duckdb_con.sql(
            "SELECT mask_or_none(x) AS r FROM (VALUES ('a'), (NULL), ('b')) t(x)"
        ).fetchall()
    finally:
        del sys.modules["_aq_test_udf_null_mod"]

    assert rows == [("masked:a",), ("",), ("masked:b",)]
    assert None in calls  # the function WAS called with None, not short-circuited


def test_exception_in_udf_propagates(duckdb_con):
    """An uncaught exception aborts the query — same as an uncaught exception
    in a Spark Python UDF aborts the Spark action."""
    import sys
    import types

    def boom(x):
        raise ValueError("bad row")

    mod = types.ModuleType("_aq_test_udf_boom_mod")
    mod.boom = boom
    sys.modules["_aq_test_udf_boom_mod"] = mod
    try:
        register_udfs(
            (
                {
                    "id": "boom",
                    "lang": "python",
                    "module": "_aq_test_udf_boom_mod",
                    "return_type": "bigint",
                },
            ),
            duckdb_con,
        )
        with pytest.raises(Exception, match="bad row"):
            duckdb_con.sql("SELECT boom(x) FROM (VALUES (1)) t(x)").fetchall()
    finally:
        del sys.modules["_aq_test_udf_boom_mod"]


def test_parameterized_udf_factory(duckdb_con):
    """`entry` is a factory when `params:` is given — mirrors Spark's
    parameterized-UDF contract exactly (gallery snippet 05_channel_sql_udf's
    make_phone_masker)."""
    import sys
    import types

    def make_adder(offset):
        def add(x):
            return x + offset

        return add

    mod = types.ModuleType("_aq_test_udf_factory_mod")
    mod.make_adder = make_adder
    sys.modules["_aq_test_udf_factory_mod"] = mod
    try:
        register_udfs(
            (
                {
                    "id": "add_offset",
                    "lang": "python",
                    "module": "_aq_test_udf_factory_mod",
                    "entry": "make_adder",
                    "return_type": "bigint",
                    "params": {"offset": 10},
                },
            ),
            duckdb_con,
        )
        rows = duckdb_con.sql(
            "SELECT add_offset(x) AS r FROM (VALUES (1), (2)) t(x) ORDER BY x"
        ).fetchall()
    finally:
        del sys.modules["_aq_test_udf_factory_mod"]

    assert [r[0] for r in rows] == [11, 12]


def test_missing_numpy_gives_actionable_message(monkeypatch):
    """A base install (`pip install aqueduct-core`, no `[duckdb]` extra) lacks
    numpy, which `con.create_function()` requires internally for argument/
    result marshalling on every Python UDF, regardless of return type. The
    raw DuckDB error text ("'numpy' is required for this operation, but it
    wasn't installed") gives no indication this is a packaging gap rather
    than a bug in the user's UDF. When numpy is genuinely absent, the error
    must name the fix (`pip install aqueduct-core[duckdb]`) while still
    chaining the underlying DuckDB error."""
    import aqueduct.executor.duckdb_.udf as udf_mod

    monkeypatch.setattr(udf_mod.importlib.util, "find_spec", lambda name: None)

    class _FakeConn:
        def create_function(self, *args, **kwargs):
            raise RuntimeError(
                "Invalid Input Error: 'numpy' is required for this operation, "
                "but it wasn't installed"
            )

    with pytest.raises(UDFError, match=r"pip install 'aqueduct-core\[duckdb\]'") as excinfo:
        register_udfs(
            (
                {
                    "id": "my_udf",
                    "lang": "python",
                    "module": "math",
                    "entry": "sqrt",
                    "return_type": "double",
                },
            ),
            _FakeConn(),
        )
    assert excinfo.value.__cause__ is not None
    assert "wasn't installed" in str(excinfo.value.__cause__)


def test_generic_create_function_failure_unchanged_when_numpy_present(monkeypatch):
    """When numpy IS installed, an unrelated create_function() failure keeps
    the plain (non-numpy-specific) message — the detection must not fire on
    every failure, only the numpy-missing one."""
    import aqueduct.executor.duckdb_.udf as udf_mod

    monkeypatch.setattr(udf_mod.importlib.util, "find_spec", lambda name: object())

    class _FakeConn:
        def create_function(self, *args, **kwargs):
            raise RuntimeError("some unrelated duckdb failure")

    with pytest.raises(
        UDFError, match=r"con\.create_function\(\) failed: some unrelated"
    ) as excinfo:
        register_udfs(
            (
                {
                    "id": "my_udf",
                    "lang": "python",
                    "module": "math",
                    "entry": "sqrt",
                    "return_type": "double",
                },
            ),
            _FakeConn(),
        )
    assert "pip install" not in str(excinfo.value)


def test_deterministic_false_sets_side_effects(duckdb_con):
    """deterministic: false must not raise and must still register a working
    UDF — proves the side_effects inversion doesn't break registration."""
    import sys
    import types

    def rand_ish(x):
        return x + 1

    mod = types.ModuleType("_aq_test_udf_nondeterministic_mod")
    mod.rand_ish = rand_ish
    sys.modules["_aq_test_udf_nondeterministic_mod"] = mod
    try:
        register_udfs(
            (
                {
                    "id": "rand_ish",
                    "lang": "python",
                    "module": "_aq_test_udf_nondeterministic_mod",
                    "return_type": "bigint",
                    "deterministic": False,
                },
            ),
            duckdb_con,
        )
        result = duckdb_con.sql("SELECT rand_ish(41) AS r").fetchone()
    finally:
        del sys.modules["_aq_test_udf_nondeterministic_mod"]

    assert result[0] == 42
