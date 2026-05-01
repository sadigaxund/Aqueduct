"""aqueduct test — isolated module testing without real sources.

Test file format (YAML):

    aqueduct_test: "1.0"
    blueprint: blueprint.yml      # relative to test file

    tests:
      - id: test_filter_nulls
        description: "Null amounts must be removed"
        module: clean_orders     # module ID to test (Channel/Junction/Funnel)

        inputs:
          raw_orders:            # upstream module ID
            schema:
              order_id: long
              amount: double
              order_date: string
            rows:
              - [1, 10.0, "2026-01-01"]
              - [2, null, "2026-01-01"]

        assertions:
          - type: row_count
            expected: 1
          - type: contains
            rows:
              - {order_id: 1, amount: 10.0}
          - type: sql
            expr: "SELECT count(*) = 1 FROM __output__"

Supports: Channel (op: sql, op: join), Junction, Funnel, Assert.
Skips: Ingress, Egress (no external I/O).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


# ── Result types ──────────────────────────────────────────────────────────────

@dataclass
class AssertionResult:
    passed: bool
    assertion_type: str
    message: str


@dataclass
class TestCaseResult:
    test_id: str
    passed: bool
    error: str | None = None
    assertion_results: list[AssertionResult] = field(default_factory=list)


@dataclass
class TestSuiteResult:
    total: int
    passed: int
    failed: int
    results: list[TestCaseResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.failed == 0


class TestError(Exception):
    """Raised for test file schema errors, not assertion failures."""


# ── Spark type mapping ────────────────────────────────────────────────────────

_TYPE_MAP = {
    "long": "bigint",
    "int": "int",
    "integer": "int",
    "short": "smallint",
    "byte": "tinyint",
    "double": "double",
    "float": "float",
    "string": "string",
    "boolean": "boolean",
    "bool": "boolean",
    "timestamp": "timestamp",
    "date": "date",
    "binary": "binary",
    "decimal": "decimal(38,18)",
}


def _spark_type(user_type: str) -> str:
    return _TYPE_MAP.get(user_type.lower(), user_type)


def _schema_ddl(schema: dict[str, str]) -> str:
    parts = [f"`{col}` {_spark_type(t)}" for col, t in schema.items()]
    return ", ".join(parts)


# ── DataFrame creation ────────────────────────────────────────────────────────

def _create_df(spark: "SparkSession", schema_dict: dict[str, str], rows: list[list]) -> "DataFrame":
    from pyspark.sql.types import StructType, _parse_datatype_string

    fields = []
    for col_name, col_type in schema_dict.items():
        spark_type_str = _spark_type(col_type)
        dtype = _parse_datatype_string(spark_type_str)
        from pyspark.sql.types import StructField
        fields.append(StructField(col_name, dtype, nullable=True))

    schema = StructType(fields)
    return spark.createDataFrame([tuple(r) for r in rows], schema=schema)


# ── Assertion evaluation ──────────────────────────────────────────────────────

def _run_assertion(
    assertion: dict[str, Any],
    result_df: "DataFrame",
    spark: "SparkSession",
    view_name: str = "__output__",
) -> AssertionResult:
    atype = assertion.get("type", "")

    if atype == "row_count":
        expected = int(assertion["expected"])
        actual = result_df.count()
        passed = actual == expected
        return AssertionResult(
            passed=passed,
            assertion_type="row_count",
            message=f"expected {expected} rows, got {actual}",
        )

    elif atype == "contains":
        expected_rows: list[dict] = assertion.get("rows", [])
        missing = []
        for expected_row in expected_rows:
            filter_parts = [
                f"`{col}` = {_sql_literal(val)}"
                if val is not None
                else f"`{col}` IS NULL"
                for col, val in expected_row.items()
            ]
            filter_expr = " AND ".join(filter_parts)
            count = result_df.filter(filter_expr).count()
            if count == 0:
                missing.append(expected_row)
        passed = len(missing) == 0
        msg = "all expected rows found" if passed else f"{len(missing)} expected row(s) not found: {missing}"
        return AssertionResult(passed=passed, assertion_type="contains", message=msg)

    elif atype == "sql":
        expr = assertion.get("expr", "")
        if not expr:
            return AssertionResult(passed=False, assertion_type="sql", message="sql assertion missing 'expr'")
        result_df.createOrReplaceTempView(view_name)
        try:
            row = spark.sql(expr).collect()
            # Expression should return a single boolean value
            if not row:
                passed = False
                msg = f"sql expr returned no rows: {expr!r}"
            else:
                val = row[0][0]
                passed = bool(val)
                msg = f"sql expr evaluated to {val!r}: {expr!r}"
        except Exception as exc:
            passed = False
            msg = f"sql expr error: {exc}"
        return AssertionResult(passed=passed, assertion_type="sql", message=msg)

    else:
        return AssertionResult(
            passed=False,
            assertion_type=atype,
            message=f"unknown assertion type {atype!r}",
        )


def _sql_literal(val: Any) -> str:
    if isinstance(val, str):
        escaped = val.replace("'", "\\'")
        return f"'{escaped}'"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    return str(val)


# ── Module execution dispatch ─────────────────────────────────────────────────

_TESTABLE_TYPES = frozenset({"Channel", "Junction", "Funnel", "Assert"})


def _execute_module(
    module: Any,
    input_dfs: dict[str, "DataFrame"],
    spark: "SparkSession",
) -> "DataFrame | dict[str, DataFrame]":
    """Run a single module against inline DataFrames. Returns result DataFrame(s)."""
    if module.type == "Channel":
        from aqueduct.executor.spark.channel import execute_sql_channel
        return execute_sql_channel(module, input_dfs, spark)

    elif module.type == "Junction":
        from aqueduct.executor.spark.junction import execute_junction
        if len(input_dfs) != 1:
            raise TestError(f"Junction {module.id!r} expects exactly 1 input, got {len(input_dfs)}")
        df = next(iter(input_dfs.values()))
        return execute_junction(module, df)

    elif module.type == "Funnel":
        from aqueduct.executor.spark.funnel import execute_funnel
        return execute_funnel(module, input_dfs)

    elif module.type == "Assert":
        from aqueduct.executor.spark.assert_ import execute_assert
        if len(input_dfs) != 1:
            raise TestError(f"Assert {module.id!r} expects exactly 1 input, got {len(input_dfs)}")
        df = next(iter(input_dfs.values()))
        passing_df, _ = execute_assert(module, df, spark, run_id="test", blueprint_id="test")
        return passing_df

    else:
        raise TestError(
            f"Module {module.id!r} has type {module.type!r}. "
            f"Only {sorted(_TESTABLE_TYPES)} can be tested in isolation."
        )


# ── Test case runner ──────────────────────────────────────────────────────────

def _run_test_case(
    test_case: dict[str, Any],
    blueprint_modules: dict[str, Any],
    spark: "SparkSession",
) -> TestCaseResult:
    test_id = test_case.get("id", "(unnamed)")
    target_id: str = test_case.get("module", "")
    if not target_id:
        return TestCaseResult(test_id=test_id, passed=False, error="test case missing 'module' field")

    module = blueprint_modules.get(target_id)
    if module is None:
        return TestCaseResult(
            test_id=test_id, passed=False,
            error=f"module {target_id!r} not found in blueprint",
        )
    if module.type not in _TESTABLE_TYPES:
        return TestCaseResult(
            test_id=test_id, passed=False,
            error=f"module {target_id!r} is type {module.type!r}; only {sorted(_TESTABLE_TYPES)} are testable",
        )

    # Build input DataFrames from inline rows
    inputs_spec: dict[str, dict] = test_case.get("inputs", {})
    if not inputs_spec:
        return TestCaseResult(test_id=test_id, passed=False, error="test case missing 'inputs'")

    input_dfs: dict[str, "DataFrame"] = {}
    for src_id, src_spec in inputs_spec.items():
        schema_dict = src_spec.get("schema", {})
        rows = src_spec.get("rows", [])
        if not schema_dict:
            return TestCaseResult(
                test_id=test_id, passed=False,
                error=f"input {src_id!r} missing 'schema'",
            )
        try:
            input_dfs[src_id] = _create_df(spark, schema_dict, rows)
        except Exception as exc:
            return TestCaseResult(
                test_id=test_id, passed=False,
                error=f"failed to create DataFrame for input {src_id!r}: {exc}",
            )

    # Execute module
    try:
        raw_result = _execute_module(module, input_dfs, spark)
    except Exception as exc:
        return TestCaseResult(test_id=test_id, passed=False, error=str(exc))

    # For Junction, test assertions against the first branch unless user specifies a branch
    if isinstance(raw_result, dict):
        branch = test_case.get("branch")
        if branch:
            result_df = raw_result.get(branch)
            if result_df is None:
                return TestCaseResult(
                    test_id=test_id, passed=False,
                    error=f"Junction branch {branch!r} not found in output",
                )
        else:
            first_key = next(iter(raw_result))
            result_df = raw_result[first_key]
            logger.debug("Junction test: using branch %r (first). Specify 'branch:' to select another.", first_key)
    else:
        result_df = raw_result

    # Run assertions
    assertions: list[dict] = test_case.get("assertions", [])
    assertion_results: list[AssertionResult] = []
    all_passed = True

    for assertion in assertions:
        try:
            ar = _run_assertion(assertion, result_df, spark)
        except Exception as exc:
            ar = AssertionResult(
                passed=False,
                assertion_type=assertion.get("type", "?"),
                message=f"assertion error: {exc}",
            )
        assertion_results.append(ar)
        if not ar.passed:
            all_passed = False

    return TestCaseResult(
        test_id=test_id,
        passed=all_passed,
        assertion_results=assertion_results,
    )


# ── Public API ────────────────────────────────────────────────────────────────

def run_test_file(
    test_file: Path,
    spark: "SparkSession",
    blueprint_path_override: Path | None = None,
) -> TestSuiteResult:
    """Parse a test YAML file and run all test cases.

    Args:
        test_file:               Path to the .aqtest.yml (or any .yml) test file.
        spark:                   Active SparkSession.
        blueprint_path_override: Override the blueprint path from the test file.

    Returns:
        TestSuiteResult with per-test results.
    """
    import yaml

    raw = yaml.safe_load(test_file.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TestError(f"Test file {test_file} is not a valid YAML mapping.")

    # Resolve blueprint path
    if blueprint_path_override:
        bp_path = blueprint_path_override
    else:
        bp_rel = raw.get("blueprint")
        if not bp_rel:
            raise TestError("Test file missing 'blueprint' field.")
        bp_path = (test_file.parent / bp_rel).resolve()

    if not bp_path.exists():
        raise TestError(f"Blueprint not found: {bp_path}")

    # Parse blueprint (no compile — we only need module config, not @aq.* resolution)
    from aqueduct.parser.parser import parse
    bp = parse(str(bp_path))
    modules_by_id = {m.id: m for m in bp.modules}

    tests: list[dict] = raw.get("tests", [])
    if not tests:
        logger.warning("Test file %s has no test cases.", test_file)
        return TestSuiteResult(total=0, passed=0, failed=0)

    results: list[TestCaseResult] = []
    for test_case in tests:
        result = _run_test_case(test_case, modules_by_id, spark)
        results.append(result)

    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed

    return TestSuiteResult(
        total=len(results),
        passed=passed,
        failed=failed,
        results=results,
    )
