"""DuckDB Assert executor tests (Pass D).

Covers every rule type (schema_match, min_rows, max_rows, freshness,
not_null, sql, sql_row, custom, spillway_rate, null_rate) and every on_fail
action (abort, warn, webhook, quarantine, trigger_agent) against a real
DuckDB relation, plus one end-to-end test driven through ``execute()`` that
backs the ``module.type.Assert: supported`` capability verdict.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from aqueduct.executor.duckdb_.assert_ import AssertError, execute_assert
from aqueduct.executor.duckdb_.executor import execute
from aqueduct.executor.models import ExecutionStatus
from aqueduct.models import Edge, Manifest, Module

pytestmark = pytest.mark.duckdb


def _module(id_, type_, config, **kw):
    return Module(id=id_, type=type_, label=id_, config=config, **kw)


def _assert_module(rules, id_="assert1"):
    return _module(id_, "Assert", {"rules": rules})


# ── schema_match ──────────────────────────────────────────────────────────


def test_schema_match_passes(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS id, 'a' AS name")
    mod = _assert_module([{"type": "schema_match", "expected": {"id": "int", "name": "string"}}])
    passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert passing is rel
    assert quarantine is None


def test_schema_match_missing_column_aborts(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS id")
    mod = _assert_module(
        [{"type": "schema_match", "expected": {"id": "int", "missing_col": "string"}}]
    )
    with pytest.raises(AssertError, match="missing columns"):
        execute_assert(mod, rel, duckdb_con, "r1", "bp")


def test_schema_match_type_mismatch_aborts(duckdb_con):
    rel = duckdb_con.sql("SELECT 'x' AS id")  # VARCHAR, not INTEGER
    mod = _assert_module([{"type": "schema_match", "expected": {"id": "int"}}])
    with pytest.raises(AssertError, match="type mismatches"):
        execute_assert(mod, rel, duckdb_con, "r1", "bp")


# ── Pass G2 — numeric-family widening ───────────────────────────────────────
#
# DuckDB's CSV sniffer only ever infers BIGINT for whole-number columns
# (never the narrower TINYINT/SMALLINT/INTEGER Spark's own inference can
# pick for the same small values) — schema_match must accept a narrower
# hub-authored expectation against DuckDB's genuinely wider actual type, the
# same "portable across engines" contract 24_assert_types_full documents.


def test_schema_match_int_expected_widens_to_bigint_actual(duckdb_con):
    rel = duckdb_con.sql("SELECT CAST(1001 AS BIGINT) AS order_id")
    mod = _assert_module([{"type": "schema_match", "expected": {"order_id": "int"}}])
    passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert passing is rel
    assert quarantine is None


def test_schema_match_bigint_expected_does_not_widen_from_int_actual(duckdb_con):
    """The reverse direction (expected WIDER than actual) must still fail —
    widening is one-directional, not a blanket "same family" pass."""
    rel = duckdb_con.sql("SELECT CAST(1001 AS INTEGER) AS order_id")
    mod = _assert_module([{"type": "schema_match", "expected": {"order_id": "bigint"}}])
    with pytest.raises(AssertError, match="type mismatches"):
        execute_assert(mod, rel, duckdb_con, "r1", "bp")


# ── min_rows / max_rows ──────────────────────────────────────────────────


def test_min_rows_passes(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM range(5) t(id)")
    mod = _assert_module([{"type": "min_rows", "min": 3}])
    passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert quarantine is None


def test_min_rows_fails_aborts(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM range(2) t(id)")
    mod = _assert_module([{"type": "min_rows", "min": 5}])
    with pytest.raises(AssertError, match="min_rows"):
        execute_assert(mod, rel, duckdb_con, "r1", "bp")


def test_max_rows_fails_aborts(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM range(10) t(id)")
    mod = _assert_module([{"type": "max_rows", "max": 5}])
    with pytest.raises(AssertError, match="max_rows"):
        execute_assert(mod, rel, duckdb_con, "r1", "bp")


# ── not_null ──────────────────────────────────────────────────────────────


def test_not_null_aborts_on_null(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1), (NULL)) t(a)")
    mod = _assert_module([{"type": "not_null", "column": "a", "on_fail": "abort"}])
    with pytest.raises(AssertError, match="not_null"):
        execute_assert(mod, rel, duckdb_con, "r1", "bp")


def test_not_null_quarantine_routes_null_rows(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1), (NULL), (3)) t(a)")
    mod = _assert_module([{"type": "not_null", "column": "a", "on_fail": "quarantine"}])
    passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert sorted(r[0] for r in passing.fetchall()) == [1, 3]
    assert quarantine is not None
    assert quarantine.fetchall()[0][0] is None
    assert "_aq_error_type" in quarantine.columns


# ── freshness ─────────────────────────────────────────────────────────────


def test_freshness_aggregate_aborts_when_stale(duckdb_con):
    rel = duckdb_con.sql("SELECT TIMESTAMP '2000-01-01 00:00:00' AS ts")
    mod = _assert_module(
        [{"type": "freshness", "column": "ts", "max_age_hours": 1, "on_fail": "abort"}]
    )
    with pytest.raises(AssertError, match="freshness"):
        execute_assert(mod, rel, duckdb_con, "r1", "bp")


def test_freshness_quarantine_routes_stale_rows(duckdb_con):
    rel = duckdb_con.sql(
        "SELECT * FROM (VALUES (CURRENT_TIMESTAMP), (TIMESTAMP '2000-01-01 00:00:00')) t(ts)"
    )
    mod = _assert_module(
        [{"type": "freshness", "column": "ts", "max_age_hours": 1, "on_fail": "quarantine"}]
    )
    passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert len(passing.fetchall()) == 1
    assert quarantine is not None
    assert len(quarantine.fetchall()) == 1


# ── sql (aggregate boolean expr, transpiled) ────────────────────────────


def test_sql_rule_passes(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (100), (200)) t(amount)")
    mod = _assert_module([{"type": "sql", "expr": "MAX(amount) <= 2000"}])
    passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert quarantine is None


def test_sql_rule_fails_aborts(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (5000)) t(amount)")
    mod = _assert_module([{"type": "sql", "expr": "MAX(amount) <= 2000"}])
    with pytest.raises(AssertError, match="sql assertion failed"):
        execute_assert(mod, rel, duckdb_con, "r1", "bp")


# ── sql_row ───────────────────────────────────────────────────────────────


def test_sql_row_quarantine_routes_failing_rows(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (10), (-5), (20)) t(amount)")
    mod = _assert_module([{"type": "sql_row", "expr": "amount > 0", "on_fail": "quarantine"}])
    passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert sorted(r[0] for r in passing.fetchall()) == [10, 20]
    assert quarantine is not None
    assert [r[0] for r in quarantine.fetchall()] == [-5]


def test_sql_row_min_pass_rate_aborts(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1), (-1), (-1), (-1)) t(amount)")
    mod = _assert_module(
        [
            {
                "type": "sql_row",
                "expr": "amount > 0",
                "min_pass_rate": 0.5,
                "on_fail": "quarantine",
            }
        ]
    )
    with pytest.raises(AssertError, match="pass_rate"):
        execute_assert(mod, rel, duckdb_con, "r1", "bp")


def test_sql_row_abort_on_any_failure(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1), (-1)) t(amount)")
    mod = _assert_module([{"type": "sql_row", "expr": "amount > 0", "on_fail": "abort"}])
    with pytest.raises(AssertError, match="failed: amount > 0"):
        execute_assert(mod, rel, duckdb_con, "r1", "bp")


# ── custom ────────────────────────────────────────────────────────────────


def test_custom_rule_passes(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1), (2)) t(a)")

    def _check(r):
        return {"passed": True}

    mod = _assert_module([{"type": "custom", "fn": "fake.mod.check"}])
    with patch(
        "aqueduct.executor.duckdb_.assert_._load_custom_callable",
        return_value=_check,
    ):
        passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert quarantine is None


def test_custom_rule_quarantine(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1), (2000)) t(amount)")

    def _check(r):
        bad = r.filter("amount > 1000")
        count = bad.aggregate("COUNT(*) AS c").fetchone()[0]
        return {
            "passed": count == 0,
            "message": f"{count} over-limit rows",
            "quarantine_df": bad if count else None,
        }

    mod = _assert_module([{"type": "custom", "fn": "fake.mod.check", "on_fail": "quarantine"}])
    with patch(
        "aqueduct.executor.duckdb_.assert_._load_custom_callable",
        return_value=_check,
    ):
        passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert quarantine is not None
    assert quarantine.fetchall()[0][0] == 2000


def test_custom_rule_exception_warns_and_continues(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")

    def _boom(r):
        raise RuntimeError("bad user code")

    mod = _assert_module([{"type": "custom", "fn": "fake.mod.boom"}])
    with patch(
        "aqueduct.executor.duckdb_.assert_._load_custom_callable",
        return_value=_boom,
    ):
        passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert quarantine is None
    assert passing is rel


def test_custom_rule_exception_aborts_when_on_fail_abort(duckdb_con):
    """Regression (Pass G1): a custom rule whose own CODE broke used to fail
    OPEN — log a warning and let the data through — regardless of the
    author's on_fail. It must now be routed through on_fail like any other
    rule failure: on_fail=abort aborts. Mirrors
    tests/test_executor/test_executor_assert.py's Spark equivalent."""
    rel = duckdb_con.sql("SELECT 1 AS a")

    def _boom(r):
        raise RuntimeError("bad user code")

    mod = _assert_module([{"type": "custom", "fn": "fake.mod.boom", "on_fail": "abort"}])
    with patch(
        "aqueduct.executor.duckdb_.assert_._load_custom_callable",
        return_value=_boom,
    ):
        with pytest.raises(
            AssertError, match=r"custom rule 'fake\.mod\.boom' raised: bad user code"
        ) as excinfo:
            execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert excinfo.value.rule_id == "custom_error"


def test_custom_rule_missing_fn_aborts_when_on_fail_abort(duckdb_con):
    """Regression (Pass G1): a custom rule with no `fn:` used to be silently
    skipped (pass-through) regardless of on_fail. Must now respect
    on_fail=abort."""
    rel = duckdb_con.sql("SELECT 1 AS a")
    mod = _assert_module([{"type": "custom", "on_fail": "abort"}])
    with pytest.raises(AssertError, match=r"custom rule missing fn path") as excinfo:
        execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert excinfo.value.rule_id == "custom_missing_fn"


def test_custom_rule_missing_fn_warns_when_on_fail_warn(duckdb_con, caplog):
    """on_fail=warn still lets the pipeline continue, but must log — the
    'skipped, no note' fail-open behavior is gone."""
    rel = duckdb_con.sql("SELECT 1 AS a")
    mod = _assert_module([{"type": "custom", "on_fail": "warn"}])
    passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert quarantine is None
    assert passing is rel
    assert "custom_missing_fn" in caplog.text
    assert "custom rule missing fn path" in caplog.text


# ── null_rate ─────────────────────────────────────────────────────────────


def test_null_rate_aborts_when_over_threshold(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1), (NULL), (NULL), (4)) t(email)")
    mod = _assert_module(
        [
            {
                "type": "null_rate",
                "column": "email",
                "max": 0.1,
                "fraction": 1.0,
                "on_fail": "abort",
            }
        ]
    )
    with pytest.raises(AssertError, match="null_rate"):
        execute_assert(mod, rel, duckdb_con, "r1", "bp")


def test_null_rate_passes_under_threshold(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1), (2), (3), (NULL)) t(email)")
    mod = _assert_module(
        [
            {
                "type": "null_rate",
                "column": "email",
                "max": 0.5,
                "fraction": 1.0,
                "on_fail": "abort",
            }
        ]
    )
    passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert quarantine is None


# ── spillway_rate ─────────────────────────────────────────────────────────


def test_spillway_rate_aborts_when_too_much_quarantined(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1), (-1), (-2)) t(a)")
    mod = _assert_module(
        [
            {"type": "sql_row", "expr": "a > 0", "on_fail": "quarantine"},
            {"type": "spillway_rate", "max": 0.1},
        ]
    )
    with pytest.raises(AssertError, match="spillway_rate"):
        execute_assert(mod, rel, duckdb_con, "r1", "bp")


def test_spillway_rate_passes_under_threshold(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1), (2), (-1)) t(a)")
    mod = _assert_module(
        [
            {"type": "sql_row", "expr": "a > 0", "on_fail": "quarantine"},
            {"type": "spillway_rate", "max": 0.9},
        ]
    )
    passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert quarantine is not None


# ── on_fail: warn / webhook / trigger_agent ──────────────────────────────


def test_on_fail_warn_does_not_raise(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM range(1) t(id)")
    mod = _assert_module([{"type": "min_rows", "min": 5, "on_fail": "warn"}])
    passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert passing is rel


def test_on_fail_webhook_fires_without_raising(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM range(1) t(id)")
    mod = _assert_module(
        [
            {
                "type": "min_rows",
                "min": 5,
                "on_fail": {"action": "webhook", "url": "https://example.invalid/hook"},
            }
        ]
    )
    with patch("aqueduct.infra.http._deliver_webhook_payload") as mock_deliver:
        execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert mock_deliver.called


def test_on_fail_trigger_agent_raises_with_flag(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM range(1) t(id)")
    mod = _assert_module([{"type": "min_rows", "min": 5, "on_fail": "trigger_agent"}])
    with pytest.raises(AssertError) as exc_info:
        execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert exc_info.value.trigger_agent is True


# ── no rules configured — pass-through ───────────────────────────────────


def test_no_rules_is_pass_through(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")
    mod = _assert_module([])
    passing, quarantine = execute_assert(mod, rel, duckdb_con, "r1", "bp")
    assert passing is rel
    assert quarantine is None


# ── end to end, driven through execute() (backs module.type.Assert) ──────


def test_module_type_assert_driven_through_execute_quarantine(duckdb_con, tmp_path):
    """Full Ingress -> Assert -> Egress(main) / Egress(spillway) pipeline —
    proves the WHOLE module type dispatches correctly through the executor
    loop, not just the handler function in isolation."""
    src_path = str(tmp_path / "src.parquet")
    duckdb_con.sql("SELECT * FROM (VALUES (1, 10), (2, -5), (3, 20)) t(id, amount)").write_parquet(
        src_path
    )
    main_out = str(tmp_path / "main.parquet")
    spill_out = str(tmp_path / "spill.parquet")

    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _assert_module(
            [
                {
                    "type": "sql_row",
                    "expr": "amount > 0",
                    "on_fail": "quarantine",
                    "error_type": "NegativeAmount",
                }
            ],
            id_="gate",
        ),
        _module("eg_main", "Egress", {"format": "parquet", "path": main_out, "mode": "overwrite"}),
        _module(
            "eg_spill", "Egress", {"format": "parquet", "path": spill_out, "mode": "overwrite"}
        ),
    )
    edges = (
        Edge(from_id="ing", to_id="gate", port="main"),
        Edge(from_id="gate", to_id="eg_main", port="main"),
        Edge(from_id="gate", to_id="eg_spill", port="spillway"),
    )
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )
    result = execute(manifest, duckdb_con, run_id="r_assert")
    assert result.status == ExecutionStatus.SUCCESS
    assert {r.module_id: r.status for r in result.module_results} == {
        "ing": "success",
        "gate": "success",
        "eg_main": "success",
        "eg_spill": "success",
    }
    assert sorted(r[0] for r in duckdb_con.read_parquet(main_out).fetchall()) == [1, 3]
    spill_rows = duckdb_con.read_parquet(spill_out).fetchall()
    assert len(spill_rows) == 1
    assert spill_rows[0][0] == 2


def test_module_type_assert_driven_through_execute_abort(duckdb_con, tmp_path):
    src_path = str(tmp_path / "src.parquet")
    duckdb_con.sql("SELECT * FROM range(2) t(id)").write_parquet(src_path)
    out_path = str(tmp_path / "out.parquet")

    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _assert_module([{"type": "min_rows", "min": 5, "on_fail": "abort"}], id_="gate"),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="gate", port="main"),
        Edge(from_id="gate", to_id="eg", port="main"),
    )
    manifest = Manifest(
        blueprint_id="bp", context={}, modules=modules, edges=edges, engine_config={}
    )
    result = execute(manifest, duckdb_con, run_id="r_assert_abort")
    assert result.status == ExecutionStatus.ERROR
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["gate"] == "error"
