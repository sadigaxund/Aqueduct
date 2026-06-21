"""Column lineage: window-function OVER-clause columns are framing, not value.

Regression for ISSUE-035 — `row_number() OVER (ORDER BY id)` must NOT report `id`
as a source of the output, and `lag(price) OVER (ORDER BY dt)` reports `price`
only (the function argument), never `dt` (the ordering).
"""
from __future__ import annotations

import pytest

from aqueduct.compiler.lineage import _extract_sql_lineage

pytestmark = pytest.mark.unit


def _edges(sql, upstreams):
    return {(r["output_column"], r["source_table"], r["source_column"])
            for r in _extract_sql_lineage("ch", sql, upstreams)}


def test_row_number_has_no_value_source():
    e = _edges("SELECT *, row_number() OVER (ORDER BY id) AS rn FROM users", ["users"])
    # rn must NOT derive from id; it's expression-generated → "*"
    assert ("rn", "users", "id") not in e
    assert ("rn", "", "*") in e
    assert ("*", "users", "*") in e  # the SELECT * passthrough still works


def test_lag_reports_argument_not_ordering():
    e = _edges("SELECT lag(price) OVER (ORDER BY dt) AS prev FROM t", ["t"])
    assert ("prev", "t", "price") in e
    assert ("prev", "t", "dt") not in e          # dt is the ORDER BY → framing


def test_window_partition_and_order_excluded():
    e = _edges("SELECT sum(amt) OVER (PARTITION BY region ORDER BY dt) AS rt FROM t", ["t"])
    assert ("rt", "t", "amt") in e
    assert ("rt", "t", "region") not in e
    assert ("rt", "t", "dt") not in e


def test_plain_expression_unchanged():
    e = _edges("SELECT a + b AS c FROM t", ["t"])
    assert ("c", "t", "a") in e and ("c", "t", "b") in e
