"""Phase 88 Domain 6 — `heal_attempts.defer_reason` schema-evolution tests.

`CREATE TABLE IF NOT EXISTS` never patches an existing table (see the
schema-evolution rule in aqueduct/surveyor/ddl.py), so a new column must
land in BOTH the CREATE DDL (fresh installs) AND the migrations tuple
(existing installs) as an idempotent `ALTER TABLE ... ADD COLUMN IF NOT
EXISTS`. These tests exercise both paths directly against DuckDB.
"""

from __future__ import annotations

import re

import pytest

from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
from aqueduct.surveyor.ddl import (
    _HEAL_ATTEMPTS_DDL,
    _HEAL_ATTEMPTS_MIGRATIONS,
)

pytestmark = pytest.mark.unit


def test_fresh_create_table_includes_defer_reason(tmp_path):
    obs = DuckDBObservabilityStore(tmp_path / "obs.db")
    with obs.connect() as cur:
        cur.execute(_HEAL_ATTEMPTS_DDL)
        cols = {row[0] for row in cur.execute("DESCRIBE heal_attempts").fetchall()}
    assert "defer_reason" in cols


def test_migration_adds_defer_reason_to_pre_existing_table(tmp_path):
    """Simulate a pre-upgrade database: a `heal_attempts` table that predates
    `defer_reason` (built from the CREATE DDL string with the column line
    stripped out, i.e. the schema Phase 88 found on disk). Running the
    migrations tuple must add the column without erroring."""
    obs = DuckDBObservabilityStore(tmp_path / "obs.db")
    # Drop the `engine` column's trailing comma (making it the last column
    # again) and strip everything from the defer_reason comment block
    # through the closing `);` — regex, not a literal string match, so this
    # test doesn't rot every time an unrelated comment line above it wraps.
    pre_upgrade_ddl = re.sub(
        r",\s*\n\s*-- Phase 88.*?\n\s*defer_reason\s+VARCHAR\s*\n\);",
        "\n);",
        _HEAL_ATTEMPTS_DDL,
        flags=re.DOTALL,
    )
    assert "defer_reason" not in pre_upgrade_ddl  # sanity: the strip actually worked
    assert pre_upgrade_ddl != _HEAL_ATTEMPTS_DDL
    with obs.connect() as cur:
        cur.execute(pre_upgrade_ddl)
        cols_before = {row[0] for row in cur.execute("DESCRIBE heal_attempts").fetchall()}
        assert "defer_reason" not in cols_before

        for migration in _HEAL_ATTEMPTS_MIGRATIONS:
            cur.execute(migration)

        cols_after = {row[0] for row in cur.execute("DESCRIBE heal_attempts").fetchall()}
        assert "defer_reason" in cols_after

        # And the migrated column is actually usable end to end.
        cur.execute(
            "INSERT INTO heal_attempts (id, run_id, attempt_num, recorded_at, defer_reason) "
            "VALUES ('x', 'run1', 1, '2026-08-23T00:00:00Z', 'insufficient_context')"
        )
        row = cur.execute("SELECT defer_reason FROM heal_attempts WHERE id = 'x'").fetchone()
        assert row[0] == "insufficient_context"


def test_migrations_are_idempotent(tmp_path):
    """Running the full migrations tuple twice in a row (e.g. two Surveyor
    `.start()` calls against the same store) must not raise."""
    obs = DuckDBObservabilityStore(tmp_path / "obs.db")
    with obs.connect() as cur:
        cur.execute(_HEAL_ATTEMPTS_DDL)
        for migration in _HEAL_ATTEMPTS_MIGRATIONS:
            cur.execute(migration)
        for migration in _HEAL_ATTEMPTS_MIGRATIONS:
            cur.execute(migration)  # second pass — must not error
        cols = {row[0] for row in cur.execute("DESCRIBE heal_attempts").fetchall()}
    assert "defer_reason" in cols
