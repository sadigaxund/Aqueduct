"""Regression tests for `aqueduct.parser.parser._resolve_engine_block_raw`
(cross-engine remediation follow-up).

The defect: the non-`conf` branch used to call `engine_block.model_dump()`
with no arguments, which emits EVERY field the pydantic model declares —
including ones the Blueprint author never wrote, each carrying its default
(typically `None`). `DuckDBEngineBlockSchema` had zero fields when this
test file was first written, so the bug was inert there; it now carries
`memory_limit`/`threads` (2.54) — a Blueprint that sets only ONE of them
would produce `{"memory_limit": "8GB", "threads": None}` for
`Blueprint.engine_config["duckdb"]` without the fix, and
`resolve_session_engine_config`'s Blueprint-wins merge would let that
`None` silently clobber a real `aqueduct.yml`-level `threads` value the
Blueprint never touched.

`TestNonConfBranchExcludesUnsetFields`/`TestConfBranchUnaffected` below keep
proving the mechanism against a SYNTHETIC stand-in model (independent of
whatever fields the real schema happens to carry at any given time);
`TestRealDuckDBEngineBlockSchema` proves the identical guarantee against the
REAL `aqueduct.parser.schema.DuckDBEngineBlockSchema`, now that it has real
fields to prove it against.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ConfigDict

from aqueduct.parser.parser import _resolve_engine_block_raw
from aqueduct.parser.schema import DuckDBEngineBlockSchema

pytestmark = pytest.mark.unit


class _FakeDuckDBEngineBlockSchema(BaseModel):
    """Stand-in for a future `DuckDBEngineBlockSchema` that (like its
    `aqueduct.yml`-level `DuckDBEngineConfig` counterpart) declares session-
    affecting fields directly rather than nesting them under `conf:`."""

    model_config = ConfigDict(extra="forbid")

    memory_limit: str | None = None
    threads: int | None = None


class _FakeSparkEngineBlockSchema(BaseModel):
    """Stand-in for `SparkEngineBlockSchema` — the `conf:`-nesting shape."""

    model_config = ConfigDict(extra="forbid")

    conf: dict = {}


class TestNonConfBranchExcludesUnsetFields:
    """The bug: an engine block with fields NOT wrapped in `conf:`."""

    def test_nothing_explicitly_set_returns_empty_dict(self):
        """A Blueprint that doesn't mention this engine's block at all —
        every field is at its pydantic default — must produce `{}`, not a
        dict full of `None`s for the merge to accidentally clobber
        `aqueduct.yml` values with."""
        block = _FakeDuckDBEngineBlockSchema()
        assert _resolve_engine_block_raw(block) == {}

    def test_one_field_explicitly_set_returns_only_that_field(self):
        """A Blueprint that sets exactly ONE field must produce a dict
        containing ONLY that field — not the other declared field's
        default. This is the exact case that silently clobbers a real
        `aqueduct.yml` value in the merge if `model_dump()` (no args) is
        used instead of `model_dump(exclude_unset=True)`: pre-fix, this
        assertion produces
        `{"memory_limit": "8GB", "threads": None}` and fails."""
        block = _FakeDuckDBEngineBlockSchema(memory_limit="8GB")
        assert _resolve_engine_block_raw(block) == {"memory_limit": "8GB"}

    def test_both_fields_explicitly_set_returns_both(self):
        block = _FakeDuckDBEngineBlockSchema(memory_limit="8GB", threads=4)
        assert _resolve_engine_block_raw(block) == {"memory_limit": "8GB", "threads": 4}

    def test_field_explicitly_set_to_its_own_default_still_counts(self):
        """pydantic's `model_fields_set` tracks EXPLICIT assignment, not
        "differs from default" — setting a field to the same value as its
        default is still a deliberate author choice and must survive."""
        block = _FakeDuckDBEngineBlockSchema(memory_limit=None, threads=4)
        assert _resolve_engine_block_raw(block) == {"memory_limit": None, "threads": 4}


class TestConfBranchUnaffected:
    """Spark's `conf:` branch is a free-form dict — it only ever contains
    what the author typed regardless of `exclude_unset`, and must keep
    working exactly as before this fix."""

    def test_conf_dict_passed_through_verbatim(self):
        block = _FakeSparkEngineBlockSchema(conf={"spark.sql.shuffle.partitions": "200"})
        assert _resolve_engine_block_raw(block) == {"spark.sql.shuffle.partitions": "200"}

    def test_empty_conf_dict_stays_empty(self):
        block = _FakeSparkEngineBlockSchema()
        assert _resolve_engine_block_raw(block) == {}


class TestRealDuckDBEngineBlockSchema:
    """Same guarantees as `TestNonConfBranchExcludesUnsetFields`, against the
    REAL `DuckDBEngineBlockSchema` (2.54: `memory_limit`/`threads`) rather
    than the synthetic stand-in above."""

    def test_nothing_explicitly_set_returns_empty_dict(self):
        block = DuckDBEngineBlockSchema()
        assert _resolve_engine_block_raw(block) == {}

    def test_one_field_explicitly_set_returns_only_that_field(self):
        block = DuckDBEngineBlockSchema(memory_limit="8GB")
        assert _resolve_engine_block_raw(block) == {"memory_limit": "8GB"}

    def test_both_fields_explicitly_set_returns_both(self):
        block = DuckDBEngineBlockSchema(memory_limit="8GB", threads=4)
        assert _resolve_engine_block_raw(block) == {"memory_limit": "8GB", "threads": 4}

    def test_field_explicitly_set_to_its_own_default_still_counts(self):
        block = DuckDBEngineBlockSchema(memory_limit=None, threads=4)
        assert _resolve_engine_block_raw(block) == {"memory_limit": None, "threads": 4}

    def test_deployment_fields_rejected_extra_forbid(self):
        """`database_path`/`s3_*`/`extension_repository` are deliberately
        NOT on the Blueprint-level schema (deployment/connection concerns,
        never a per-pipeline override) — `extra="forbid"` rejects them at
        parse time rather than silently accepting and dropping them."""
        with pytest.raises(Exception):
            DuckDBEngineBlockSchema(database_path="/tmp/x.duckdb")
