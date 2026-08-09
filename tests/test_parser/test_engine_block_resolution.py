"""Regression tests for `aqueduct.parser.parser._resolve_engine_block_raw`
(cross-engine remediation follow-up).

The defect: the non-`conf` branch used to call `engine_block.model_dump()`
with no arguments, which emits EVERY field the pydantic model declares —
including ones the Blueprint author never wrote, each carrying its default
(typically `None`). `DuckDBEngineBlockSchema` has zero fields today, so the
bug is currently inert, but the moment it gains fields (the next task in
this phase) a Blueprint that sets only ONE of them would produce
`{"memory_limit": "8GB", "threads": None, ...}` for `Blueprint.engine_config
["duckdb"]`, and `resolve_session_engine_config`'s Blueprint-wins merge
would let those `None`s silently clobber real `aqueduct.yml`-level values
the Blueprint never touched.

Because the real `DuckDBEngineBlockSchema` has no fields to prove this
against, these tests build a SYNTHETIC pydantic model with the shape the
next task will give it (a couple of `X | None = None` fields) and call the
fixed function directly — proving the mechanism (`model_dump(exclude_unset
=True)`, keyed off `model_fields_set`) rather than today's accidentally-safe
empty schema.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ConfigDict

from aqueduct.parser.parser import _resolve_engine_block_raw

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
