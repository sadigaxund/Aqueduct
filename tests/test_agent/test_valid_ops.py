"""Tests for LLM-facing operation lists.

Ensures that the newly added ``replace_macro`` operation is exposed to the model
via the ``_VALID_OPS`` constant used in the system prompt.
"""

import pytest

from aqueduct.agent.prompts import _VALID_OPS


def test_valid_ops_contains_replace_macro():
    """_VALID_OPS should include the ``replace_macro`` opcode."""
    assert "replace_macro" in _VALID_OPS
