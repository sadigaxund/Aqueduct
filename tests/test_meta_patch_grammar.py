"""Meta-test — the PatchSpec grammar's op set stays a closed, data-mutation-free
list (Phase 87 heal-as-PR precondition, 2026-08-24 design audit item "Smaller
items").

``VALID_PATCH_OPS`` (``aqueduct/patch/grammar.py``) is the canonical op list a
PatchSpec's discriminated union accepts. Every op mutates Blueprint/config
*shape* only — module config, labels, edges, context values, engine config,
retry policy, dependency declarations. None of them writes pipeline DATA (no
op reads/writes a dataset, a row, a file's contents, or any external system).
That precondition is what makes ``aqueduct patch pr`` safe to open on a
reviewer's behalf without a human having executed anything: applying a patch
can only ever change what a NEXT run would do, never data that already moved.

This asserts the op set EXACT-MATCH against a named list, not merely "does not
contain an obviously-dangerous name" — adding a new op (data-mutating or not)
fails this test and forces a conscious decision to update the list here,
rather than silently inheriting the zero-data-mutation guarantee.
"""

from __future__ import annotations

import typing

import pytest

from aqueduct.patch.grammar import VALID_PATCH_OPS, PatchOperation

pytestmark = pytest.mark.unit

# The exact op set as of Phase 87. Every one of these operates on Blueprint
# structure/config or engine config — never on pipeline data. Op names that
# would mutate data (e.g. a hypothetical "write_row", "delete_file",
# "execute_sql") do not belong here; adding one is exactly the case this test
# exists to force a conscious decision on.
_KNOWN_GOOD_OPS: frozenset[str] = frozenset(
    {
        "replace_module_config",
        "set_module_config_key",
        "replace_module_label",
        "insert_module",
        "remove_module",
        "replace_context_value",
        "add_probe",
        "replace_edge",
        "set_module_on_failure",
        "replace_retry_policy",
        "add_arcade_ref",
        "defer_to_human",
        "set_engine_config",
        "replace_macro",
        "declare_dependency",
    }
)

# Op-name substrings that would signal a data-mutating operation, had one
# been added. Not itself the assertion (the exact-match above already fails
# on ANY addition) — a documented cross-check so a reviewer reading this file
# sees explicitly what class of op the exact-match is defending against.
_DATA_MUTATION_SIGNALS: tuple[str, ...] = (
    "write",
    "delete",
    "execute",
    "insert_row",
    "drop_table",
    "truncate",
    "upload",
    "download",
)


def test_valid_patch_ops_is_exactly_the_known_good_list():
    """A future op addition must fail here — forcing a reviewer to confirm it
    does not cross into data mutation, not silently pass the grammar guard."""
    assert set(VALID_PATCH_OPS) == _KNOWN_GOOD_OPS


def test_valid_patch_ops_carries_no_data_mutation_signal():
    """Cross-check: none of the known-good op names itself reads as a
    data-mutating verb. Defends against a rename that reintroduces the class
    this grammar has always excluded."""
    offenders = [op for op in VALID_PATCH_OPS if any(sig in op for sig in _DATA_MUTATION_SIGNALS)]
    assert offenders == []


def test_valid_patch_ops_matches_the_discriminated_union():
    """`VALID_PATCH_OPS` must stay in sync with `PatchOperation`'s actual
    discriminator values — the prompt-facing list and the schema the parser
    enforces must never drift apart."""
    (union_type,) = typing.get_args(PatchOperation)[:1]
    op_models = typing.get_args(union_type)
    union_ops = {typing.get_args(model.model_fields["op"].annotation)[0] for model in op_models}
    assert union_ops == set(VALID_PATCH_OPS)


def test_falsifiability_known_good_list_would_catch_an_addition():
    """Prove the exact-match guard can actually fail (AGENTS.md: "a meta-test
    that guards CI can be unfalsifiable — prove it can fail"). A hypothetical
    grammar that added one more op would no longer equal the known-good set."""
    hypothetical = set(VALID_PATCH_OPS) | {"write_row"}
    assert hypothetical != _KNOWN_GOOD_OPS
