"""ExecutionStatus vocabulary — serialization byte-identity (taxonomy class #7)."""

import pytest

from aqueduct.executor.models import ExecutionStatus

pytestmark = pytest.mark.unit


def test_members_and_serialized_values():
    assert {m.name for m in ExecutionStatus} == {"SUCCESS", "ERROR", "SKIPPED", "PATCHED"}
    # StrEnum: stored/JSON values must stay byte-identical to the old bare strings.
    assert ExecutionStatus.SUCCESS == "success"
    assert ExecutionStatus.ERROR == "error"
    assert ExecutionStatus.SKIPPED == "skipped"
    assert ExecutionStatus.PATCHED == "patched"
    assert str(ExecutionStatus.PATCHED) == "patched"
    assert f"{ExecutionStatus.PATCHED}" == "patched"


def test_patch_store_lifecycle_constants():
    from aqueduct.stores.object_store import PatchStore

    assert (PatchStore.PENDING, PatchStore.APPLIED, PatchStore.REJECTED) == (
        "pending", "applied", "rejected",
    )
