"""Tests for aqueduct exit codes."""
import pytest

pytestmark = pytest.mark.unit

def test_exit_codes_exposed():
    from aqueduct import exit_codes
    assert hasattr(exit_codes, "SUCCESS")
    assert exit_codes.SUCCESS == 0
    assert exit_codes.CONFIG_ERROR == 1
    assert exit_codes.DATA_OR_RUNTIME == 2
    assert exit_codes.HEAL_PENDING == 3
    assert exit_codes.VALIDATION_GATE == 4
    assert exit_codes.USAGE_ERROR == 5

def test_exit_codes_in_all():
    from aqueduct.exit_codes import __all__
    assert set(__all__) == {
        "SUCCESS", "CONFIG_ERROR", "DATA_OR_RUNTIME", 
        "HEAL_PENDING", "VALIDATION_GATE", "USAGE_ERROR"
    }
