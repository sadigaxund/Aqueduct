"""Sync constants — verify duplicate frozensets / column-name sets are consistent.

If a new format, error column, or meta key is added in one place but not another,
the mismatch is caught here rather than as a silent production breakage.
"""

from __future__ import annotations

import pytest

# aqueduct.executor.spark.* pulls in pyspark at package-import time
# (aqueduct/executor/spark/__init__.py) regardless of which submodule you
# want — a hard ModuleNotFoundError here is a fatal collection error, so
# this must be an explicit importorskip before the import, not just a marker.
pytest.importorskip("pyspark", reason="pyspark not installed — install aqueduct-core[spark]")
from aqueduct.executor.spark.error_columns import (  # noqa: E402
    AQ_ERROR_MODULE,
    AQ_ERROR_MSG,
    AQ_ERROR_RULE,
    AQ_ERROR_TS,
    AQ_ERROR_TYPE,
)
from aqueduct.patch.grammar import PATCH_META_KEY  # noqa: E402

pytestmark = [pytest.mark.unit, pytest.mark.spark]


def test_aq_error_columns_match_expected():
    assert AQ_ERROR_MODULE == "_aq_error_module"
    assert AQ_ERROR_TYPE == "_aq_error_type"
    assert AQ_ERROR_MSG == "_aq_error_msg"
    assert AQ_ERROR_TS == "_aq_error_ts"
    assert AQ_ERROR_RULE == "_aq_error_rule"


def test_patch_meta_key_matches_expected():
    assert PATCH_META_KEY == "_aq_meta"
