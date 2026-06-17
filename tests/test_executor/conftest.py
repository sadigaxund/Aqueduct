"""Skip all Spark executor tests when pyspark is not installed.

The executor test files import pyspark (directly or transitively through
aqueduct.executor.spark.*) at module level.  Without this guard the entire
test-executor directory fails collection with ``ModuleNotFoundError``
before pytest can even consider fixture-based skip logic.
"""

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed — install aqueduct-core[spark]")
