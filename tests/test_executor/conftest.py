"""Skip all Spark executor tests when pyspark is not installed.

The executor test files import pyspark (directly or transitively through
aqueduct.executor.spark.*) at module level.  Without this guard the entire
test-executor directory fails collection with ``ModuleNotFoundError``
before pytest can even consider fixture-based skip logic.

``pytest.importorskip`` is NOT safe to call at conftest.py module level:
conftest loading happens in an earlier pytest phase than test-module
collection, and a ``Skipped`` raised there is NOT caught the way it is for
an actual test file — it propagates as a fatal error and aborts the entire
pytest invocation (crashed every job that collects this directory without
pyspark installed, e.g. the `coverage` CI job). ``collect_ignore_glob`` is
pytest's supported mechanism for conditionally skipping a whole directory's
collection before any file in it is imported.
"""

import importlib.util

collect_ignore_glob = ["*"] if importlib.util.find_spec("pyspark") is None else []
