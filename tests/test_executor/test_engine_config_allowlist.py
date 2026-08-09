"""Per-engine healing config-key allowlist (``engine_config_allowlist.yml``).

The Blueprint-level ``engine.<name>`` block carries no capability leaf —
``capability_leaves.py``'s ``_SCHEMA_BLOCKS`` never included
``SparkEngineBlockSchema``/``DuckDBEngineBlockSchema`` — so this allowlist is
the only thing standing between the healing agent's ``set_engine_config`` op
and an arbitrary engine key. These tests pin:

  - the loader's hard validation (malformed entries raise
    ``EngineConfigAllowlistError`` naming the offending pattern),
  - the free-form-bag-vs-typed-fields addressing split (glob patterns for
    Spark's ``conf`` bag, exact+type-checked field names for DuckDB),
  - the presence guard (an absent file is not the same as an explicit empty
    one), and
  - that every SHIPPED file (``aqueduct/executor/spark/
    engine_config_allowlist.yml``, ``aqueduct/executor/duckdb_/
    engine_config_allowlist.yml``) loads clean through the real validator —
    a typo in the data breaks this test, not a live heal.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aqueduct.errors import AqueductError, EngineConfigAllowlistError
from aqueduct.executor.engine_config_allowlist import (
    DECLARATION_FILENAME,
    AllowlistEntry,
    EnumConstraint,
    RangeConstraint,
    _size_to_bytes,
    check_presence,
    discover_allowlist_paths,
    discover_registered_engines,
    load_allowlist,
)

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_SPARK_PATH = _REPO / "aqueduct" / "executor" / "spark" / DECLARATION_FILENAME
_DUCKDB_PATH = _REPO / "aqueduct" / "executor" / "duckdb_" / DECLARATION_FILENAME


def _write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / DECLARATION_FILENAME
    p.write_text(text, encoding="utf-8")
    return p


# ── error taxonomy ───────────────────────────────────────────────────────────


def test_engine_config_allowlist_error_is_a_sibling_not_a_subclass():
    """A FOURTH capability-adjacent error type, deliberately NOT derived from
    (or a parent of) the three existing ones — different content (a
    key/type/range triple, not a leaf verdict), different fix (edit the
    entry directly — no sync/scaffold tooling exists for this file), and a
    different future consumer (a patch gate, not the compile-time capability
    check). A handler for one must not silently swallow this one."""
    from aqueduct.errors import CapabilityDeclarationError, CapabilityScopeError, EnginePluginError

    assert issubclass(EngineConfigAllowlistError, AqueductError)
    assert not issubclass(EngineConfigAllowlistError, CapabilityDeclarationError)
    assert not issubclass(CapabilityDeclarationError, EngineConfigAllowlistError)
    assert not issubclass(EngineConfigAllowlistError, CapabilityScopeError)
    assert not issubclass(EngineConfigAllowlistError, EnginePluginError)


# ── the size literal parser ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    "literal,expected_bytes",
    [
        ("1024", 1024),
        ("1k", 1024),
        ("4g", 4 * 1024**3),
        ("512MB", 512 * 1024**2),
        ("2.5g", int(2.5 * 1024**3)),
        ("256GB", 256 * 1024**3),
    ],
)
def test_size_literal_parses(literal, expected_bytes):
    assert _size_to_bytes(literal) == pytest.approx(expected_bytes)


def test_size_literal_rejects_garbage():
    with pytest.raises(ValueError):
        _size_to_bytes("not-a-size")


# ── presence guard ───────────────────────────────────────────────────────────


def test_missing_file_raises_engine_config_allowlist_error(tmp_path):
    with pytest.raises(EngineConfigAllowlistError) as exc_info:
        load_allowlist(tmp_path / DECLARATION_FILENAME, "spark")
    assert "no" in str(exc_info.value) and DECLARATION_FILENAME in str(exc_info.value)
    assert exc_info.value.engine == "spark"


def test_explicit_empty_file_loads_clean(tmp_path):
    """An explicit `entries: []` is a real, meaningful, human-made decision —
    distinct from the file being absent (see test above)."""
    p = _write(tmp_path, "engine: spark\nentries: []\n")
    allowlist = load_allowlist(p, "spark")
    assert allowlist.entries == ()


def test_check_presence_passes_for_the_real_install():
    """Both shipped engines (spark, duckdb) are registered and both ship a
    file — this must not raise against the real, installed entry points."""
    check_presence()


def test_check_presence_names_missing_engine(monkeypatch):
    import aqueduct.executor.engine_config_allowlist as eca

    monkeypatch.setattr(
        eca,
        "discover_allowlist_paths",
        lambda: {"spark": Path("/x/engine_config_allowlist.yml"), "duckdb": None},
    )
    with pytest.raises(EngineConfigAllowlistError) as exc_info:
        eca.check_presence()
    assert "duckdb" in exc_info.value.keys
    assert "spark" not in exc_info.value.keys


def test_discover_registered_engines_finds_both_real_engines():
    found = discover_registered_engines()
    assert "spark" in found
    assert "duckdb" in found


def test_discover_allowlist_paths_resolves_without_importing_engine_modules():
    """Discovery must work via find_spec only — never by importing
    aqueduct.executor.spark.engine / aqueduct.executor.duckdb_.engine, which
    would pull in pyspark / register capabilities as a side effect.

    Guards against the two ENGINE modules specifically (not bare
    ``pyspark``, which some unrelated fixture elsewhere in the session may
    have already imported before this test runs — that import happened for
    reasons unconnected to ``discover_allowlist_paths``, so asserting on
    engine-module identity is the precise claim, not a proxy for it).
    """
    import sys

    for mod in ("aqueduct.executor.spark.engine", "aqueduct.executor.duckdb_.engine"):
        assert mod not in sys.modules, f"{mod} was already imported by an earlier test"

    paths = discover_allowlist_paths()

    assert "aqueduct.executor.spark.engine" not in sys.modules
    assert "aqueduct.executor.duckdb_.engine" not in sys.modules
    assert paths["spark"] == _SPARK_PATH.resolve()
    assert paths["duckdb"] == _DUCKDB_PATH.resolve()


# ── malformed-file hard validation ───────────────────────────────────────────


@pytest.mark.parametrize(
    "text,match",
    [
        ("not a mapping", "must be a mapping"),
        ("entries: []", "must be a mapping with 'engine:'"),
        ("engine: spark", "must be a mapping with 'engine:'"),
        ("engine: duckdb\nentries: []\n", "declares engine 'duckdb', expected 'spark'"),
        ("engine: spark\nentries: not-a-list\n", "must be a list"),
        (
            "engine: spark\nentries:\n  - type: int\n",
            "needs a 'pattern' and a 'type'",
        ),
        (
            "engine: spark\nentries:\n  - pattern: spark.sql.shuffle.partitions\n",
            "needs a 'pattern' and a 'type'",
        ),
        (
            "engine: spark\nentries:\n  - pattern: ''\n    type: int\n",
            "non-empty string",
        ),
        (
            "engine: spark\nentries:\n"
            "  - pattern: spark.sql.shuffle.partitions\n    type: bogus\n",
            "invalid type",
        ),
        (
            "engine: spark\nentries:\n"
            "  - pattern: spark.sql.shuffle.partitions\n    type: int\n"
            "    range: [1, 10]\n    enum: [1, 2]\n",
            "declares both 'range' and 'enum'",
        ),
        (
            "engine: spark\nentries:\n"
            "  - pattern: spark.sql.adaptive.enabled\n    type: bool\n"
            "    range: [true, false]\n",
            "does not support one",
        ),
        (
            "engine: spark\nentries:\n"
            "  - pattern: spark.sql.shuffle.partitions\n    type: int\n"
            "    range: [1, 2, 3]\n",
            "2-element",
        ),
        (
            "engine: spark\nentries:\n"
            "  - pattern: spark.sql.shuffle.partitions\n    type: int\n"
            "    range: [10, 1]\n",
            "exceeds its max",
        ),
        (
            "engine: spark\nentries:\n"
            "  - pattern: spark.executor.memory\n    type: size\n"
            "    range: ['64g', '1m']\n",
            "exceeds its max",
        ),
        (
            "engine: spark\nentries:\n"
            "  - pattern: spark.executor.memory\n    type: size\n"
            "    range: ['not-a-size', '1g']\n",
            "not a valid",
        ),
        (
            "engine: spark\nentries:\n"
            "  - pattern: spark.sql.shuffle.partitions\n    type: int\n"
            "    enum: [1, 2, 'x']\n",
            "is not a",
        ),
        (
            "engine: spark\nentries:\n"
            "  - pattern: spark.sql.shuffle.partitions\n    type: int\n"
            "    enum: []\n",
            "non-empty list",
        ),
        (
            "engine: spark\nentries:\n"
            "  - pattern: spark.sql.shuffle.partitions\n    type: int\n"
            "    range: [1, 10]\n"
            "  - pattern: spark.sql.shuffle.partitions\n    type: int\n"
            "    range: [1, 20]\n",
            "duplicate entry",
        ),
    ],
)
def test_malformed_spark_entry_raises(tmp_path, text, match):
    p = _write(tmp_path, text)
    with pytest.raises(EngineConfigAllowlistError, match=match):
        load_allowlist(p, "spark")


def test_glob_pattern_rejected_for_typed_field_engine(tmp_path):
    p = _write(tmp_path, "engine: duckdb\nentries:\n  - pattern: 'mem*'\n    type: size\n")
    with pytest.raises(EngineConfigAllowlistError, match="declares typed fields"):
        load_allowlist(p, "duckdb")


def test_unknown_field_rejected_for_typed_field_engine(tmp_path):
    p = _write(
        tmp_path,
        "engine: duckdb\nentries:\n  - pattern: database_path\n    type: str\n",
    )
    with pytest.raises(EngineConfigAllowlistError, match="not a declared field"):
        load_allowlist(p, "duckdb")


def test_type_mismatch_against_pydantic_field_rejected(tmp_path):
    """DuckDBEngineBlockSchema.threads is `int | None` — declaring it `str`
    in the allowlist must be caught at load time, not silently accepted."""
    p = _write(tmp_path, "engine: duckdb\nentries:\n  - pattern: threads\n    type: str\n")
    with pytest.raises(EngineConfigAllowlistError, match="typed"):
        load_allowlist(p, "duckdb")


def test_no_engine_block_schema_rejected(tmp_path):
    p = _write(tmp_path, "engine: trino\nentries: []\n")
    with pytest.raises(EngineConfigAllowlistError, match="no engine.<name> Blueprint block"):
        load_allowlist(p, "trino")


# ── free-form bag matching (Spark) ───────────────────────────────────────────


def test_glob_entry_matches_family():
    entry = AllowlistEntry(pattern="spark.sql.adaptive.*.enabled", value_type="bool")
    assert entry.matches("spark.sql.adaptive.skewJoin.enabled")
    assert entry.matches("spark.sql.adaptive.coalescePartitions.enabled")
    assert not entry.matches("spark.sql.adaptive.enabled")
    assert not entry.matches("spark.sql.shuffle.partitions")


def test_exact_entry_matches_only_itself():
    entry = AllowlistEntry(pattern="spark.sql.shuffle.partitions", value_type="int")
    assert entry.matches("spark.sql.shuffle.partitions")
    assert not entry.matches("spark.sql.shuffle.partitionsX")


# ── the two shipped files, through the REAL validator ────────────────────────


def test_shipped_spark_allowlist_loads_clean():
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    assert allowlist.is_free_form is True
    assert len(allowlist.entries) > 0
    patterns = {e.pattern for e in allowlist.entries}
    assert "spark.sql.shuffle.partitions" in patterns
    assert "spark.sql.autoBroadcastJoinThreshold" in patterns


def test_shipped_spark_allowlist_serializer_is_a_closed_enum():
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    (entry,) = [e for e in allowlist.entries if e.pattern == "spark.serializer"]
    assert isinstance(entry.constraint, EnumConstraint)
    assert "org.apache.spark.serializer.KryoSerializer" in entry.constraint.values
    assert entry.value_type == "str"


def test_shipped_spark_allowlist_excludes_redirect_and_credential_keys():
    """The excluded families named in the file's own comments must not
    accidentally be covered by a broader glob some other entry declares."""
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    dangerous = [
        "spark.master",
        "spark.submit.deployMode",
        "spark.kubernetes.namespace",
        "spark.yarn.queue",
        "spark.hadoop.fs.s3a.access.key",
        "spark.hadoop.fs.s3a.secret.key",
        "spark.ssl.enabled",
        "spark.authenticate",
    ]
    for key in dangerous:
        assert not any(e.matches(key) for e in allowlist.entries), key


def test_shipped_spark_allowlist_max_result_size_excludes_unlimited():
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    (entry,) = [e for e in allowlist.entries if e.pattern == "spark.driver.maxResultSize"]
    assert isinstance(entry.constraint, RangeConstraint)
    assert _size_to_bytes(entry.constraint.minimum) > 0


def test_shipped_duckdb_allowlist_loads_clean():
    allowlist = load_allowlist(_DUCKDB_PATH, "duckdb")
    assert allowlist.is_free_form is False
    patterns = {e.pattern for e in allowlist.entries}
    assert patterns == {"memory_limit", "threads"}


def test_shipped_duckdb_allowlist_threads_range_matches_pydantic_gt_zero():
    from aqueduct.parser.schema import DuckDBEngineBlockSchema

    allowlist = load_allowlist(_DUCKDB_PATH, "duckdb")
    (entry,) = [e for e in allowlist.entries if e.pattern == "threads"]
    assert isinstance(entry.constraint, RangeConstraint)
    assert entry.constraint.minimum >= 1
    # threads carries `gt=0` in the pydantic model; the allowlist's floor
    # must not permit a value the schema itself would reject.
    field_info = DuckDBEngineBlockSchema.model_fields["threads"]
    gt = next((m.gt for m in field_info.metadata if hasattr(m, "gt")), None)
    if gt is not None:
        assert entry.constraint.minimum > gt


@pytest.mark.parametrize("engine,path", [("spark", _SPARK_PATH), ("duckdb", _DUCKDB_PATH)])
def test_every_shipped_file_loads_through_the_real_validator(engine, path):
    """The load-bearing regression test: a typo in either data file (bad
    type, inverted range, unknown field, glob on a typed engine, …) fails
    THIS test — never surfaces mid-heal, since nothing consumes this file
    at heal time yet."""
    allowlist = load_allowlist(path, engine)
    assert allowlist.engine == engine
