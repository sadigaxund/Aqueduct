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
    DenyEntry,
    EngineConfigAllowlist,
    EnumConstraint,
    RangeConstraint,
    _glob_canary,
    _size_to_bytes,
    check_presence,
    discover_allowlist_paths,
    discover_registered_engines,
    evaluate_set_engine_config,
    load_allowlist,
    validate_against_deny,
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


def test_shipped_spark_allowlist_max_result_size_carries_no_invented_range():
    """Part 1: no magnitude range survives — the entry is shape-only."""
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    (entry,) = [e for e in allowlist.entries if e.pattern == "spark.driver.maxResultSize"]
    assert entry.constraint is None
    assert entry.value_type == "size"


def test_shipped_spark_allowlist_max_result_size_excludes_unlimited_via_deny():
    """Part 2: the `0` = unlimited exclusion moved from a range floor to a
    scoped deny_values row — still enforced, now structurally."""
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    (entry,) = [
        d for d in allowlist.deny_entries if d.pattern == "spark.driver.maxResultSize"
    ]
    assert entry.deny_values is not None
    assert 0 in entry.deny_values
    assert "0" in entry.deny_values


def test_shipped_duckdb_allowlist_loads_clean():
    allowlist = load_allowlist(_DUCKDB_PATH, "duckdb")
    assert allowlist.is_free_form is False
    patterns = {e.pattern for e in allowlist.entries}
    assert patterns == {"memory_limit", "threads"}


def test_shipped_duckdb_allowlist_carries_no_invented_range():
    """Part 1: `["128MB","256GB"]`/`[1,256]` were invented magnitudes with no
    real machine to derive a bound from — both entries are shape-only now."""
    allowlist = load_allowlist(_DUCKDB_PATH, "duckdb")
    for entry in allowlist.entries:
        assert entry.constraint is None, f"{entry.pattern!r} still carries a constraint"


@pytest.mark.parametrize("engine,path", [("spark", _SPARK_PATH), ("duckdb", _DUCKDB_PATH)])
def test_every_shipped_file_loads_through_the_real_validator(engine, path):
    """The load-bearing regression test: a typo in either data file (bad
    type, inverted range, unknown field, glob on a typed engine, …) fails
    THIS test — never surfaces mid-heal, since nothing consumes this file
    at heal time yet."""
    allowlist = load_allowlist(path, engine)
    assert allowlist.engine == engine


# ── DenyEntry.matches / .forbids ─────────────────────────────────────────────


def test_deny_entry_absolute_ban_forbids_any_value():
    entry = DenyEntry(pattern="spark.master", reason="x")
    assert entry.matches("spark.master")
    assert not entry.matches("spark.mastery")
    assert entry.forbids()
    assert entry.forbids("spark://x:7077")


def test_deny_entry_scoped_value_ban_forbids_only_listed_values():
    entry = DenyEntry(pattern="spark.driver.maxResultSize", reason="x", deny_values=("0", 0))
    assert entry.matches("spark.driver.maxResultSize")
    assert entry.forbids(0)
    assert entry.forbids("0")
    assert not entry.forbids("4g")
    assert not entry.forbids(None)


def test_deny_entry_glob_matches_family():
    entry = DenyEntry(pattern="spark.kubernetes.*", reason="x")
    assert entry.matches("spark.kubernetes.namespace")
    assert not entry.matches("spark.kubernetes")


# ── canary derivation ────────────────────────────────────────────────────────


def test_glob_canary_returns_exact_pattern_unchanged():
    assert _glob_canary("spark.master") == "spark.master"
    assert _glob_canary("database_path") == "database_path"


def test_glob_canary_substitutes_wildcards():
    assert _glob_canary("spark.kubernetes.*") == "spark.kubernetes.x"
    assert _glob_canary("*.jars.repositories") == "x.jars.repositories"
    assert _glob_canary("spark.*.extraJavaOptions") == "spark.x.extraJavaOptions"
    assert _glob_canary("spark.authenticate*") == "spark.authenticatex"


# ── deny layer: malformed shape ──────────────────────────────────────────────


@pytest.mark.parametrize(
    "text,match",
    [
        ("engine: spark\nentries: []\ndeny: not-a-list\n", "'deny:' must be a list"),
        (
            "engine: spark\nentries: []\ndeny:\n  - reason: x\n",
            "needs a 'pattern' and a 'reason'",
        ),
        (
            "engine: spark\nentries: []\ndeny:\n  - pattern: spark.master\n",
            "needs a 'pattern' and a 'reason'",
        ),
        (
            "engine: spark\nentries: []\ndeny:\n  - pattern: ''\n    reason: x\n",
            "non-empty string",
        ),
        (
            "engine: spark\nentries: []\ndeny:\n  - pattern: spark.master\n    reason: ''\n",
            "non-empty string",
        ),
        (
            "engine: spark\nentries: []\ndeny:\n"
            "  - pattern: spark.driver.maxResultSize\n    reason: x\n    deny_values: []\n",
            "non-empty list",
        ),
    ],
)
def test_malformed_deny_entry_raises(tmp_path, text, match):
    p = _write(tmp_path, text)
    with pytest.raises(EngineConfigAllowlistError, match=match):
        load_allowlist(p, "spark")


# ── deny layer: TEST 1 — allow/deny overlap caught at load time ─────────────


def test_absolute_ban_deny_overlapping_an_exact_allow_entry_raises_at_load(tmp_path):
    p = _write(
        tmp_path,
        "engine: spark\nentries:\n"
        "  - pattern: spark.master\n    type: str\n"
        "deny:\n"
        "  - pattern: spark.master\n    reason: redirects where work runs\n",
    )
    with pytest.raises(EngineConfigAllowlistError, match="reachable through allow"):
        load_allowlist(p, "spark")


def test_absolute_ban_deny_overlapping_a_glob_allow_entry_raises_at_load(tmp_path):
    """The glob-family case: an allow row broad enough to re-admit a whole
    denied family must be caught via the deny pattern's own canary."""
    p = _write(
        tmp_path,
        "engine: spark\nentries:\n"
        "  - pattern: 'spark.kubernetes.*'\n    type: str\n"
        "deny:\n"
        "  - pattern: 'spark.kubernetes.*'\n    reason: cluster-placement family\n",
    )
    with pytest.raises(EngineConfigAllowlistError, match="reachable through allow"):
        load_allowlist(p, "spark")


def test_scoped_value_deny_is_exempt_from_the_overlap_check(tmp_path):
    """A `deny_values`-scoped row is MEANT to share a pattern with an allow
    entry (narrowing it, not banning it) — this must load clean."""
    p = _write(
        tmp_path,
        "engine: spark\nentries:\n"
        "  - pattern: spark.driver.maxResultSize\n    type: size\n"
        "deny:\n"
        "  - pattern: spark.driver.maxResultSize\n"
        "    deny_values: ['0', 0]\n    reason: 0 means unlimited\n",
    )
    allowlist = load_allowlist(p, "spark")
    assert len(allowlist.deny_entries) == 1
    assert allowlist.deny_entries[0].deny_values == ("0", 0)


def test_no_overlap_in_the_two_shipped_files_by_construction():
    """Loading either shipped file at all (via the fixtures used throughout
    this module) already proves this, since the overlap check runs inside
    `load_allowlist` — this test names the property explicitly."""
    spark = load_allowlist(_SPARK_PATH, "spark")
    duckdb = load_allowlist(_DUCKDB_PATH, "duckdb")
    for allowlist in (spark, duckdb):
        for d in allowlist.deny_entries:
            if d.deny_values is not None:
                continue
            canary = _glob_canary(d.pattern)
            assert not any(e.matches(canary) for e in allowlist.entries), (
                allowlist.engine,
                d.pattern,
            )


# ── deny layer: TEST 2 — family freeze ───────────────────────────────────────

# Hard-coded from the originating task's spec, deliberately NOT read back
# from the shipped YAML — this is the "prose became code" guarantee.
# Deleting a deny row (or narrowing a glob) breaks this test, naming the
# family, instead of silently losing coverage.
_SPARK_DENY_FAMILIES: dict[str, list[str]] = {
    "placement": [
        "spark.master",
        "spark.submit.*",
        "spark.kubernetes.*",
        "spark.yarn.*",
        "spark.mesos.*",
    ],
    "credentials_and_endpoints": [
        "spark.hadoop.*",
        "*.jars.repositories",
    ],
    "safety_disable": [
        "spark.ssl.*",
        "spark.network.crypto.*",
        "spark.authenticate*",
        "spark.driver.maxResultSize",  # value-deny (0 = unlimited)
    ],
    "code_loading": [
        "spark.jars",
        "spark.jars.packages",
        "spark.files",
        "spark.submit.pyFiles",
        "spark.plugins",
        "spark.sql.extensions",
        "spark.driver.extraClassPath",
        "spark.executor.extraClassPath",
        "spark.driver.extraJavaOptions",
        "spark.executor.extraJavaOptions",
        "spark.driver.extraLibraryPath",
        "spark.executor.extraLibraryPath",
    ],
}


@pytest.mark.parametrize("family", sorted(_SPARK_DENY_FAMILIES))
def test_spark_deny_family_is_covered_by_a_shipped_deny_entry(family):
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    for member in _SPARK_DENY_FAMILIES[family]:
        canary = _glob_canary(member)
        assert any(d.matches(canary) for d in allowlist.deny_entries), (
            f"family {family!r} member {member!r} (canary {canary!r}) is not "
            "covered by any shipped Spark deny entry"
        )


_DUCKDB_DENY_FIELDS = ["database_path", "extension_repository", "s3_endpoint"]


@pytest.mark.parametrize("field", _DUCKDB_DENY_FIELDS)
def test_duckdb_deny_guards_the_deployment_connection_fields(field):
    allowlist = load_allowlist(_DUCKDB_PATH, "duckdb")
    assert any(d.matches(field) for d in allowlist.deny_entries), field


# ── deny layer: TEST 3 — a synthetic extension entry cannot cross it ────────


def test_validate_against_deny_refuses_a_synthetic_extension_entry_for_spark_master():
    """Pinned NOW against the validation function even though the operator-
    extension config surface does not exist yet — this is the contract that
    surface must be built under."""
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    with pytest.raises(EngineConfigAllowlistError, match="redirects where work runs"):
        validate_against_deny(allowlist, "spark.master", value="spark://evil:7077")


def test_validate_against_deny_refuses_a_synthetic_entry_violating_deny_values():
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    with pytest.raises(EngineConfigAllowlistError, match="0 means unlimited"):
        validate_against_deny(allowlist, "spark.driver.maxResultSize", value=0)
    with pytest.raises(EngineConfigAllowlistError, match="0 means unlimited"):
        validate_against_deny(allowlist, "spark.driver.maxResultSize", value="0")


def test_validate_against_deny_allows_a_non_denied_candidate():
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    # Must not raise: neither the key nor this value is denied.
    validate_against_deny(allowlist, "spark.executor.memory", value="4g")
    validate_against_deny(allowlist, "spark.driver.maxResultSize", value="4g")


def test_validate_against_deny_cannot_be_bypassed_by_a_hand_built_allowlist():
    """Immutability check: constructing an EngineConfigAllowlist with an
    empty/narrowed deny_entries tuple is possible in Python (it's a plain
    dataclass), but nothing in this module ever hands out such an object —
    load_allowlist() is the only producer, always reading the shipped core
    file. This test documents the contract at the object level: a caller
    who bypasses load_allowlist() and fabricates their own
    EngineConfigAllowlist gets exactly the (lack of) protection they built,
    which is a statement about load_allowlist() being the sole legitimate
    source, not a loophole in validate_against_deny() itself."""
    fabricated = EngineConfigAllowlist(engine="spark", is_free_form=True, entries=(), deny_entries=())
    validate_against_deny(fabricated, "spark.master")  # does not raise: no deny data at all
    real = load_allowlist(_SPARK_PATH, "spark")
    with pytest.raises(EngineConfigAllowlistError):
        validate_against_deny(real, "spark.master")


# ── evaluate_set_engine_config — the Gate 1 enforcement logic ───────────────
#
# The shipped files (spark/duckdb) carry no range/enum-bearing entries except
# `spark.serializer`'s enum, so these tests build small synthetic allowlists
# to pin the constraint branches the real data never exercises today (per the
# module docstring: "range/enum are a MECHANISM, not a mandate" — but the
# mechanism itself must work once an operator/entry uses it).


def test_evaluate_against_real_spark_file_allowed_passes():
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    assert evaluate_set_engine_config(allowlist, "spark.sql.shuffle.partitions", 200) is None


def test_evaluate_against_real_spark_file_denied_key():
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    reason = evaluate_set_engine_config(allowlist, "spark.master", "local[*]")
    assert reason is not None
    assert "redirects where work runs" in reason


def test_evaluate_against_real_spark_file_denied_value():
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    reason = evaluate_set_engine_config(allowlist, "spark.driver.maxResultSize", 0)
    assert reason is not None
    assert "0 means unlimited" in reason


def test_evaluate_against_real_spark_file_unlisted_key():
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    reason = evaluate_set_engine_config(allowlist, "spark.totally.unlisted.key", "x")
    assert reason is not None
    assert "not on engine 'spark'" in reason


def test_evaluate_against_real_spark_file_wrong_type():
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    reason = evaluate_set_engine_config(allowlist, "spark.sql.shuffle.partitions", "200")
    assert reason is not None
    assert "expected int, got str" in reason


def test_evaluate_enum_constraint_pass_and_fail():
    allowlist = load_allowlist(_SPARK_PATH, "spark")
    assert evaluate_set_engine_config(
        allowlist, "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
    ) is None
    reason = evaluate_set_engine_config(allowlist, "spark.serializer", "com.example.Bogus")
    assert reason is not None
    assert "not one of" in reason


def _synthetic_allowlist(entry: AllowlistEntry) -> EngineConfigAllowlist:
    return EngineConfigAllowlist(engine="spark", is_free_form=True, entries=(entry,), deny_entries=())


def test_evaluate_int_range_constraint_pass_and_fail():
    allowlist = _synthetic_allowlist(
        AllowlistEntry(
            pattern="spark.sql.shuffle.partitions", value_type="int",
            constraint=RangeConstraint(minimum=1, maximum=2000),
        )
    )
    assert evaluate_set_engine_config(allowlist, "spark.sql.shuffle.partitions", 200) is None
    reason = evaluate_set_engine_config(allowlist, "spark.sql.shuffle.partitions", 5000)
    assert reason is not None
    assert "outside" in reason


def test_evaluate_size_range_constraint_pass_and_fail():
    allowlist = _synthetic_allowlist(
        AllowlistEntry(
            pattern="spark.executor.memory", value_type="size",
            constraint=RangeConstraint(minimum="128m", maximum="64g"),
        )
    )
    assert evaluate_set_engine_config(allowlist, "spark.executor.memory", "4g") is None
    reason = evaluate_set_engine_config(allowlist, "spark.executor.memory", "128g")
    assert reason is not None
    assert "outside" in reason


def test_evaluate_typed_field_duckdb_pass_and_fail():
    allowlist = load_allowlist(_DUCKDB_PATH, "duckdb")
    assert evaluate_set_engine_config(allowlist, "threads", 4) is None
    reason = evaluate_set_engine_config(allowlist, "database_path", "/tmp/evil.db")
    assert reason is not None
    assert "may not relocate it" in reason
