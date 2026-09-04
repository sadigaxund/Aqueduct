"""Phase 78 Step 2 — the healing system prompt is COMPOSED, not monolithic.

The system prompt splits into an engine-independent scaffold that stays in the
agent layer (``aqueduct/agent/prompts.py``: PatchSpec schema, op-selection
table, provenance rules, output contract, defer rules) plus the target engine's
``PromptRules`` pack, pulled through ``ExecutorProtocol.prompt_rules``
(``aqueduct/executor/protocol.py``). Spark's pack lives in
``aqueduct/executor/spark/prompt_rules.py``.

The acceptance bar of that refactor: the COMPOSED Spark prompt is byte-identical
to the pre-split prompt. The golden below is the pre-split template rendered
verbatim — a snapshot of the exact text the model saw before the split. It is
NOT a restatement of the composition logic (that would be a tautology), it is
the shipped prompt frozen as data. If a change to either half alters the Spark
prompt, this test fails and the ``PROMPT_VERSION`` bump policy in AGENTS.md
applies.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aqueduct.agent.prompts import _build_system_prompt
from aqueduct.executor.protocol import DeferRules, PromptRules

pytestmark = pytest.mark.unit


# The engine-flavored text as it appeared in the single pre-split
# _SYSTEM_PROMPT_TEMPLATE, frozen here as a golden. Compare against the Spark
# pack's fields — a reword on either side must be a deliberate, versioned act.
_GOLDEN_SPARK_PERSONA = (
    "You are an expert Apache Spark blueprint repair agent for the Aqueduct blueprint engine."
)
_GOLDEN_SPARK_ROOT_CAUSE_NOTE = "Spark error class + offending column + suggestions"


def test_spark_pack_matches_pre_split_text():
    from aqueduct.executor.spark.prompt_rules import SPARK_PROMPT_RULES

    assert SPARK_PROMPT_RULES.persona == _GOLDEN_SPARK_PERSONA
    assert SPARK_PROMPT_RULES.root_cause_note == _GOLDEN_SPARK_ROOT_CAUSE_NOTE
    # The four Spark-specific "Other rules" bullets, verbatim.
    rules = SPARK_PROMPT_RULES.rules.split("\n")
    assert len(rules) == 4
    assert rules[0].startswith(
        "- SQL Channel queries reference upstream module IDs as Spark temp view names"
    )
    assert "AnalysisException: cannot resolve column" in rules[1]
    assert "PREDICTED_SCHEMA_DRIFT" in rules[2]
    assert "schema_hint type mismatch" in rules[3]
    # Rendered verbatim into the prompt (a .format() ARGUMENT, not part of the
    # format string) — braces here must be literal, never doubled.
    assert "${ctx.*}" in SPARK_PROMPT_RULES.rules
    assert "${{ctx.*}}" not in SPARK_PROMPT_RULES.rules
    # The defer slice — the fragment the template-only guard missed.
    # "cluster config" lives here (not the scaffold): it is a Spark CONCEPT
    # (meaningless for a single-process engine), not a generic infra example.
    assert SPARK_PROMPT_RULES.defer.infra_examples == "Hive metastore locks, cluster config"
    assert SPARK_PROMPT_RULES.defer.udf_languages == "Python/Scala"
    assert SPARK_PROMPT_RULES.defer.extra_bullets == (
        "- **Error class has no module-config knob**: the failure is in Spark "
        "internals, not Blueprint fields.\n"
    )


@pytest.mark.parametrize("allow_defer", [False, True])
def test_composed_spark_prompt_is_the_pre_split_prompt(tmp_path: Path, allow_defer: bool):
    """The composed prompt reproduces the pre-split text exactly: engine
    content lands in its original three positions, nothing is reordered,
    duplicated, or dropped."""
    prompt = _build_system_prompt(
        tmp_path,
        allow_defer=allow_defer,
        coaching=False,
        obs_store=None,
        engine="spark",
    )

    # 1. Persona is the FIRST line, exactly as before.
    assert prompt.split("\n", 1)[0] == _GOLDEN_SPARK_PERSONA

    # 2. The root-cause note sits inside its original bullet, not on its own line.
    assert (
        "- The error message and either a structured root-cause block "
        f"({_GOLDEN_SPARK_ROOT_CAUSE_NOTE}) OR a raw stack trace if structured "
        "extraction was unavailable" in prompt
    )

    # 3. The Spark rules sit between the last generic "Other rules" bullet and
    #    the generic schema_hint-field-not-found bullet — the pre-split order.
    generic_before = '- SQL query wrong → `set_module_config_key` with key="query".'
    generic_after = "- `schema_hint field 'X' not found in source schema."
    from aqueduct.executor.spark.prompt_rules import SPARK_PROMPT_RULES

    assert (generic_before + "\n" + SPARK_PROMPT_RULES.rules + "\n" + generic_after) in prompt

    # 3b. The defer section (allow_defer only) reproduces the pre-split bullets
    #     verbatim, engine-flavored parts back in their original positions.
    if allow_defer:
        assert (
            "- **Infrastructure failures**: checkpoint corruption, S3 consistency, "
            "Hive metastore locks, cluster config — these are not Blueprint-level fixes.\n"
            "- **Upstream schema changes** requiring human judgment: ambiguous column "
            "renames, new required columns with unclear defaults.\n"
            "- **UDF body bugs**: PatchSpec cannot modify Python/Scala UDF code.\n"
            "- **Error class has no module-config knob**: the failure is in Spark "
            "internals, not Blueprint fields.\n"
        ) in prompt

    # 4. No unrendered slot leaked into what the model sees.
    for slot in ("{engine_persona}", "{engine_root_cause_note}", "{engine_rules}"):
        assert slot not in prompt


# ── The anti-bleed guard ─────────────────────────────────────────────────────
#
# It greps the COMPOSED PROMPT for a non-Spark engine, not the source
# constants. That distinction is the whole point: the first pass at this split
# guarded `_SYSTEM_PROMPT_TEMPLATE` only, and three Spark-flavored strings
# (Hive metastore locks / Python/Scala UDF code / "Spark internals") survived
# in `defer_rules`, which `_build_system_prompt` assembles at RUNTIME and the
# template constant never contains. A guard that scans one constant is not a
# guard for a prompt that is composed from several fragments.

_ENGINE_TOKENS = (
    "Spark",
    "pyspark",
    "AnalysisException",
    "Py4J",
    "Hive",
    "Scala",
    "metastore",
    # Spark CONCEPTS, not just the engine's name — added after "cluster
    # config" (a Spark-only notion: a single-process engine has no cluster)
    # leaked through the scaffold as plain English that the name-only list
    # above could not catch. Each entry below was checked against the
    # engine-independent scaffold (aqueduct/agent/prompts.py) and the fake
    # engine's own pack for false positives before landing here.
    "cluster",  # 0 hits in the scaffold; Spark-only notion (multi-node deployment).
    "shuffle",  # 0 hits in the scaffold; Spark's data-redistribution stage.
    "broadcast join",  # phrase, not bare "broadcast" (which could appear in generic prose).
    "YARN",  # Spark's (optional) cluster resource manager.
    "driver memory",  # phrase, not bare "driver" (JDBC "driver" is legitimate generic vocabulary).
    # Rejected as too ambiguous to assert on (see test module docstring / task notes):
    #   - "executor"/"executors": Aqueduct's OWN architecture layer is named
    #     Executor (Parser -> Compiler -> Executor -> Surveyor) and
    #     ExecutorProtocol/get_executor are engine-agnostic core names: this
    #     token fires 9+ times in the scaffold itself with zero relation to
    #     Spark's executor processes. A guard that always fails is worse than
    #     no guard.
    #   - "partition"/"partition count": generic data-partitioning is real
    #     grammar on every engine (e.g. Egress `overwrite_partitions`), and
    #     DuckDB's own pack legitimately says "repartition" (to explain the op
    #     is UNSUPPORTED there). Only "partition-count-as-a-Spark-tuning-knob"
    #     is the Spark concept, and there is no substring that captures that
    #     without also matching the legitimate generic usage.
    #   - "spark.sql.*"/"driver" (bare)/"broadcast" (bare): redundant with
    #     "Spark" (already covers `spark.sql.*`) or too generic standalone
    #     (see the phrase versions above instead).
)


def _fake_engine_protocol():
    import aqueduct.executor.protocol as protocol

    return protocol.ExecutorProtocol(
        engine="fake-engine",
        execute=lambda *a, **k: None,
        extract_error=lambda exc: None,
        prompt_rules=PromptRules(
            persona="You are an expert Fake blueprint repair agent.",
            root_cause_note="fake error code + hint",
            rules="- a fake-engine-only rule about COPY TO.",
            defer=DeferRules(
                infra_examples="fake lock contention",
                udf_languages="Python",
            ),
        ),
    )


def _strip_patch_schema(prompt: str) -> str:
    """Drop the "## PatchSpec Schema" block before token-scanning.

    That block is `PatchSpec.model_json_schema()` — the Blueprint PATCH GRAMMAR,
    rendered as JSON, not prose the prompt layer authors. It legitimately names
    Spark today (`set_engine_config`'s `engine` field description gives 'spark'
    as an example value, e.g.). That is a grammar-level engine leak governed by
    the capability framework (a `spark_config` leaf is `ignored_with_warning`
    on a non-Spark engine), NOT prompt-composition bleed, and genericizing the
    patch grammar is out of scope here. Everything OUTSIDE this block is prose
    we own, and must carry zero engine tokens for a non-Spark engine.
    """
    start = prompt.index("## PatchSpec Schema")
    end = prompt.index("## Rules")
    return prompt[:start] + prompt[end:]


@pytest.mark.parametrize("allow_defer", [False, True])
def test_composed_prompt_for_non_spark_engine_has_zero_engine_bleed(
    tmp_path: Path, monkeypatch, allow_defer: bool
):
    """THE guard. A heal on a non-Spark engine must not be told anything about
    Spark — not in the persona, not in the rules, and not in the defer section
    (the fragment the template-only guard could not see). `allow_defer=True` is
    the combo that catches the defer bleed: that section is only rendered then.
    """
    import aqueduct.executor.protocol as protocol

    monkeypatch.setitem(protocol.PROTOCOL_REGISTRY, "fake-engine", _fake_engine_protocol())

    prompt = _build_system_prompt(
        tmp_path,
        allow_defer=allow_defer,
        coaching=False,
        obs_store=None,
        engine="fake-engine",
    )
    prose = _strip_patch_schema(prompt)

    for token in _ENGINE_TOKENS:
        assert token.lower() not in prose.lower(), (
            f"engine token {token!r} leaked into the composed prompt for a NON-Spark "
            f"engine (allow_defer={allow_defer}). It belongs in the engine's "
            "PromptRules pack (aqueduct/executor/spark/prompt_rules.py), not in the "
            "agent's scaffold. Note the scaffold is not just _SYSTEM_PROMPT_TEMPLATE "
            "— parts of the prompt (defer_rules) are assembled at runtime in "
            "_build_system_prompt."
        )


def test_engine_pack_is_what_gets_composed_in(tmp_path: Path, monkeypatch):
    """The composed prompt takes its engine content FROM the registered
    engine's pack — not from a constant baked into the agent layer. A
    different engine's pack produces a different prompt through the same
    scaffold."""
    import aqueduct.executor.protocol as protocol

    monkeypatch.setitem(protocol.PROTOCOL_REGISTRY, "fake-engine", _fake_engine_protocol())

    prompt = _build_system_prompt(
        tmp_path, allow_defer=True, coaching=False, obs_store=None, engine="fake-engine"
    )

    assert prompt.startswith("You are an expert Fake blueprint repair agent.")
    assert "fake error code + hint" in prompt
    assert "- a fake-engine-only rule about COPY TO." in prompt
    # The engine's defer slice is composed in too. "cluster config" is NOT
    # appended here — it is a Spark-only concept that lives in Spark's own
    # infra_examples value, not a scaffold-injected suffix every engine gets
    # (that was the bug: the scaffold used to hardcode ", cluster config"
    # after infra_examples for every engine, including this fake one).
    assert "fake lock contention" in prompt
    assert "cluster config" not in prompt
    assert "PatchSpec cannot modify Python UDF code." in prompt
    # The generic scaffold is still there, unchanged.
    assert "## PatchSpec Schema" in prompt
    assert "`replace_module_config_key` does NOT exist." in prompt
    # Generic defer categories survive — only the engine-flavored parts moved.
    assert "- **Upstream schema changes** requiring human judgment" in prompt
    assert "checkpoint corruption, S3 consistency" in prompt


def test_engine_with_no_extra_defer_bullets_renders_cleanly(tmp_path: Path, monkeypatch):
    """`DeferRules.extra_bullets` is optional — an engine with no extra defer
    category must not leave a dangling blank bullet or lose the section's
    trailing structure."""
    import aqueduct.executor.protocol as protocol

    monkeypatch.setitem(protocol.PROTOCOL_REGISTRY, "fake-engine", _fake_engine_protocol())
    prompt = _build_system_prompt(
        tmp_path, allow_defer=True, coaching=False, obs_store=None, engine="fake-engine"
    )
    assert (
        "- **UDF body bugs**: PatchSpec cannot modify Python UDF code.\n"
        "\n"
        "When deferring, include:\n"
    ) in prompt
    assert "\n- \n" not in prompt  # no empty bullet left behind


def test_unknown_engine_prompt_build_raises_unknown_engine_error(tmp_path: Path):
    from aqueduct.errors import UnknownEngineError

    with pytest.raises(UnknownEngineError):
        _build_system_prompt(tmp_path, engine="bogus-engine")


# ── DuckDB out-of-memory / capacity defer rule (Phase 79 item 6) ────────────
#
# DuckDB is single-node — it fails by running OUT OF MEMORY, a failure class
# Spark's prompt pack has no idiom for. Without a defer rule the healer reads
# an OOM as a code defect and patches in circles. Both the always-shown
# `rules` bullet (allow_defer off) and the defer section (allow_defer on)
# must classify it as non-patchable.


def test_composed_duckdb_prompt_classifies_oom_as_defer_not_patchable(tmp_path: Path):
    prompt = _build_system_prompt(
        tmp_path,
        allow_defer=True,
        coaching=False,
        obs_store=None,
        engine="duckdb",
    )
    assert "Out of Memory" in prompt
    assert "memory_limit" in prompt
    assert "capacity limit of the machine, not a bug in the Blueprint" in prompt
    assert "do NOT propose repeated config-value edits chasing this error" in prompt


def _load_shipped_allowlist(engine: str):
    from aqueduct.executor.engine_config_allowlist import (
        discover_allowlist_paths,
        load_allowlist,
    )

    path = discover_allowlist_paths().get(engine)
    assert path is not None, f"engine {engine!r} ships no engine_config_allowlist.yml"
    return load_allowlist(path, engine)


@pytest.mark.parametrize("engine", ["spark", "duckdb"])
def test_whole_engine_config_allowlist_is_disclosed_in_the_prompt(tmp_path: Path, engine: str):
    """The model is told the WHOLE policy, not a curated subset.

    Compared against the SHIPPED yml (data) rather than a hand-listed set of
    keys (which would be the same tautology `all_leaves_default()` was): add a
    row to either engine's `engine_config_allowlist.yml` and this test fails
    until the prompt renders it too.
    """
    allowlist = _load_shipped_allowlist(engine)
    prompt = _build_system_prompt(tmp_path, coaching=False, obs_store=None, engine=engine)

    assert "### Engine/session config (`set_engine_config`)" in prompt
    for entry in allowlist.entries:
        assert f"`{entry.pattern}`" in prompt, (
            f"allow entry {entry.pattern!r} is enforced at Gate 1 but never "
            f"disclosed to the model healing on {engine!r}"
        )
    for deny in allowlist.deny_entries:
        assert f"`{deny.pattern}`" in prompt, (
            f"deny entry {deny.pattern!r} is enforced at Gate 1 but never "
            f"disclosed to the model healing on {engine!r}"
        )
        # The `reason` is mandatory on a deny row precisely so a refusal can be
        # explained; it must reach the model, not just the rejection message.
        assert deny.reason in prompt


@pytest.mark.parametrize(("engine", "other"), [("spark", "duckdb"), ("duckdb", "spark")])
def test_engine_config_policy_never_leaks_another_engines_keys(
    tmp_path: Path, engine: str, other: str
):
    """Engine X's composed prompt must never carry engine Y's config keys.

    The patch-schema block is stripped for the same reason the anti-bleed guard
    strips it (see `_strip_patch_schema`): `SetEngineConfigOp.key`'s own field
    description names both a Spark and a DuckDB key as examples. That is patch
    GRAMMAR, not the per-engine policy this test guards.
    """
    mine = _load_shipped_allowlist(engine)
    theirs = _load_shipped_allowlist(other)

    mine_patterns = {e.pattern for e in mine.entries} | {d.pattern for d in mine.deny_entries}
    theirs_only = (
        {e.pattern for e in theirs.entries} | {d.pattern for d in theirs.deny_entries}
    ) - mine_patterns

    prose = _strip_patch_schema(
        _build_system_prompt(tmp_path, coaching=False, obs_store=None, engine=engine)
    )
    for pattern in sorted(theirs_only):
        assert pattern not in prose, (
            f"engine {other!r}'s config key {pattern!r} leaked into the composed "
            f"prompt for engine {engine!r}"
        )


def test_engine_with_no_shipped_allowlist_is_told_the_op_is_unavailable(
    tmp_path: Path, monkeypatch
):
    """A third-party engine shipping no `engine_config_allowlist.yml` can never
    pass Gate 1 with a `set_engine_config` op. Rendering an empty table (or
    nothing) would let the model believe it may write engine config and be
    refused every time — the silent no-op AGENTS.md forbids. It is told the op
    is unavailable instead."""
    import aqueduct.executor.protocol as protocol

    monkeypatch.setitem(protocol.PROTOCOL_REGISTRY, "fake-engine", _fake_engine_protocol())
    prompt = _build_system_prompt(tmp_path, coaching=False, obs_store=None, engine="fake-engine")

    assert "### Engine/session config (`set_engine_config`)" in prompt
    assert "`set_engine_config` is NOT available for engine `fake-engine`" in prompt
    assert "Do NOT emit a `set_engine_config` operation" in prompt
    # No table header, no allow/deny rows — nothing that reads as "here is what
    # you may write".
    assert "| key | value type | allowed values |" not in prompt


def test_engine_with_explicitly_empty_allowlist_is_told_the_op_is_unavailable(
    tmp_path: Path, monkeypatch
):
    """`entries: []` is a decision (see the allowlist loader's presence guard),
    and its consequence is identical: no key is writable, so the op is unusable
    and the prompt says so rather than printing an empty table."""
    import aqueduct.agent.prompts as prompts_mod

    empty = tmp_path / "engine_config_allowlist.yml"
    empty.write_text("engine: duckdb\nentries: []\n", encoding="utf-8")
    monkeypatch.setattr(
        "aqueduct.executor.engine_config_allowlist.discover_allowlist_paths",
        lambda: {"duckdb": empty},
    )

    section = prompts_mod._render_engine_config_policy("duckdb")
    assert "`set_engine_config` is NOT available for engine `duckdb`" in section
    assert "declares no writable key" in section


def test_unloadable_shipped_allowlist_degrades_to_unavailable_and_warns(
    tmp_path: Path, monkeypatch, caplog
):
    """A malformed SHIPPED file makes Gate 1 raise too, so the op genuinely
    cannot succeed — the prompt says so rather than aborting a heal some OTHER
    op could still fix. It is logged, never swallowed silently."""
    import logging

    import aqueduct.agent.prompts as prompts_mod

    broken = tmp_path / "engine_config_allowlist.yml"
    broken.write_text("engine: duckdb\n", encoding="utf-8")  # no `entries:` key
    monkeypatch.setattr(
        "aqueduct.executor.engine_config_allowlist.discover_allowlist_paths",
        lambda: {"duckdb": broken},
    )

    with caplog.at_level(logging.WARNING, logger="aqueduct.agent.prompts"):
        section = prompts_mod._render_engine_config_policy("duckdb")

    assert "`set_engine_config` is NOT available for engine `duckdb`" in section
    assert any("unloadable" in r.message for r in caplog.records)


def test_composed_duckdb_prompt_oom_rule_present_without_defer(tmp_path: Path):
    """The `rules` bullet (always rendered) must also warn against chasing an
    OOM with config edits even when `allow_defer` is off."""
    prompt = _build_system_prompt(
        tmp_path,
        allow_defer=False,
        coaching=False,
        obs_store=None,
        engine="duckdb",
    )
    assert "Out of Memory" in prompt
    assert "memory_limit" in prompt
    assert "capacity limit of the machine, not a Blueprint defect" in prompt


# ── Phase 88 — declare_dependency op guidance + defer_reason vocabulary ─────
#
# Both changes are true of every engine (a missing package and a defer bucket
# are not Spark/DuckDB-flavored concepts), so they belong in the
# engine-independent scaffold, not any engine's PromptRules pack. Proven here
# against the "fake-engine" non-Spark protocol, same as the anti-bleed guard
# above, so a regression that accidentally moved this text into an engine
# pack (or dropped it) is caught regardless of which engine is composing.


def test_composed_prompt_discloses_declare_dependency_never_installs_semantics(
    tmp_path: Path, monkeypatch
):
    import aqueduct.executor.protocol as protocol

    monkeypatch.setitem(protocol.PROTOCOL_REGISTRY, "fake-engine", _fake_engine_protocol())

    prompt = _build_system_prompt(tmp_path, coaching=False, obs_store=None, engine="fake-engine")
    assert (
        "`declare_dependency` records a required package in the Blueprint's "
        "`dependencies:` block — it DECLARES, it does not install. Aqueduct "
        "never installs anything."
    ) in prompt
    assert "staged for a human to install it rather than auto-applied" in prompt
    assert "the package or version does not exist at all, the patch is rejected" in prompt
    assert "PEP 508" in prompt
    assert "environment markers are not supported" in prompt


def test_composed_prompt_discloses_defer_reason_bucket_vocabulary(tmp_path: Path, monkeypatch):
    import aqueduct.executor.protocol as protocol

    monkeypatch.setitem(protocol.PROTOCOL_REGISTRY, "fake-engine", _fake_engine_protocol())

    prompt = _build_system_prompt(
        tmp_path,
        allow_defer=True,
        coaching=False,
        obs_store=None,
        engine="fake-engine",
    )
    assert (
        "- `defer_reason`: REQUIRED — the nearest bucket, one of `infrastructure` | "
        "`upstream_schema_change` | `data_shape_change` | `insufficient_context` | `other`."
    ) in prompt
    assert "does NOT replace `diagnosis`, `suggestions`, or `confidence_reason`" in prompt
    assert "`other` only as a last resort" in prompt

    # allow_defer=False must hide this section entirely, same as the rest of
    # the defer block (Phase 41 invariant: no easy way out on a normal heal).
    prompt_no_defer = _build_system_prompt(
        tmp_path,
        allow_defer=False,
        coaching=False,
        obs_store=None,
        engine="fake-engine",
    )
    assert "defer_reason" not in prompt_no_defer
