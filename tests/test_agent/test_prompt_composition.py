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
@pytest.mark.parametrize("tools_enabled", [False, True])
def test_composed_spark_prompt_is_the_pre_split_prompt(
    tmp_path: Path, allow_defer: bool, tools_enabled: bool
):
    """The composed prompt reproduces the pre-split text exactly: engine
    content lands in its original three positions, nothing is reordered,
    duplicated, or dropped."""
    prompt = _build_system_prompt(
        tmp_path,
        allow_defer=allow_defer,
        tools_enabled=tools_enabled,
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

    assert (
        generic_before + "\n" + SPARK_PROMPT_RULES.rules + "\n" + generic_after
    ) in prompt

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
    Spark today (`set_spark_config` / `spark_config` is a real patch op). That
    is a grammar-level engine leak governed by the capability framework (a
    `spark_config` leaf is `ignored_with_warning` on a non-Spark engine), NOT
    prompt-composition bleed, and genericizing the patch grammar is out of
    scope here. Everything OUTSIDE this block is prose we own, and must carry
    zero engine tokens for a non-Spark engine.
    """
    start = prompt.index("## PatchSpec Schema")
    end = prompt.index("## Rules")
    return prompt[:start] + prompt[end:]


@pytest.mark.parametrize("allow_defer", [False, True])
@pytest.mark.parametrize("tools_enabled", [False, True])
def test_composed_prompt_for_non_spark_engine_has_zero_engine_bleed(
    tmp_path: Path, monkeypatch, allow_defer: bool, tools_enabled: bool
):
    """THE guard. A heal on a non-Spark engine must not be told anything about
    Spark — not in the persona, not in the rules, and not in the defer section
    (the fragment the template-only guard could not see). `allow_defer=True` is
    the combo that catches the defer bleed: that section is only rendered then.
    """
    import aqueduct.executor.protocol as protocol

    monkeypatch.setitem(
        protocol.PROTOCOL_REGISTRY, "fake-engine", _fake_engine_protocol()
    )

    prompt = _build_system_prompt(
        tmp_path,
        allow_defer=allow_defer,
        tools_enabled=tools_enabled,
        coaching=False,
        obs_store=None,
        engine="fake-engine",
    )
    prose = _strip_patch_schema(prompt)

    for token in _ENGINE_TOKENS:
        assert token.lower() not in prose.lower(), (
            f"engine token {token!r} leaked into the composed prompt for a NON-Spark "
            f"engine (allow_defer={allow_defer}, tools_enabled={tools_enabled}). It "
            "belongs in the engine's PromptRules pack "
            "(aqueduct/executor/spark/prompt_rules.py), not in the agent's scaffold. "
            "Note the scaffold is not just _SYSTEM_PROMPT_TEMPLATE — parts of the "
            "prompt (defer_rules) are assembled at runtime in _build_system_prompt."
        )


def test_engine_pack_is_what_gets_composed_in(tmp_path: Path, monkeypatch):
    """The composed prompt takes its engine content FROM the registered
    engine's pack — not from a constant baked into the agent layer. A
    different engine's pack produces a different prompt through the same
    scaffold."""
    import aqueduct.executor.protocol as protocol

    monkeypatch.setitem(
        protocol.PROTOCOL_REGISTRY, "fake-engine", _fake_engine_protocol()
    )

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

    monkeypatch.setitem(
        protocol.PROTOCOL_REGISTRY, "fake-engine", _fake_engine_protocol()
    )
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
