"""Spark engine healing prompt-rules pack (Phase 78 Step 2).

The engine-specific half of the healing system prompt. The generic scaffold
(PatchSpec schema, op-selection table, provenance rules, output contract,
defer rules) lives in ``aqueduct/agent/prompts.py`` and is engine-independent;
everything here names Spark, Spark's exception vocabulary, or Spark-flavored
advice, and is supplied to the agent through
``ExecutorProtocol.prompt_rules`` — the agent imports no engine specifics.

Text is verbatim from the pre-split ``_SYSTEM_PROMPT_TEMPLATE``: the composed
Spark prompt is byte-identical to the prompt shipped before the split (guarded
by ``tests/test_agent/test_prompt_composition.py``). This is a refactor of
WHERE the text lives, not a rewrite of the text — do not reword these strings
without following the ``PROMPT_VERSION`` bump policy in AGENTS.md.

Pure data — **pyspark-free**, like ``spark/capabilities.py``. It is imported
at ``aqueduct.engines`` entry-point resolution time, long before any Spark
session exists.

``rules`` is rendered VERBATIM into the composed prompt (it is a ``.format()``
*argument*, not part of the format string), so braces here are literal — no
``{{``/``}}`` doubling.
"""

from __future__ import annotations

from aqueduct.executor.protocol import DeferRules, PromptRules

# The engine's slice of the "when to defer to a human" section. That section is
# assembled at RUNTIME (only when `allow_defer` is on), not inside the template
# constant — which is exactly why three Spark-flavored strings survived the
# first pass at this split. Generic categories (upstream schema changes,
# checkpoint corruption, object-store consistency) stay in the agent scaffold;
# only what is Spark's own goes here — including "cluster config", which is a
# Spark CONCEPT (a single-process engine has no cluster to configure), not a
# generic infra example, so it lives in infra_examples rather than the scaffold.
_SPARK_DEFER = DeferRules(
    # Spark-only: a single-node engine has no Hive metastore to lock, and
    # "cluster config" only means something for a multi-node engine.
    infra_examples="Hive metastore locks, cluster config",
    # Spark's UDF registry accepts python/scala/java; another engine's does not.
    udf_languages="Python/Scala",
    # "Spark internals" names an engine — a whole bullet that is Spark's alone.
    extra_bullets=(
        "- **Error class has no module-config knob**: the failure is in Spark "
        "internals, not Blueprint fields.\n"
    ),
)

SPARK_PROMPT_RULES = PromptRules(
    persona=(
        "You are an expert Apache Spark blueprint repair agent for the Aqueduct blueprint engine."
    ),
    root_cause_note="Spark error class + offending column + suggestions",
    rules=(
        "- SQL Channel queries reference upstream module IDs as Spark temp view names (e.g. `FROM yellow_process__ingress`). NEVER use `${ctx.*}` inside a SQL query string.\n"
        "- If a Channel fails with unexpected column names (e.g. AnalysisException: cannot resolve column), check whether an upstream Ingress has the wrong `format` — Spark can silently misread Parquet as CSV. Fix the Ingress `format`, not the Channel SQL.\n"
        "- If `error_class` is `PREDICTED_SCHEMA_DRIFT`, the upstream SOURCE physically changed (a column was added, dropped, or renamed) — the format/header rule above does NOT apply. Do NOT change the Ingress `format`, `header`, or `options`; the provenance shows these are intentional authored values, and a real source change is not fixed by reinterpreting the file. Instead update the downstream Channel SQL / `schema_hint` to match the new column set, or make NO change if a dropped column is genuinely gone and unused.\n"
        "- `schema_hint type mismatch on 'X': expected 'A', actual 'B'` — read this literally: `expected` is the schema_hint value ALREADY declared in the Blueprint for field X (re-proposing it is a no-op, not a fix); `actual` is the type Spark inferred from the real data. The fix is `set_module_config_key` (or the appropriate provenance-driven op) changing the schema_hint for X to `actual` — NOT re-setting it to `expected`. If `actual` is clearly wrong for the field's domain (e.g. a numeric id column inferred as `string` because of a corrupt row), this is a genuine data problem outside Blueprint control — defer to a human if deferral is permitted (see the deferral rules), otherwise state that in `root_cause` and make no change."
    ),
    defer=_SPARK_DEFER,
)


__all__ = ["SPARK_PROMPT_RULES"]
