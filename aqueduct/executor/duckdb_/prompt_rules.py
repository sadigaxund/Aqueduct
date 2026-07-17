"""DuckDB engine healing prompt-rules pack (Phase 78 Stage A).

The engine-specific half of the healing system prompt (see
``aqueduct/executor/protocol.py::PromptRules`` for the contract this fills).
The generic scaffold (PatchSpec schema, op-selection table, provenance rules,
output contract, defer rules) lives in ``aqueduct/agent/prompts.py`` and is
engine-independent; everything here names DuckDB, DuckDB's own exception
vocabulary, or DuckDB-flavored advice — written fresh for this engine, not
copied from Spark's pack (``aqueduct/executor/spark/prompt_rules.py``). There
is zero Spark-specific text below: no temp views (DuckDB registers relations
directly), no AnalysisException, no Hive metastore, no Python/Scala UDF
vocabulary.

Pure data — **duckdb-free**, like ``duckdb_/capabilities.py``. Imported at
``aqueduct.engines`` entry-point resolution time, long before any DuckDB
connection exists.

``rules`` is rendered VERBATIM into the composed prompt (a ``.format()``
*argument*, not part of the format string), so braces here are literal — no
``{{``/``}}`` doubling.
"""

from __future__ import annotations

from aqueduct.executor.protocol import DeferRules, PromptRules

# The engine's slice of the "when to defer to a human" section. Generic
# categories (upstream schema changes, checkpoint corruption, object-store
# consistency, cluster config) stay in the agent scaffold; only what is
# DuckDB's own goes here.
_DUCKDB_DEFER = DeferRules(
    # DuckDB is single-process and embedded — it has no cluster/metastore to
    # lock, but it DOES have a single-writer constraint on a persistent
    # database file, which is its own infrastructure failure mode.
    infra_examples="a concurrent writer already holding the DuckDB database file lock",
    # DuckDB Python UDFs are unsupported this stage (Stage A capability
    # table); `lang: java` is Spark-only. Naming a UDF language here would
    # be misleading before either is actually implemented/tested.
    udf_languages="Python (not yet supported on this engine — see the capability table)",
    extra_bullets=(
        "- **Error class has no module-config knob**: the failure is in "
        "DuckDB's own query planner/optimizer, not Blueprint fields.\n"
    ),
)

DUCKDB_PROMPT_RULES = PromptRules(
    persona=(
        "You are an expert DuckDB blueprint repair agent for the Aqueduct blueprint engine."
    ),
    root_cause_note="DuckDB exception class + offending column/table name + suggested identifiers",
    rules=(
        "- SQL Channel queries reference upstream module IDs as DuckDB relation names registered on the connection (e.g. `FROM yellow_process__ingress`). NEVER use `${ctx.*}` inside a SQL query string.\n"
        "- DuckDB is a single-node, in-process engine: `channel.op.repartition`, `channel.op.coalesce`, and `channel.op.cache` have no DuckDB equivalent and are UNSUPPORTED — do not propose a patch that adds one of these ops on this engine; propose removing the op or expressing the same intent inside `op: sql` instead.\n"
        "- If a Channel fails with a Binder Error naming a column that does not exist, check whether an upstream Ingress has the wrong `format` — DuckDB infers CSV columns/types from the file header and a mismatched `format` (e.g. reading a Parquet file as CSV) produces garbage column names, not a clean read error. Fix the Ingress `format`, not the Channel SQL.\n"
        "- `schema_hint type mismatch on 'X': expected 'A', actual 'B'` — read this literally: `expected` is the schema_hint value ALREADY declared in the Blueprint for field X (re-proposing it is a no-op, not a fix); `actual` is the type DuckDB inferred from the real data. The fix is `set_module_config_key` (or the appropriate provenance-driven op) changing the schema_hint for X to `actual` — NOT re-setting it to `expected`. If `actual` is clearly wrong for the field's domain, this is a genuine data problem outside Blueprint control — defer to a human if deferral is permitted, otherwise state that in `root_cause` and make no change.\n"
        "- A Binder Error's 'Did you mean \"col\"?' suggestion names the closest real column by edit distance, not necessarily the semantically correct one — check the Blueprint's declared schema_hint / upstream Channel `select` list before trusting the suggestion verbatim.\n"
        "- DuckDB's `COPY ... TO` Egress write has no native append mode (`egress.mode.append` is UNSUPPORTED on this engine) — never propose a patch that sets `mode: append` for a DuckDB Egress; only `overwrite`, `error`, `errorifexists`, and `ignore` are valid write modes here."
    ),
    defer=_DUCKDB_DEFER,
)


__all__ = ["DUCKDB_PROMPT_RULES"]
