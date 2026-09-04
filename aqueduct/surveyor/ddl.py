"""Observability-store DDL owned by the Surveyor.

Extracted from ``surveyor.py`` so the schema definitions live in one place.
``surveyor.py`` re-imports these names, so existing references
(`aqueduct.surveyor.surveyor._SIGNAL_OVERRIDES_DDL`, etc.) keep working.

Pure SQL strings — no imports, no ``pyspark``.
"""

from __future__ import annotations

_DDL = """
CREATE TABLE IF NOT EXISTS run_records (
    run_id         VARCHAR PRIMARY KEY,
    blueprint_id   VARCHAR NOT NULL,
    status         VARCHAR NOT NULL,
    started_at     TIMESTAMPTZ NOT NULL,
    finished_at    TIMESTAMPTZ,
    module_results JSON,
    parent_run_id  VARCHAR,
    -- Phase 85 D8 — top-level execution engine ("spark", "duckdb", ...) for
    -- this run. Every OTHER table that carries `engine` (failure_contexts,
    -- healing_outcomes, heal_attempts, patch_index) stamps it as a plain
    -- column; run_records previously only had it buried inside the
    -- unindexed `module_results` JSON blob (one entry per module), which
    -- made "WHERE engine = ?" for a SUCCESSFUL run impossible without
    -- JSON-parsing every row. Populated by Surveyor.start()/.record() from
    -- the Surveyor's own required `engine` constructor arg — same source
    -- module_results' per-module `engine` field already uses.
    engine         VARCHAR
);

CREATE TABLE IF NOT EXISTS failure_contexts (
    run_id            VARCHAR PRIMARY KEY,
    blueprint_id      VARCHAR NOT NULL,
    failed_module     VARCHAR NOT NULL,
    error_message     VARCHAR NOT NULL,
    stack_trace       VARCHAR,
    manifest_json     VARCHAR,     -- Phase 39: blob path or inline JSON
    provenance_json   VARCHAR,     -- Phase 39: blob path or inline JSON
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ NOT NULL,
    -- Structured Spark-error extraction. Populated when PySparkException or
    -- Py4JJavaError surfaces enough metadata to identify the failure class,
    -- offending object, and suggested column names — much cheaper for the
    -- agent to consume than a raw multi-kilobyte JVM stack trace.
    error_class       VARCHAR,
    root_exception    JSON,
    sql_state         VARCHAR,
    object_name       VARCHAR,
    suggested_columns JSON,
    -- Phase 78 — execution engine this failure occurred on ("spark",
    -- "duckdb", ...). Stamped by Surveyor.record() from its required
    -- `engine` constructor arg.
    engine            VARCHAR
);

CREATE TABLE IF NOT EXISTS healing_outcomes (
    id           VARCHAR PRIMARY KEY,
    run_id       VARCHAR NOT NULL,
    parent_run_id VARCHAR,
    failed_module VARCHAR,
    failure_category VARCHAR,
    model        VARCHAR,
    patch_id     VARCHAR,
    confidence   DOUBLE PRECISION,
    patch_applied BOOLEAN,
    run_success_after_patch BOOLEAN,
    applied_at   VARCHAR,
    prompt_version VARCHAR,
    -- Phase 45: exact failure-signature hash + how the heal was resolved.
    -- 'llm' is the only value written since Phase 92 removed the
    -- signature-keyed pending-reuse/replay paths (a pre-2.3.0 database may
    -- still carry historical 'cached'/'replayed' rows).
    failure_signature VARCHAR,
    resolution   VARCHAR,
    failure_signature_coarse VARCHAR,
    -- Phase 46: 0-based cascade tier index of the model that produced the
    -- patch; NULL outside multi-model cascade (or when no LLM was involved).
    model_cascade_position INTEGER,
    -- Phase 78 — execution engine this heal targeted ("spark", "duckdb", ...).
    engine       VARCHAR
);

CREATE TABLE IF NOT EXISTS patch_simulation (
    id           VARCHAR PRIMARY KEY,
    run_id       VARCHAR,
    blueprint_id VARCHAR,
    patch_id     VARCHAR NOT NULL,
    gate         VARCHAR NOT NULL,
    status       VARCHAR NOT NULL,
    detail       VARCHAR,
    sample_rows  BIGINT,
    duration_ms  BIGINT,
    recorded_at  VARCHAR NOT NULL
);

-- Column-level lineage extracted at compile time (driver-side, zero Spark actions).
-- Merged from the former lineage.db in Phase 38.
CREATE TABLE IF NOT EXISTS column_lineage (
    blueprint_id   VARCHAR NOT NULL,
    run_id         VARCHAR NOT NULL,
    channel_id     VARCHAR NOT NULL,
    output_column  VARCHAR NOT NULL,
    source_table   VARCHAR NOT NULL,
    source_column  VARCHAR NOT NULL,
    captured_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_lineage_channel
    ON column_lineage (blueprint_id, channel_id);

-- Phase 56 (Lineage v2): SQL-AST normalised fingerprint per Channel.
-- Changelog, NOT a run-log: one row per distinct fingerprint per
-- (blueprint_id, channel_id). Repeat runs of unchanged SQL only bump
-- last_seen/last_run_id (ON CONFLICT), so size tracks SQL edits, not runs.
CREATE TABLE IF NOT EXISTS channel_fingerprints (
    blueprint_id  VARCHAR NOT NULL,
    channel_id    VARCHAR NOT NULL,
    fingerprint   VARCHAR NOT NULL,
    canonical_sql VARCHAR NOT NULL,
    first_seen    TIMESTAMPTZ NOT NULL,
    last_seen     TIMESTAMPTZ NOT NULL,
    first_run_id  VARCHAR NOT NULL,
    last_run_id   VARCHAR NOT NULL,
    PRIMARY KEY (blueprint_id, channel_id, fingerprint)
);
CREATE INDEX IF NOT EXISTS idx_fingerprint_latest
    ON channel_fingerprints (blueprint_id, channel_id, last_seen);
"""

_SIGNAL_OVERRIDES_DDL = """
CREATE TABLE IF NOT EXISTS signal_overrides (
    signal_id     VARCHAR PRIMARY KEY,
    passed        BOOLEAN NOT NULL,
    error_message VARCHAR,
    set_at        TIMESTAMPTZ NOT NULL
);
"""

# Per-attempt log for the unified reprompt loop.
# One row per LLM turn (success or failure) so post-mortem can answer
# "what did attempt 2 actually say" — which `healing_outcomes` alone could
# not (it only carries the final patch outcome).
_HEAL_ATTEMPTS_DDL = """
CREATE TABLE IF NOT EXISTS heal_attempts (
    id                    VARCHAR PRIMARY KEY,
    run_id                VARCHAR NOT NULL,
    attempt_num           INTEGER NOT NULL,
    error_class           VARCHAR,
    where_field           VARCHAR,
    normalized_message    VARCHAR,
    signature_hash        VARCHAR,
    tokens_in             INTEGER NOT NULL DEFAULT 0,
    tokens_out            INTEGER NOT NULL DEFAULT 0,
    latency_ms            INTEGER NOT NULL DEFAULT 0,
    gate_that_rejected    VARCHAR,
    escalated             BOOLEAN NOT NULL DEFAULT FALSE,
    stop_reason           VARCHAR,
    prompt_version        VARCHAR,
    recorded_at           VARCHAR NOT NULL,
    -- Phase 75 — agentic mode. JSON array of {name, args_summary,
    -- duration_ms, result_preview} for every tool call made during THIS
    -- attempt (empty array in oneshot mode) — one JSON column rather than
    -- new scalar columns per field, since the per-call shape is a list.
    tool_calls_json       VARCHAR,
    -- Phase 77 — progressive (chained) multi-patch healing. 1-based link
    -- index within the chain this attempt belongs to; NULL for a normal
    -- (non-progressive) heal attempt. `attempt_num` already carries the
    -- reprompt sequence WITHIN one link, so this is a distinct axis, not a
    -- reuse of an existing column — see AGENTS.md's schema-evolution rule
    -- below for why a new column (not a repurposed one) is correct here.
    chain_link            INTEGER,
    -- Phase 78 — execution engine this attempt targeted ("spark", "duckdb", ...).
    engine                VARCHAR,
    -- Phase 88 Domain 6 — queryable bucket from the DeferToHumanOp's
    -- `defer_reason` enum, when this attempt's patch deferred to a human.
    -- NULL for every non-deferring attempt.
    defer_reason          VARCHAR
);
"""

# Schema-evolution rule: `CREATE TABLE IF NOT EXISTS` is a no-op on an
# existing table — it NEVER adds columns. A new column must therefore land
# in BOTH places: the CREATE above (fresh installs) AND this migrations
# tuple (existing installs), as an idempotent `ADD COLUMN IF NOT EXISTS`
# (supported by DuckDB and Postgres alike). Executed right after the CREATE
# on every Surveyor init.
_HEAL_ATTEMPTS_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE heal_attempts ADD COLUMN IF NOT EXISTS tool_calls_json VARCHAR",
    "ALTER TABLE heal_attempts ADD COLUMN IF NOT EXISTS chain_link INTEGER",
    "ALTER TABLE heal_attempts ADD COLUMN IF NOT EXISTS engine VARCHAR",
    "ALTER TABLE heal_attempts ADD COLUMN IF NOT EXISTS defer_reason VARCHAR",
)

# Phase 78 — `engine` column migrations for the two other pre-existing tables
# that gained it (see the schema-evolution rule above: CREATE TABLE IF NOT
# EXISTS never touches an existing table).
_FAILURE_CONTEXTS_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE failure_contexts ADD COLUMN IF NOT EXISTS engine VARCHAR",
)
_HEALING_OUTCOMES_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE healing_outcomes ADD COLUMN IF NOT EXISTS engine VARCHAR",
)

# Phase 85 D8 — `run_records.engine`, mirroring the Phase-84 `benchmark_results`
# migration pattern (`aqueduct/surveyor/benchmark_store.py`'s
# `_BENCHMARK_RESULTS_MIGRATIONS`): idempotent `ADD COLUMN IF NOT EXISTS`,
# supported identically by DuckDB and Postgres, run right after the CREATE on
# every Surveyor init so a pre-2.2 store gains the column in place instead of
# being orphaned. The index MUST run after the ADD COLUMN, not inside `_DDL`
# above — `CREATE INDEX IF NOT EXISTS run_records(engine)` against a
# pre-existing table that does not have the column YET (before this
# migration runs) raises a BinderException, since `CREATE TABLE IF NOT
# EXISTS` is a no-op there. Order in this tuple matters.
_RUN_RECORDS_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE run_records ADD COLUMN IF NOT EXISTS engine VARCHAR",
    "CREATE INDEX IF NOT EXISTS idx_run_records_engine ON run_records (engine)",
)
