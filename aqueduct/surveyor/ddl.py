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
    parent_run_id  VARCHAR
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
    suggested_columns JSON
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
    -- Phase 45 signature memory: exact failure-signature hash + how the heal
    -- was resolved ('llm' fresh agent patch, 'cached' pending-patch reuse,
    -- 'replayed' zero-token replay of an archived successful patch).
    failure_signature VARCHAR,
    resolution   VARCHAR,
    failure_signature_coarse VARCHAR,
    -- Phase 46: 0-based cascade tier index of the model that produced the
    -- patch; NULL outside multi-model cascade (or when no LLM was involved).
    model_cascade_position INTEGER
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

_EXPLAIN_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS explain_snapshot (
    blueprint_id     VARCHAR NOT NULL,
    run_id           VARCHAR NOT NULL,
    module_id        VARCHAR NOT NULL,
    captured_at      VARCHAR NOT NULL,
    exchange_count   INTEGER NOT NULL,
    python_udf_count INTEGER NOT NULL,
    broadcast_count  INTEGER NOT NULL,
    plan_text        VARCHAR NOT NULL,
    PRIMARY KEY (blueprint_id, run_id, module_id)
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
    tool_calls_json       VARCHAR
);
"""
