# aqscenarios — planned scenarios (temporary)

Each scenario = a Blueprint + an injected failure + expected-patch
assertions, run via `aqueduct benchmark --scenarios
gallery/aqscenarios/suite-01-healing/`. See `aqscenario.yml.template`
for the file shape.

Categories below are the **canonical `expected_category` values** the
Agent must classify into (from `aqueduct/surveyor/scenario.py`). Long-
term target per the README is 20–30 scenarios; this is the seed set —
~2 per category covering the highest-frequency real failures.

Layout: `suite-01-healing/<id>.aqscenario.yml`, blueprints shared under
`suite-01-healing/blueprints/`.

## schema_drift
- [ ] `schema_drift_column_rename` — upstream renamed `event_ts` →
      `event_time`; downstream SQL still selects `event_ts`.
- [ ] `schema_drift_missing_column` — a column the Channel selects no
      longer exists in the source.

## sql_column_not_found  (your "misreferenced table/column")
- [ ] `sql_bad_column_ref` — Channel SQL references a column that isn't
      in any upstream module.
- [ ] `sql_bad_table_ref` — Channel SQL `FROM`s a module id that isn't
      an upstream edge.

## type_mismatch  (your "wrong type when reading a source")
- [ ] `type_string_vs_numeric` — amount read as string, arithmetic /
      aggregation downstream fails.
- [ ] `type_bad_cast` — explicit `CAST` to an incompatible type.

## bad_path
- [ ] `bad_path_typo` — Ingress path has a typo / missing partition.
- [ ] `bad_path_ctx_unresolved` — path uses `${ctx.*}` that was never
      provided.

## format_mismatch
- [ ] `format_csv_read_as_parquet` — source is CSV, Ingress declares
      `format: parquet` → "Unable to infer schema".

## oom_config
- [ ] `oom_executor_memory` — failure resolved by an executor-memory /
      shuffle-partitions config patch (assert `forbidden_ops` excludes
      full-config replacement).

## missing_context
- [ ] `missing_context_value` — Blueprint references a context key with
      no default and none supplied.

## Your "bad SQL syntax" hint
- [ ] `sql_syntax_error` — malformed Channel SQL (unbalanced paren /
      bad keyword). Likely classifies as `other`; useful to confirm the
      Agent still produces a valid, minimal patch.

## permission_error / other
- [ ] (defer) `permission_denied` — S3/FS 403 on read. Add once a
      higher-value category is exhausted.

When the seed set is green in `aqueduct benchmark`, record the baseline
in `aqscenarios/README.md` and delete this file.
