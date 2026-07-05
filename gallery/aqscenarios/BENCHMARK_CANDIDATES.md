# Benchmark scenario candidates — the hard classes

*Audit note (2026-07-03): the existing 10 scenarios (01–10) are a solid baseline — they cover the
syntactic/config classes (bad column, wrong format, path typo, type mismatch, guardrail, OOM,
schema merge, broadcast timeout, small files) and are worth keeping as-is. What they don't cover
are the classes below: failures where the error text alone doesn't contain the answer, where the
naive fix is wrong, or where nothing errors at all. These separate a harness-guided small model
from a lucky one-shot. Not implemented — spec sketches for later.*

Grading idea: tag each scenario `tier: baseline | hard | silent` and report heal-rate per tier —
"model X heals 9/10 baseline, 4/12 hard" is the honest benchmark headline.

## Tier: hard — error text is misleading or insufficient

1. **Ambiguous column after self/double join** — `Reference 'id' is ambiguous`. Correct patch
   must alias ONE side and requalify; naive patch renames the wrong side or drops the column.
2. **Ivy/JAR ↔ Spark version mismatch** — `NoSuchMethodError` buried under a
   ServiceConfigurationError wall (the delta-spark 4.0-on-Spark-4.1 case we hit ourselves).
   Correct patch = `spark.jars.packages` coordinate change, NOT a module config change.
3. **Concurrent Delta write conflict** — `ConcurrentAppendException`. Correct answer depends on
   context: retry (transient) vs `partition_by` isolation (structural). Model must choose, and
   choosing retry when partitions overlap is wrong.
4. **Partition-column type mismatch against existing table** — append fails because the existing
   table's partition column is `date` and the new frame has `string`. Fix is a cast in the channel,
   not `mergeSchema: true` (which the model will reach for and which corrupts the layout).
5. **Decimal precision overflow → nulls then downstream NOT NULL assert fails** — the failing
   module is the Assert; the root cause is two modules upstream. Tests cross-module root-causing.
6. **CSV quote/escape/multiline** — row count explodes (embedded newlines) or columns shift.
   Fix is `multiLine`/`quote`/`escape` options; error may be a downstream type error, not a read error.
7. **UDF exception inside the Python worker** — real cause is 40 lines deep in
   `PythonException` executor traceback; driver-side message is generic. Tests trace excavation.
8. **JDBC pushdown type mapping** — e.g. Postgres `numeric` → Spark decimal(38,18) overflow, or
   `timestamptz` semantics. Fix = `customSchema` / query-level cast, not a Spark config.
9. **Case sensitivity** — column exists as `UserID`, query says `userid`, and
   `spark.sql.caseSensitive` interacts. Suggested-columns metadata makes this LOOK easy; correct
   patch must not blindly rename when both spellings exist.
10. **s3a auth chain** — `403` vs `NoCredentialsProvider` vs endpoint-region mismatch: three
    different fixes behind near-identical stack traces (config key placement — our own
    `perf_hadoop_fs_in_options` class — vs env var vs endpoint).

## Tier: silent — nothing errors, data is wrong (heal must be probe/assert-triggered)

11. **Watermark regression / bad incremental state** — depot watermark ahead of data → every run
    reads 0 rows, pipeline "succeeds". Signal: row_count probe trend to zero. Correct action:
    watermark reset, NOT a code patch.
12. **Left-join key type coercion** — `int` joined to `string` key silently null-extends;
    output row count fine, null rate in joined columns spikes. Signal: null_rates probe.
13. **Timezone/DST boundary** — daily aggregation double-counts/drops one hour twice a year.
    Signal: threshold probe on daily totals. The "fix" is session timezone or column tz cast.
14. **Non-deterministic fanout divergence** — our own `nondeterministic_fanout` warning class as a
    runtime scenario: two branches disagree on `uuid()` values. Tests whether the agent connects a
    compile warning to a runtime symptom.
15. **Dedup dropping the wrong duplicates** — `dropDuplicates` without orderBy keeps arbitrary
    rows; downstream totals drift run-to-run. Fix = window + row_number ordering.

## Tier: perf — not broken, slow (future perf-healing track)

16. **Skewed join key** — one task 100× longer; fix = salting or AQE skew-join flags.
17. **collect() in custom probe on full data** — driver OOM only at production scale.
18. **Shuffle partition explosion** — 200 defaults on tiny data / too few on huge data;
    fix = AQE or explicit repartition. Pairs with `explain_snapshot` plan-derived counts.

## Notes for implementation

- Silent-tier scenarios need `expected_action` beyond a PatchSpec (watermark reset, advisory) —
  may need a scenario-schema extension.
- Hard-tier 2 and 10 are infra-class: patches target `spark_config`/session, which the current
  patch grammar may not fully address — that's the point (they also motivate the
  spark_config-ops grammar extension; see TODOs Phase 70+ notes).
- Each scenario should ship with a `red_herring:` field documenting the WRONG fix a naive model
  proposes — grading can then distinguish "healed" from "healed correctly".
