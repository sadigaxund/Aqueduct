# Spark Performance & Behavior Guide for Aqueduct

This document summarizes critical Spark behaviors that affect Aqueduct's design. Refer to it when implementing or modifying Executor modules.

## Core Principles

- **Lazy Evaluation**: DataFrames are immutable plans. No computation until an action (e.g., `.write`, `.count`).
- **Narrow vs Wide Transformations**: Narrow (filter, map) keep data in partitions; wide (groupBy, join, distinct) cause shuffles.
- **Predicate Pushdown**: Spark pushes filters to the source when using SQL or DataFrame expressions. Use `spark.sql()` or `expr()` to leverage this.

## Aqueduct‑Specific Rules (DO NOT VIOLATE)

1. **Never add Spark actions in the critical path.**  
   - Probes: use `SparkListener` (zero‑cost) or explicit `.sample()` actions.  
   - Never call `.count()`, `.collect()`, `.show()` on the main DataFrame.

2. **Performance-degrading metric actions require explicit user opt-in.**  
   - Any `df.count()`, `df.collect()`, or extra scans added for observability MUST be gated behind a named flag in `aqueduct.yml` under the `metrics:` section.  
   - Default value is always `False` (zero-cost production mode).  
   - **You MUST emit a visible `click.echo()` warning at run start when any such flag is enabled.** The warning must name the flag, the cost, and tell the user to disable in production. This rule is non-negotiable — no silent performance degradation.  
   - In code: check the flag at the call site; never enable extra actions unconditionally.  
   - Current flags: none. `MetricsConfig` in `config.py` is an empty skeleton — add flags here when warranted.

3. **Use SQL expressions over Python UDFs whenever possible.**  
   - Python UDFs force expensive row‑by‑row serialization.  
   - LLM patches should favor SQL `CASE`, `RLIKE`, built‑ins.

4. **Validate `schema_hint` explicitly.**  
   - Spark's `nullable=false` is not enforced—it's only a hint.  
   - Ingress must compare `df.schema` against the hint and raise on mismatch.

5. **Union by name, not position.**  
   - Funnel `union_all` uses `unionByName` with `allowMissingColumns` in permissive mode.

6. **Avoid unintentional shuffles.**  
   - Junction uses `filter` (narrow).  
   - Funnel `union_all` is narrow; `union` (distinct) is wide.  
   - Performance tests compare baseline explain plans to catch regressions.

## Common Pitfalls & Mitigations

| Pitfall | Mitigation in Aqueduct |
| :--- | :--- |
| Coalesce can create skewed partitions | If implementing `op: coalesce`, document trade‑off; prefer `repartition` for balance. |
| Date parsing returns `null` on failure | `schema_hint` catches type mismatches; users should validate formats in SQL. |
| Python UDFs cause memory contention | Discourage; use SQL built‑ins or register Scala UDFs in Phase 8. |
| Catalyst cost model chooses join strategies | Provide `spark_config` overrides (`spark.sql.autoBroadcastJoinThreshold`) but do not force hints. |

## Reference: Transformation Types

| Operation | Narrow/Wide | Shuffle? |
| :--- | :--- | :--- |
| `filter`, `select` | Narrow | No |
| `unionByName` | Narrow | No |
| `distinct` | Wide | Yes |
| `groupBy`, `join` | Wide | Yes |
| `repartition` | Wide | Yes |
| `coalesce` | Narrow | No (but may cause skew) |