# 6. Observability, Probes and Flow Report

# **6. Observability, Probes & Flow Report**

See the dedicated **[Observability Guide](../observability_guide.md)** for:

- Full schema reference for `observability.db` and `benchmark.duckdb`
- Diagnostic query cookbook (run post-mortem, heal-loop forensics, cost analysis)
- Probe signal reference and cost model
- Store backend configuration (DuckDB / Postgres / Redis)

### Key design constraint

All observability is governed by one rule: **no Spark actions may be added to the critical execution path** beyond what the Blueprint configures. Default Probe signals (`schema_snapshot`, `execution_partitions`, `row_count_estimate` via SparkListener) are zero-cost. Sample-based signals (`null_rates`, `value_distribution`) require explicit opt-in.

---

