# 7. Lineage

# **7. Lineage**

## **7.1 Two lineage layers**

| Layer | Computed at | Purpose |
| :- | :- | :- |
| **Structural Lineage** | Compile time (static) | Column-level DAG of the Blueprint. Used by the lineage gate and the LLM. |
| **Runtime Flow Report** | Post-run | Per-column quality metrics from Probe signals. |

## **7.2 Structural Lineage**

Structural lineage is computed at compile time (in `aqueduct/compiler/lineage.py`, zero Spark actions) by `sqlglot` analysis of Channel SQL queries. It maps each output column to its upstream source module and column. Stored in the `column_lineage` table inside `observability.db`. Used by:

- **Lineage gate:** Before a patch is applied, the lineage of the patched Blueprint is compared to the original. Lost columns or broken references are flagged.
- **LLM context:** The structural lineage for the failed module's neighbourhood is included in the FailureContext. The agent can trace column origins from it without accessing the original Spark session.

### Channel SQL fingerprints

Each `op: sql` Channel also gets a **normalised AST fingerprint**, sqlglot
canonicalises the query (formatting/comment/keyword-case insensitive) and the
SHA-256 of the canonical form is recorded in `channel_fingerprints`. The table
is a *changelog*: a new row appears only when a Channel's SQL changes
*semantically* (a reformat does not), so it answers "did this transform change,
and when" without storing one row per run.

### Type-tracked column chains

`aqueduct lineage <bp.yml> --chain <column> --types` traces a single column
source→output, annotating each hop with an sqlglot-inferred SQL `output_type`
and a `transform_op` (`passthrough` | `rename` | `CAST` | `CONCAT` | a function
name | `literal` | `expression`) and flagging type changes. It is computed **on
demand** from the compiled Manifest: nothing extra is persisted and no Spark
action runs. This is a human debugging tool ("why is this column a string now",
rename-impact, type-drift); it is **not** part of the healing loop, which
already reads full SQL from the manifest. sqlglot resolves ~90% of SparkSQL
expressions; the rest fall back to `output_type=UNKNOWN`.

## **7.3 Runtime Flow Report**

Generated post-run from Probe signals. Shows per-column, per-Module status (OK / Degraded / Error) with null rates, row estimates, schema snapshots, and thresholds. `aqueduct report --trend <column> --blueprint <id>` adds a **cross-run** view of one column's null-rate and type history, a read-side aggregate over `probe_signals` (no extra table).

