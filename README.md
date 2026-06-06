<p align="center">
  <img src="https://github.com/user-attachments/assets/ceb9dd65-5a20-4325-bdc6-2244417333ea" 
       alt="Aqueduct Logo" 
       width="280" />
</p>

<h1 align="center">Aqueduct</h1>

<p align="center">
  <strong>Self-healing Spark pipelines. Declarative. Observable. Autonomous.</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/aqueduct-core/"><img src="https://img.shields.io/pypi/v/aqueduct-core?style=flat-square" alt="PyPI" /></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-3.11%2B-blue?style=flat-square" alt="Python" /></a>
  <a href="https://github.com/sadigaxund/aqueduct/actions/workflows/test-suite.yml"><img src="https://img.shields.io/github/actions/workflow/status/sadigaxund/aqueduct/test-suite.yml?branch=main&style=flat-square" alt="Test Suite" /></a>
  <a href="https://github.com/sadigaxund/aqueduct/actions/workflows/version-matrix.yml"><img src="https://img.shields.io/github/actions/workflow/status/sadigaxund/aqueduct/version-matrix.yml?branch=main&style=flat-square&label=compatibility" alt="Compatibility Matrix" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square" alt="License" /></a>
</p>

---

**Wake up to a pending patch — not a wall of Spark errors.**

Aqueduct is a control plane for Apache Spark. Define pipelines as YAML Blueprints; Aqueduct compiles, executes, and observes them on Spark — and when they break, an LLM agent autonomously diagnoses and patches them. Pluggable stores (DuckDB / Postgres), watermark-based incremental processing, zero-cost observability, Airflow integration. No DAG code, no on-call digging through logs.

## How It Works

1. You write a **Blueprint** (YAML)
2. Aqueduct **compiles** it into a Manifest
3. **Executor** runs it on Spark
4. **Surveyor** observes everything
5. On failure → **LLM Agent** creates a structured `PatchSpec`
6. Patch goes through guardrails → sandbox → explain gates → applied (or staged for review)

**Model-agnostic by design.** Aqueduct's healing loop works with any LLM — from a local 7B model running on Ollama to Claude Sonnet via API. The constrained PatchSpec grammar (13 deterministic operations, no code generation) means even small models produce valid, guardrail-passing patches for common failures: path typos, format mismatches, column renames, SQL fixes. ~70% of production Spark errors are healable by a 7B model in a single attempt. Advanced features like `deep_loop` (in-conversation sandbox feedback) and multi-model cascades unlock higher heal rates with larger models.

## Core Concepts

| Concept       | Purpose                              |
|---------------|--------------------------------------|
| **Blueprint** | Your pipeline definition             |
| **Channel**   | Transformations (SQL or native ops)  |
| **Spillway**  | Routes bad rows to error sink        |
| **Probe**     | Non-blocking observability taps      |
| **Assert**    | Inline quality gates                 |
| **Depot**     | Cross-run state & watermarks         |
| **Arcade**    | Reusable sub-pipelines               |

Full details in the [documentation](#References).


## The Healing Flow

When a pipeline fails, Aqueduct does not throw the stack trace at an LLM and hope for the best. Healing runs as a staged pipeline. Every stage is auditable, and the LLM works inside a constrained grammar — it cannot write code, mutate files, or run shell commands.

### Modes

Approval mode decides who applies a generated patch. Deterministic guardrails — allowed paths, forbidden operations, minimum confidence — bound every patch regardless of mode.

| Mode | Who applies the patch | When it changes the Blueprint | Use when |
|---|---|---|---|
| `disabled` | LLM never fires | Never | Healing is intentionally off. |
| `human` | Engineer reviews and applies | Only after human accepts | Production. Default behind CI/CD. |
| `ci` | External CI receives patch and opens a PR | Only after merge | Production with code review. |
| `auto` | Aqueduct applies in-memory, re-validates, writes only if the re-run succeeds | Only on a successful re-run | Trusted environments — dev, scoped pipelines. |

Low-confidence patches and any guardrail violation auto-escalate to human review.

### The stages

1. **Capture.** Transient errors retry first. Non-transient failures — schema drift, missing columns, bad paths, OOM — trigger the agent. The Surveyor assembles a self-contained failure package: compiled module config, a provenance map of where every config value came from, a sliced lineage neighbourhood, and a structured root-cause block with the offending column and Spark's own suggestions. The LLM sees an actionable diagnosis, not a JVM trace.

2. **Prune.** A context pruner trims the package to the failure's blast radius. Pruning improves accuracy and latency; cost is not the bottleneck.

3. **Generate.** The LLM responds only with a structured patch — a list of typed operations that map one-to-one to Blueprint edits. Anything else is rejected.

4. **Reprompt.** Schema errors, guardrail violations, and gate rejections feed back into the same conversation as annotated, field-level corrections. The model sees what it wrote and why it was wrong, instead of restarting cold. Slow providers and stuck-signature loops are bounded by a multi-axis budget — wall-clock, tokens, reprompt count, and progress-stall windows — and the loop can honestly defer to a human when no fix is possible, returning a structured diagnosis instead of a hallucinated patch.

5. **Gate.** Before a patch touches the Blueprint, four gates run in order: **guardrails** (path and operation policy), **compile-check** (the patched dict must still produce a valid Manifest), **lineage gate** (column-level diff catches broken references before Spark sees them), and **sandbox gate** (a sampled or full replay catches "parsed but produces nothing"). Sandbox rejections feed back into the same reprompt thread, not a separate retry.

6. **Confirm and write.** Only after every gate passes does the patch run against the real pipeline. The on-disk Blueprint is rewritten only if the full re-run succeeds. Failed patches stage to `patches/pending/` for inspection; successful patches commit to `patches/applied/`, giving every autonomous change a Git-diffable audit trail.

### Why it is reliable

- **No silent mutations.** Every patch is a structured diff with a rationale and a confidence score. Low confidence escalates to human review.
- **No production data corruption.** The sandbox validates patches against representative data before they reach live writes.
- **No runaway loops.** Budgets bound wall-clock, tokens, and stuck-signature counts. A rolling rate-limit caps healing attempts per hour per blueprint.
- **No black-box decisions.** Every LLM turn persists with the gate that rejected it, a stable error signature, and the prompt version. One outer run id joins every iteration of one heal call.

### Why it is efficient

Healing terminates on the first successful patch. Each turn carries only the context the model needs. Structured error extraction replaces multi-kilobyte traces with a short root-cause block. The gate pyramid rejects bad patches in seconds via lineage and sandbox sampling — full-pipeline replay only ever runs against patches that have already passed cheaper checks.

## Getting Started

### Installation

```bash
pip install aqueduct-core[spark]
```

Compose extras as needed — `pip install aqueduct-core[spark,airflow,aws]`:

| Extra | Adds | Install when |
|---|---|---|
| `spark` | PySpark 4 + Delta Lake | Running pipelines on this host. |
| `airflow` | Apache Airflow operator shim | Scheduler / worker host; the box submitting jobs to Spark. |
| `secrets` | AWS + GCP + Azure secret-manager SDKs (or pick `aws` / `gcp` / `azure` individually) | Resolving `@aq.secret('KEY')` against a cloud vault. |
| `stores` | Postgres + Redis backends (or pick `postgres` / `redis` individually) | Replacing single-writer DuckDB defaults for obs / lineage / depot. |
| `all` | Everything above | Single-laptop dev. |

### A first blueprint

```yaml
aqueduct: "1.0"
id: hello.pipeline

macros:
  active: "status = 'active' AND deleted_at IS NULL"

modules:
  - id: load
    type: Ingress
    config: { format: csv, path: "data/in.csv", options: { header: true } }

  - id: clean
    type: Channel
    config:
      op: sql
      query: "SELECT order_id, amount FROM load WHERE {{ macros.active }}"

  - id: save
    type: Egress
    config: { format: parquet, path: "data/out/", mode: overwrite }

edges:
  - { from: load,  to: clean }
  - { from: clean, to: save }

agent:
  approval_mode: human
  budget:
    max_reprompts: 5
    max_seconds: 120
    same_signature_overall: 3
```

Engine-wide defaults live in a separate `aqueduct.yml` (LLM provider, store backends, danger settings). Inline module tests live in `*.aqtest.yml`. Repeatable healing benchmarks live in `*.aqscenario.yml`. The [Gallery](gallery/) has runnable examples of each.

### Five commands to know

1. **`aqueduct doctor blueprints/hello.yml`** — preflight check. Validates YAML, resolves paths, verifies LLM reachability, opens stores.
2. **`aqueduct run blueprints/hello.yml`** — execute the pipeline. On failure, the agent generates a patch under `patches/pending/`.
3. **`aqueduct patch apply patches/pending/<id>.json --blueprint blueprints/hello.yml`** — review and accept a staged patch. Moves it to `patches/applied/`.
4. **`aqueduct test blueprints/hello.aqtest.yml`** — run Channel / Junction / Funnel modules against inline data. No Ingress, no Egress, no external I/O.
5. **`aqueduct benchmark gallery/aqscenarios/ --model claude-sonnet-4-6 --model qwen2.5-coder:7b`** — compare LLM models against simulated failures. No Spark required.

Full reference in [CLI Reference](docs/cli_reference.md).


## References

- **[Blueprint & Engine Spec](docs/specs.md)** — Module types, configs, architecture, healing loop
- **[CLI Reference](docs/cli_reference.md)** — All commands and flags
- **[Spark Guide](docs/spark_guide.md)** — Warnings, performance, tuning
- **[Observability Guide](docs/observability_guide.md)** — Schemas + diagnostic query cookbook
- **[Production Guide](docs/production_guide.md)** — Cluster deployment, security, Delta operations
- **[Compatibility Matrix](docs/compatibility.md)** — Supported Python × Spark versions, pinning recipe
- **[Roadmap](docs/roadmap.md)** — Deferred features and future plans
- **[Gallery](gallery/)** — Real working examples

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md).

**Aqueduct is Apache 2.0 licensed** — free, open source, no telemetry, no lock-in.
