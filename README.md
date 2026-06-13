<p align="center">
  <img src="https://github.com/user-attachments/assets/ceb9dd65-5a20-4325-bdc6-2244417333ea" 
       alt="Aqueduct Logo" 
       width="280" />
</p>

<h1 align="center">Aqueduct</h1>

<p align="center">
  <strong>Self-healing Spark pipelines. Declarative. Observable. Autonomous.</strong>
</p>

<!-- Badges: two deliberate rows so a long badge can never orphan the last one onto its own line. -->
<p align="center">
  <a href="https://pypi.org/project/aqueduct-core/"><img src="https://img.shields.io/pypi/v/aqueduct-core?style=flat-square&logo=pypi&logoColor=white&color=2563eb" alt="PyPI" /></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-3.11%2B-2563eb?style=flat-square&logo=python&logoColor=white" alt="Python" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-2563eb?style=flat-square" alt="License" /></a>
  <br/>
  <a href="https://github.com/sadigaxund/aqueduct/actions/workflows/test-suite.yml"><img src="https://img.shields.io/github/actions/workflow/status/sadigaxund/aqueduct/test-suite.yml?branch=main&style=flat-square&logo=githubactions&logoColor=white&label=tests" alt="Test Suite" /></a>
  <a href="https://github.com/sadigaxund/aqueduct/actions/workflows/version-matrix.yml"><img src="https://img.shields.io/github/actions/workflow/status/sadigaxund/aqueduct/version-matrix.yml?branch=main&style=flat-square&label=compatibility" alt="Compatibility Matrix" /></a>
  <!-- TODO(maturity): set the real project status. Options: experimental · alpha · beta · stable -->
  <img src="https://img.shields.io/badge/status-beta-orange?style=flat-square" alt="Project status: beta" />
  <a href="https://github.com/sadigaxund/aqueduct/stargazers"><img src="https://img.shields.io/github/stars/sadigaxund/aqueduct?style=flat-square&logo=github&color=2563eb" alt="Stars" /></a>
</p>

---

## Why Aqueduct

A Spark job fails at 3 a.m. on a column rename upstream. Today that means a paged engineer scrolling a four-kilobyte JVM stack trace to find a one-line fix. Aqueduct turns it into a Git-diffable patch waiting for review in the morning.

<kbd>
  <img width="1600" height="764" alt="Screenshot 2026-06-13 at 05-23-26 (1)" src="https://github.com/user-attachments/assets/dedd5af5-aa59-4f09-9fa8-9632b02a6894" />
</kbd>  

<br><br>

- **Declarative, not DAG code.** Pipelines are YAML *Blueprints* — no PySpark boilerplate, no scheduler glue, no operator classes. Bring your own scheduler; Aqueduct is the control plane on top of Spark.
- **Self-healing, not just alerting.** On failure an LLM agent diagnoses the root cause and emits a structured patch that passes guardrail, lineage, and sandbox gates *before* it touches your pipeline. No codegen, no shell access, no silent mutation — and a failure it has solved before heals from memory with **zero tokens**.
- **Observable by construction.** Every run, every heal attempt, every column-lineage edge lands in a queryable store — at zero extra Spark actions on the hot path.
- **Model-agnostic.** Any LLM — a local 7B on Ollama up to a frontier API. The constrained patch grammar (14 deterministic operations, no code generation) is what keeps small models reliable; multi-model cascades escalate to bigger models only when needed.

> **Wake up to a pending patch — not a wall of Spark errors.**

---

## Table of Contents

- [What You Get](#what-you-get)
- [Core Concepts](#core-concepts)
- [The Healing Flow](#the-healing-flow)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [How It Compares](#how-it-compares)
- [References](#references)
- [Contributing](#contributing)

---

## What You Get

| Capability | What it does | Details |
|---|---|---|
| **Self-healing** | LLM diagnoses failures, emits gated, Git-diffable patches — human, CI, or auto approval | [Spec §8](docs/specs.md) |
| **Heal memory** | Failure signatures cache validated fixes — repeat failures heal with zero LLM tokens | [Spec §8.2](docs/specs.md) |
| **Observability store** | Runs, failures, heal attempts, metrics in queryable DuckDB/Postgres | [Observability Guide](docs/observability_guide.md) |
| **Column lineage** | Compile-time, zero Spark actions; powers the patch lineage gate | [Spec §7](docs/specs.md) |
| **Data quality** | Inline `Assert` rules + Spillway quarantine — bad rows routed, not dropped, typed like catch blocks | [Spec §5](docs/specs.md) |
| **Module tests** | `aqueduct test` runs transforms against inline fixtures — no I/O, no cluster | [CLI Reference](docs/cli_reference.md) |
| **LLM benchmark** | `aqueduct benchmark` scores models against simulated failures — pick the cheapest model that heals your pipelines | [CLI Reference](docs/cli_reference.md) |
| **Safety rails** | Guardrails, multi-axis budgets, hourly heal caps, sandbox replay before any live write | [Spec §8.3](docs/specs.md) |

## Core Concepts

| Concept       | Purpose                                       |
|---------------|-----------------------------------------------|
| **Blueprint** | Your pipeline definition                      |
| **Ingress**   | Reads sources (CSV, Parquet, Delta, JDBC)     |
| **Channel**   | Transformations (SQL or native ops)           |
| **Egress**    | Writes sinks (overwrite, append, Delta merge) |
| **Junction**  | Fan-out (conditional, broadcast, partition)   |
| **Funnel**    | Fan-in (unions, coalesce, zip)                |
| **Spillway**  | Routes bad rows to error sink                 |
| **Probe**     | Non-blocking observability taps               |
| **Assert**    | Inline quality gates                          |
| **Depot**     | Cross-run state & watermarks                  |
| **Arcade**    | Reusable sub-pipelines                        |

Full details in the [References](#references).

## The Healing Flow

When a pipeline fails, Aqueduct does not throw a stack trace at an LLM and hope. Healing is a staged, auditable pipeline, and the model works inside a constrained grammar — it cannot write code, mutate files, or run shell commands.

<!--
  TODO(demo): terminal GIF / asciinema cast goes HERE — proof next to the claim.
  Ideal clip (~20s): `aqueduct run` hits a schema-drift failure, the agent
  generates a patch, gates pass, the re-run goes green.
-->
<p align="center">
  <em>📽️ Demo coming soon — a live heal, start to green, in under 20 seconds.</em>
</p>

<kbd>
  
<img width="1362" height="1410" alt="Screenshot 2026-06-13 at 05-28-42 (1)(1)" src="https://github.com/user-attachments/assets/29bd3782-ec31-4666-9cb3-3ee8690b38b2" />

</kbd>

### Approval modes

Who applies a generated patch. Deterministic guardrails — allowed paths, forbidden operations, minimum confidence — bound every patch regardless of mode.

| Mode | Who applies the patch | When the Blueprint changes | Use when |
|---|---|---|---|
| `disabled` | LLM never fires | Never | Healing is intentionally off. |
| `human` | Engineer reviews and applies | Only after human accepts | Production. Default behind CI/CD. |
| `ci` | External CI receives patch, opens a PR | Only after merge | Production with code review. |
| `auto` | Aqueduct applies in-memory, re-validates, writes only if the re-run succeeds | Only on a successful re-run | Trusted environments — dev, scoped pipelines. |

Low-confidence patches and any guardrail violation auto-escalate to human review.

<details>
<summary><strong>What a patch looks like</strong> (click to expand)</summary>

Every patch is a `PatchSpec` — a structured, Git-diffable JSON document staged under `patches/pending/`. No code, no shell, just declarative operations against the Blueprint:

```jsonc
// patches/pending/hello-pipeline-20260611T031412.json (abridged)
{
  "patch_id": "hello-pipeline-20260611T031412",
  "run_id": "9f3c2e1a",
  "category": "config_error",
  "root_cause": "Ingress 'load' reads data/in.csv, but the upstream job renamed the file to data/input.csv.",
  "confidence": 0.92,
  "rationale": "PATH_NOT_FOUND on data/in.csv; a sibling data/input.csv exists with a matching schema, so the path is stale rather than the data missing.",
  "operations": [
    { "op": "set_module_config_key", "module_id": "load", "key": "path", "value": "data/input.csv" }
  ]
}
```

Review it, then `aqueduct patch apply` — or let `auto` mode validate and apply it for you.

</details>

### Why it holds up

- **No silent mutations.** Every patch is a structured diff with a rationale and a confidence score; low confidence escalates.
- **No production data corruption.** The sandbox validates patches against representative data before any live write.
- **No runaway loops.** A multi-axis budget bounds wall-clock, tokens, reprompt count, and stuck-signature windows; a rolling rate-limit caps heals per hour per blueprint.
- **No black-box decisions.** Every LLM turn persists with the gate that rejected it, a stable error signature, and the prompt version. One run id joins every iteration of a heal.
- **Efficient.** Healing stops on the first successful patch. Structured error extraction replaces multi-kilobyte traces with a short root-cause block, and cheap lineage/sandbox checks reject bad patches in seconds before any full-pipeline replay.

For the stage-by-stage detail, see the [Blueprint & Engine Spec](docs/specs.md).

## Architecture

Aqueduct is a single CLI that runs on the Spark driver — no servers, no daemons. Logic flows through four immutable layers:

<kbd>
<img width="1343" height="663" alt="Screenshot 2026-06-13 at 05-31-32 (1)(1)" src="https://github.com/user-attachments/assets/d3f00605-2322-4a7b-a3e8-583bbf932d88" />
</kbd>
<br>

- **Parser** validates YAML into an immutable AST.
- **Compiler** resolves context, expands Arcades and macros, extracts column lineage, and assembles a fully-resolved Manifest.
- **Executor** runs the Manifest on Spark; all PySpark code is isolated under `executor/spark/`.
- **Surveyor** records runs, failures, and lineage to pluggable stores and triggers the Agent on failure.

## Getting Started

### Installation

```bash
pip install aqueduct-core[spark]
```

> **Requirements:** Python 3.11+ · Java 17 for the `spark` extra (`JAVA_HOME` must point to it).
> Every release is CI-tested against three pinned combos — **LTS** (Python 3.11 · Spark 4.1), **Latest** (Python 3.13 · Spark 4.1), **Legacy** (Python 3.12 · Spark 3.5). Live results in the [Compatibility Matrix](docs/compatibility.md).

Compose extras as needed — `pip install aqueduct-core[spark,airflow,aws]`:

| Extra | Adds | Install when |
|---|---|---|
| `spark` | PySpark 4 + Delta Lake | Running pipelines on this host. |
| `airflow` | Apache Airflow operator shim | Scheduler / worker host; the box submitting jobs to Spark. |
| `secrets` | AWS + GCP + Azure secret-manager SDKs (or pick `aws` / `gcp` / `azure` individually) | Resolving `@aq.secret('KEY')` against a cloud vault. |
| `stores` | Postgres + Redis backends (or pick `postgres` / `redis` individually) | Replacing single-writer DuckDB defaults for obs / lineage / depot. |
| `llm` | `json-repair` — last-ditch recovery of malformed LLM patch JSON | Healing with small local models that emit imperfect JSON. |
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

## How It Compares

<!-- TODO(positioning): tighten these one-liners with real benchmark/feature claims once verified. -->

| | Aqueduct | dbt | Dagster / Airflow | Raw PySpark |
|---|---|---|---|---|
| Pipeline definition | Declarative YAML Blueprints | SQL models | Python DAG code | Imperative code |
| Engine | Apache Spark | Warehouse SQL | Orchestration only | Apache Spark |
| On failure | **Autonomous LLM patch + gates** | Manual fix | Retry / alert | Manual fix |
| Column lineage | Built-in, compile-time | Built-in | Plugin | DIY |
| Built-in observability store | Yes (DuckDB/Postgres) | Partial | External | DIY |

Aqueduct sits where a Spark transformation engine and an autonomous repair loop meet — it is not a scheduler (pair it with Airflow via the `airflow` extra) and not a warehouse SQL tool.

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
