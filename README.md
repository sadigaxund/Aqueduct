<p align="center">
  <img src="https://github.com/user-attachments/assets/ceb9dd65-5a20-4325-bdc6-2244417333ea" 
       alt="Aqueduct Logo" 
       width="280" />
</p>

<h1 align="center">Aqueduct</h1>

<p align="center">
  <strong>Self-healing data pipelines. Declarative. Observable. Autonomous.</strong>
  <br/>
  <strong>One blueprint. Spark or DuckDB. Your data never leaves your servers.</strong>
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

A pipeline fails at 3 a.m. over a column rename upstream. Somebody gets paged. They scroll a four-kilobyte stack trace to find a one-line fix. Aqueduct turns that night into a Git-diffable patch, waiting for review in the morning.


<img width="1280" height="720" alt="ezgif-3d9a30cf90eb278e" src="https://github.com/user-attachments/assets/92acb8fb-80ad-4cac-b5bd-e00a114d22cb" />

>
> **Wake up to a pending patch instead of a wall of errors.**
>
<br>

- **Declarative, not DAG code.** Pipelines are YAML *Blueprints*. No PySpark boilerplate, no operator classes. Bring your own scheduler; Aqueduct is the control plane on top of the engine.
- **Self-healing rather than alerting.** On failure, an LLM agent diagnoses the root cause and emits a structured patch. The patch must clear guardrail, lineage, and sandbox gates before it touches your pipeline. The agent cannot generate code or touch a shell. A failure it has solved before heals from memory, with zero LLM tokens.
- **Observable by construction.** Every run, heal attempt, and column-lineage edge lands in a queryable store. Nothing is added to the hot path to make that happen.
- **Model-agnostic and local-first.** Anthropic natively, or any OpenAI-compatible endpoint: OpenRouter, DeepSeek, Groq, a local 7B on Ollama or LM Studio. Set `provider` and `base_url`, done. Point it at a model inside your perimeter and your data, schemas, and error traces never leave your servers. Multi-model cascades escalate to a bigger model only when the small one gets stuck.
- **The harness owns correctness; the model only proposes.** The LLM does exactly one thing deterministic code cannot: turn an unstructured failure into a structured hypothesis. Everything after that is deterministic. A constrained patch grammar with 14 operations and no code generation. Validation gates. Budget caps. A signature cache. That division of labor is why a small local model holds up here when raw code-generating assistants don't.


---

## Table of contents

- [Supported engines](#supported-engines)
- [What you get](#what-you-get)
- [Core concepts](#core-concepts)
- [The healing flow](#the-healing-flow)
- [Architecture](#architecture)
- [Observability dashboard](#observability-dashboard)
- [Getting started](#getting-started)
- [How it compares](#how-it-compares)
- [References](#references)
- [Contributing](#contributing)

---

## Supported engines

<table align="center">
  <tr>
    <td align="center" width="320">
      <img src="https://cdn.simpleicons.org/apachespark/E25A1C" width="56" alt="Apache Spark" /><br/><br/>
      <strong>Apache Spark</strong><br/>
      <sub>Distributed batch at cluster scale.<br/>
      Delta Lake, JDBC, custom data sources,<br/>
      the full Blueprint grammar.</sub>
    </td>
    <td align="center" width="320">
      <img src="https://cdn.simpleicons.org/duckdb/FFF000" width="56" alt="DuckDB" /><br/><br/>
      <strong>DuckDB</strong><br/>
      <sub>Single-node production, zero JVM.<br/>
      Ships with the base install.<br/>
      Same blueprint, same CLI, same healing loop.</sub>
    </td>
  </tr>
</table>

<p align="center">
  <code>aqueduct run pipeline.yml --set deployment.engine=duckdb</code>
</p>

One blueprint format, one CLI, per-engine execution. Each engine declares a capability table, and the compiler checks your blueprint against it. If the target engine can't do something, you get a compile error naming the exact capability instead of a runtime surprise.

Three promises, each enforced by machinery rather than review discipline:

- Every `supported` entry in the [capability matrix](docs/compatibility.md) is backed by a named test that runs on that engine in CI.
- Nothing that can change your results is ever silently ignored. The engine honors it, warns about it, or refuses to compile.
- Healed blueprints record which engine produced each patch. Deploy a DuckDB-healed blueprint to Spark and the compiler tells you.

The two engines do not promise identical values, and Aqueduct does not pretend they do. SQL dialects genuinely differ. Your `Assert` rules run on both engines, so the properties you care about are the properties that get checked. That is your portability contract.

## What you get

| Capability | What it does | Details |
|---|---|---|
| **Self-healing** | LLM diagnoses failures, emits gated, Git-diffable patches with human, CI, or auto approval | [Spec §8](docs/specs.md) |
| **Heal memory** | Failure signatures cache validated fixes; repeat failures heal with zero LLM tokens | [Spec §8.2](docs/specs.md) |
| **Engine capability gate** | Unsupported features fail at compile time with a named capability, never mid-run | [Compatibility Matrix](docs/compatibility.md) |
| **Portable types** | One type vocabulary across engines; ambiguous spellings rejected at parse time | [Spec §9](docs/specs.md) |
| **Heal provenance** | Blueprints record which engine healed them; cross-engine deploys warn at compile | [Spec §8.14](docs/specs.md) |
| **Observability store** | Runs, failures, heal attempts, metrics in queryable DuckDB/Postgres | [Observability Guide](docs/observability_guide.md) |
| **Column lineage** | Compile-time, zero engine actions; powers the patch lineage gate | [Spec §7](docs/specs.md) |
| **Data quality** | Inline `Assert` rules + Spillway quarantine: bad rows are routed to a typed error sink instead of being dropped | [Spec §4.4](docs/specs.md) |
| **Module tests** | `aqueduct test` runs transforms against inline fixtures, with no I/O and no cluster | [CLI Reference](docs/cli_reference.md) |
| **LLM benchmark** | `aqueduct benchmark` scores models against simulated failures so you can pick the cheapest model that heals your pipelines | [CLI Reference](docs/cli_reference.md) |
| **Safety rails** | Guardrails, multi-axis budgets, hourly heal caps, sandbox replay before any live write | [Spec §8.3](docs/specs.md) |
| **Observability dashboard** | `aqueduct dashboard`, a local read-only Streamlit viewer: fleet, runs, lineage, healing patches with before/after diff, performance, quality | [Observability Guide](docs/observability_guide.md) |

## Core concepts

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
| **Regulator** | Gate driven by Probe signals (skip / abort / trigger agent) |
| **Assert**    | Inline quality gates                          |
| **Depot**     | Cross-run state & watermarks                  |
| **Arcade**    | Reusable sub-pipelines                        |

Full details in the [References](#references).

## The healing flow

When a pipeline fails, Aqueduct does not throw a stack trace at an LLM and hope. Healing is a staged, auditable pipeline. The model works inside a constrained grammar: it cannot write code, edit files, or run shell commands.

A generated patch clears five gates before it ever touches the Blueprint. Guardrails first: deterministic policy checks on paths, operations, and confidence. Then compile-check: the patched Blueprint must still parse. Then lineage: does the patch break a downstream column consumer. Then sandbox: replay against representative data. Then a plan-regression check. They run in that order and the first failure wins:

```
✓ guardrails  →  ✓ compile-check  →  ✓ lineage  →  ✓ sandbox  →  ✓ plan-regression  →  patch applied
```

Every patch clears the pyramid before it touches the Blueprint. `aqueduct patch preview --sandbox` runs the same pyramid on demand, before you decide to apply.

<kbd>
  
<img width="1362" height="1410" alt="Figure 1: The Healing Flow" src="https://github.com/user-attachments/assets/29bd3782-ec31-4666-9cb3-3ee8690b38b2" />

</kbd>

### Approval modes

Who applies a generated patch. Deterministic guardrails (allowed paths, forbidden operations, minimum confidence) bound every patch regardless of mode.

| Mode | Who applies the patch | When the Blueprint changes | Use when |
|---|---|---|---|
| `disabled` | LLM never fires | Never | Healing is intentionally off. |
| `human` | Engineer reviews and applies | Only after human accepts | Production. Default behind CI/CD. |
| `ci` | External CI receives patch, opens a PR | Only after merge | Production with code review. |
| `auto` | Aqueduct applies in-memory, re-validates, writes only if the re-run succeeds | Only on a successful re-run | Trusted environments: dev, scoped pipelines. |

Low-confidence patches and any guardrail violation auto-escalate to human review.

<details>
<summary><strong>What a patch looks like</strong> (click to expand)</summary>

Every patch is a `PatchSpec`, a structured, Git-diffable JSON document staged under `patches/pending/`. It contains declarative operations against the Blueprint, never code or shell commands:

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

Review it, then `aqueduct patch apply`, or let `auto` mode validate and apply it for you.

</details>

### Why it holds up

- **Every change is visible.** A patch is a structured diff with a rationale and a confidence score. Low confidence escalates to a human.
- **Live data stays safe.** The sandbox validates each patch against representative data before any live write.
- **Loops are bounded.** A multi-axis budget caps wall-clock time, tokens, reprompts, and stuck-signature windows. A rolling rate limit caps heals per hour per blueprint.
- **Decisions are auditable.** Every LLM turn is recorded with the gate that rejected it, a stable error signature, and the prompt version. One run id joins every iteration of a heal.
- **Efficient.** Healing stops on the first successful patch. Structured error extraction replaces multi-kilobyte traces with a short root-cause block. Cheap lineage and sandbox checks reject bad patches in seconds, before any full-pipeline replay.

For the stage-by-stage detail, see the [Blueprint & Engine Spec](docs/specs.md).

## Architecture

Where Aqueduct sits in a data platform: the control plane between your scheduler and the engine.

<kbd>
  <img width="1600" height="764" alt="Figure 2: Aqueduct at a glance" src="https://github.com/user-attachments/assets/dedd5af5-aa59-4f09-9fa8-9632b02a6894" />
</kbd>

<br><br>

Inside the box, Aqueduct is a single CLI that runs on the driver, with no servers and no daemons. Logic flows through four immutable layers:

<kbd>
<img width="1343" height="663" alt="Figure 3: Architecture" src="https://github.com/user-attachments/assets/d3f00605-2322-4a7b-a3e8-583bbf932d88" />
</kbd>
<br>

- **Parser** validates YAML into an immutable AST.
- **Compiler** resolves context, expands Arcades and macros, extracts column lineage, gates the blueprint against the target engine's capabilities, and assembles a fully-resolved Manifest.
- **Executor** runs the Manifest on the target engine. Engines register through an entry-point protocol; Spark code is isolated under `executor/spark/`, DuckDB under `executor/duckdb_/`. The core never imports an engine by name.
- **Surveyor** records runs, failures, and lineage to pluggable stores and triggers the Agent on failure.

## Observability dashboard

`aqueduct dashboard` launches a local, read-only Streamlit viewer over the same observability store the engine writes to. It runs on demand, like the Spark UI. It is not a production server, and no pipeline requires it. One place for fleet health, per-run module metrics, column lineage, the self-heal patch stream with before/after diffs, performance trends, and data-quality signals across every blueprint. Backend-agnostic (DuckDB or Postgres). Every view re-reads with short-lived connections, so it can't block a running pipeline's writer.

```bash
pip install "aqueduct-core[dashboard]"
aqueduct dashboard            # opens http://localhost:8501
```

<!--
  TODO(gallery): drop screenshots at docs/media/dashboard/*.png, then DELETE the
  "coming soon" <p> below and uncomment the table.
  Use static PNGs, NOT a GIF — the dashboard is dense (tables/charts); stills read
  sharper and load faster than a blurry tab-cycling GIF. Suggested four below.
-->
<!--
| | |
|---|---|
| <img src="docs/media/dashboard/runs.png"        alt="Runs → run detail (module metrics)" />   | <img src="docs/media/dashboard/lineage.png"     alt="Column-lineage Sankey + SQL changelog" /> |
| <img src="docs/media/dashboard/healing.png"     alt="Healing → patch before/after diff" />     | <img src="docs/media/dashboard/performance.png" alt="Performance → cost-vs-data trends" />      |
-->
## Getting started

### Installation

```bash
pip install aqueduct-core              # DuckDB engine included, no JVM needed
pip install "aqueduct-core[spark]"     # adds Apache Spark + Delta Lake
```

> **Requirements:** Python 3.11+. Java 17 for the `spark` extra only (`JAVA_HOME` must point to it).
> Every release is CI-tested against three pinned combos: LTS (Python 3.11 · Spark 4.1), Latest (Python 3.13 · Spark 4.1), Legacy (Python 3.12 · Spark 3.5). Live results in the [Compatibility Matrix](docs/compatibility.md).

Compose extras as needed, for example `pip install "aqueduct-core[spark,airflow,aws]"`:

| Extra | Adds | Install when |
|---|---|---|
| `spark` | PySpark 4 + Delta Lake | Running pipelines on Spark on this host. |
| `airflow` | Apache Airflow operator shim | Scheduler / worker host; the box submitting jobs. |
| `secrets` | AWS + GCP + Azure secret-manager SDKs (or pick `aws` / `gcp` / `azure` individually) | Resolving `@aq.secret('KEY')` against a cloud vault. |
| `stores` | Postgres + Redis backends (or pick `postgres` / `redis` individually) | Replacing single-writer DuckDB defaults for obs / lineage / depot. |
| `llm` | `json-repair`, last-ditch recovery of malformed LLM patch JSON | Healing with small local models that emit imperfect JSON. |
| `all` | Everything above | Single-laptop dev. |

### A first blueprint

```yaml
aqueduct: "1.0"
id: hello.pipeline
name: Hello Pipeline

macros:
  active: "status = 'active' AND deleted_at IS NULL"

modules:
  - id: load
    type: Ingress
    label: Load orders
    config: { format: csv, path: "data/in.csv", options: { header: true } }

  - id: clean
    type: Channel
    label: Filter active
    config:
      op: sql
      query: "SELECT order_id, amount FROM load WHERE {{ macros.active }}"

  - id: save
    type: Egress
    label: Write parquet
    config: { format: parquet, path: "data/out/", mode: overwrite }

edges:
  - { from: load,  to: clean }
  - { from: clean, to: save }

agent:
  approval: human
```

Run it on either engine:

```bash
aqueduct run blueprints/hello.yml                                  # engine from aqueduct.yml
aqueduct run blueprints/hello.yml --set deployment.engine=duckdb   # no cluster, no JVM
```

Engine-wide defaults live in a separate `aqueduct.yml` (target engine, LLM provider, store backends, danger settings). Inline module tests live in `*.aqtest.yml`. Repeatable healing benchmarks live in `*.aqscenario.yml`. The [Gallery](gallery/) has runnable examples of each.

### Five commands to know

1. **`aqueduct doctor blueprints/hello.yml`** is the preflight check. It validates YAML, resolves paths, verifies LLM reachability, and opens stores.
2. **`aqueduct run blueprints/hello.yml`** executes the pipeline. On failure, the agent generates a patch under `patches/pending/`.
3. **`aqueduct patch apply patches/pending/<id>.json --blueprint blueprints/hello.yml`** reviews and accepts a staged patch, moving it to `patches/applied/`.
4. **`aqueduct test blueprints/hello.aqtest.yml`** runs Channel / Junction / Funnel modules against inline data, without touching Ingress, Egress, or any external I/O.
5. **`aqueduct benchmark gallery/aqscenarios/ --model claude-sonnet-4-6 --model qwen2.5-coder:7b`** compares LLM models against simulated failures. No engine required.

Full reference in [CLI Reference](docs/cli_reference.md).

## How it compares

<!-- TODO(positioning): tighten these one-liners with real benchmark/feature claims once verified. -->

| | Aqueduct | dbt | Dagster / Airflow | Raw PySpark |
|---|---|---|---|---|
| Pipeline definition | Declarative YAML Blueprints | SQL models | Python DAG code | Imperative code |
| Engine | Spark or DuckDB, capability-gated | Warehouse SQL | Orchestration only | Apache Spark |
| On failure | **Autonomous LLM patch + gates** | Manual fix | Retry / alert | Manual fix |
| Column lineage | Built-in, compile-time | Built-in | Plugin | DIY |
| Built-in observability store | Yes (DuckDB/Postgres) | Partial | External | DIY |

Aqueduct sits where a transformation engine and an autonomous repair loop meet. It is not a scheduler (pair it with Airflow via the `airflow` extra) and it is not a warehouse SQL tool.

## References

- **[Blueprint & Engine Spec](docs/specs.md)**: module types, configs, architecture, type system, healing loop
- **[SKILL.md](SKILL.md)**: distilled Blueprint-authoring guide for LLMs (grammar, patterns, provider base_urls)
- **[CLI Reference](docs/cli_reference.md)**: all commands and flags
- **[Spark Engine Guide](docs/spark_guide.md)**: warnings, performance, tuning
- **[Observability Guide](docs/observability_guide.md)**: schemas + diagnostic query cookbook
- **[Production Guide](docs/production_guide.md)**: cluster deployment, security, Delta operations
- **[Compatibility Matrix](docs/compatibility.md)**: supported Python × Spark versions, per-engine capability tables
- **[Extending Aqueduct](docs/extending.md)**: how to add an execution engine (`ExecutorProtocol`, capability declarations, entry points)
- **[Roadmap](docs/roadmap.md)**: deferred features and future plans
- **[Gallery](gallery/)**: real working examples

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md).

Aqueduct is Apache 2.0 licensed: free and open source, with no telemetry and no lock-in.
