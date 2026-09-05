# 5. Context Registry

# **5. Context Registry**

## **5.1 Three-Tier Resolution Model**

| Tier | Syntax | Resolved at | Performance cost |
| :- | :- | :- | :- |
| Tier 0: Static | `${ctx.namespace.key}` | Parse time | Zero: substituted before Manifest is written |
| Tier 1: Runtime function | `@aq.fn(args)` | Pre-job (Compiler) | Driver-only, milliseconds |
| Tier 2: UDF | `udf_id` called by name in Channel SQL (§5.4 `udf_registry`) | Engine execution | Distributed: operates on DataFrame columns |

## **5.2 Tier 0: Static Context**

```yaml
context:
  env:    ${AQUEDUCT_ENV:-dev}
  tables:
    orders: "s3://data/${ctx.env}/orders"
  params:
    dedup_key:  "order_id"
    batch_size: 10000
```

Resolution order (highest priority wins):

1. CLI flags: `aqueduct run --ctx env=prod`
2. Environment variables matching `AQUEDUCT_CTX_*` prefix
3. `context_profiles` block for the active profile (`--profile` flag)
4. `context:` block static defaults

### Env-var overrides (`AQUEDUCT_CTX_*`)

Any environment variable prefixed `AQUEDUCT_CTX_` overrides the context key
obtained by stripping the prefix and **lowercasing the rest**, so
`AQUEDUCT_CTX_ENV=prod aqueduct run blueprint.yml` is equivalent to
`--ctx env=prod`, and `AQUEDUCT_CTX_BATCH_SIZE=500` overrides a top-level
`batch_size` key. This is the override hook for CI pipelines, Airflow, and
schedulers that can set environment variables but cannot manipulate CLI
arguments.

Two rules to keep in mind:

- **Only top-level (dot-free) keys are addressable.** Nested context keys
  flatten to dot-notation (`params.batch_size`), and environment-variable
  names cannot contain dots: underscores are *not* translated to dots, so
  `AQUEDUCT_CTX_PARAMS_BATCH_SIZE` defines a new `params_batch_size` key
  rather than overriding `params.batch_size`. Keys you want overridable from
  the environment should live at the top level of `context:` (underscores in
  the key name itself are fine). Nested keys remain overridable via `--ctx
  params.batch_size=500`.
- **This is override, not substitution.** `${AQUEDUCT_ENV:-dev}` *inside* the
  `context:` block is env-var *substitution* (the value is read into a
  context key you named); `AQUEDUCT_CTX_*` is env-var *override* (the
  variable name selects which context key to replace). The two are
  independent mechanisms and compose: a substituted default is still
  replaceable by an `AQUEDUCT_CTX_*` override or `--ctx`.

## **5.3 Tier 1: Runtime Functions (`@aq.*`)**

| Function | Description |
| :- | :- |
| `@aq.date.today(format="%Y-%m-%d")` | Current date (UTC). Pinned by `--execution-date` for idempotent backfills. |
| `@aq.date.yesterday(format="%Y-%m-%d")` | Date - 1. |
| `@aq.date.offset(base, days)` | Offset a date string by N days. Useful for backfill windows: `@aq.date.offset(base=@aq.date.today(), days=-7)`. |
| `@aq.date.month_start(format="%Y-%m-%d")` | First day of the current month. |
| `@aq.date.format(date_str, pattern)` | Reformat an ISO date string into a custom pattern. |
| `@aq.run.id()` | Auto-generated UUID for this pipeline run. |
| `@aq.run.timestamp()` | ISO-8601 timestamp of compilation. |
| `@aq.run.prev_id()` | Run ID of the previous pipeline execution (reads `_last_run_id` from Depot). Fails compilation (2.68) if no depot backend is configured — see `@aq.depot.get` below. |
| `@aq.env('KEY')` | Read environment variable. Fails fast when absent, unlike `${VAR:-default}` which supports a fallback. |
| `@aq.secret('KEY')` | Read from AWS/GCP/Azure secrets manager or environment fallback. |
| `@aq.depot.get('key')` | Read from the default Depot KV store at compile time. `@aq.depot.<name>.get('key')` reads a named mount (see the Depot glossary entry + Observability Guide). **Fails compilation (`CompileError`, 2.68)** if no depot backend is configured at all — a Blueprint that references a depot read needs a real mount, or the read would silently fall back to the default and mask the pipeline going incremental-in-name-only. A configured depot with the key simply absent is unaffected: that still returns the default, unchanged. |
| `@aq.blueprint.id()` | This Blueprint's `id`. |
| `@aq.blueprint.name()` | This Blueprint's `name`. |
| `@aq.blueprint.dir()` | Absolute directory of the Blueprint file: the safe "relative-to-this-pipeline" anchor for output paths (e.g. `path: @aq.blueprint.dir()/out`). |
| `@aq.blueprint.path()` | Absolute path of the Blueprint file. |
| `@aq.deployment.env()` | `deployment.env` (e.g. `dev` / `cluster` / `cloud`), branch paths/behaviour by environment. |
| `@aq.deployment.target()` | `deployment.target` (e.g. `local` / `standalone`). |
| `@aq.deployment.engine()` | The execution engine this Manifest is compiled for (`deployment.engine`, e.g. `spark`): stamp it into an output path or a tag when the same Blueprint runs on more than one engine. |
| `@aq.version()` | The Aqueduct engine version: useful for stamping outputs. |

> `@aq.blueprint.* / @aq.deployment.*` exposes **pipeline identity + deployment context** known at compile time. Note what is **deliberately absent**: `cwd` / user / host, those differ across laptop ↔ CI ↔ Spark driver ↔ cluster, so they would make a Blueprint non-reproducible. Use `@aq.blueprint.dir()` as the stable anchor instead.

### 5.3.1 Resolution scopes: *where* each `@aq.*` resolves

The config and Blueprints resolve at **different times**, and a scope is usable
only where it exists:

| Resolution point | Allowed syntax | Why |
| :- | :- | :- |
| **`aqueduct.yml`** (engine config) | `${ENV}`, `${VAR:-default}`, `@aq.secret('KEY')` only | Loaded first, standalone: no Blueprint and no run exist yet, so per-pipeline / per-run scopes have nothing to resolve against. |
| **Blueprint compile** (per run: `context`, module `config`, blueprint-level `agent:` / `retry_policy:` / `engine:`, …) | **All `@aq.*`**: `date`, `run`, `blueprint`, `deployment`, `depot`, `secret`, `env`, `version` | The single point where the whole stack is in scope: deployment (from the loaded config) ⊃ blueprint (id/path) ⊃ run (run_id), plus the depot built from config. |

The model is **override-downstream, not propagate-uphill**: one config is shared
by many Blueprints, and one Blueprint by many runs; values flow config → blueprint
→ run, and each lower layer overrides what it inherits. There is no path back
*up*: a Blueprint cannot inject per-run values into `aqueduct.yml`, because the
config is fully resolved *before* any Blueprint is parsed.

Consequently, a non-secret `@aq.*` (e.g. `@aq.run.id()`, `@aq.blueprint.id()`) in
`aqueduct.yml` is a **hard error**: those scopes do not exist at config-load
time; use them inside the Blueprint. Per-pipeline store isolation (a depot / obs
store per Blueprint) needs no `@aq` in config, the backend handles it
automatically, keyed on `blueprint_id`.

## **5.4 UDF Registry**

UDFs are registered from importable code, not inline source. Two execution
models: `lang: python` (default) and `lang: java`/`scala` (JAR-backed).

**Python UDFs** point at an importable module + function:

```yaml
udf_registry:
  - id: clean_phone
    lang: python                   # default
    module: my_project.udfs        # importable module (must be on PYTHONPATH)
    entry: clean_phone             # function name in that module (defaults to `id`)
    return_type: STRING
```

The driver imports `module` and looks up `entry` (a plain Python callable),
registering it with `spark.udf.register`. `module` resolves against the Manifest's
`base_dir` first (a sibling `.py` file next to the Blueprint, see **§3, `base_dir`**),
falling back to a normal import from `PYTHONPATH` / an installed package. Python UDFs
execute row-at-a-time via the JVM bridge: for high-volume Channels prefer native Spark SQL.

**Parameterized (context-aware) Python UDFs.** Add a `params:` map and `entry`
becomes a **factory** (`entry(**params) -> callable`), so one importable
function is reused across blueprints and environments with different settings:

```yaml
udf_registry:
  - id: mask_pii
    module: my_project.udfs
    entry: make_masker             # factory: make_masker(char, keep_last, salt) -> callable
    return_type: STRING
    params:
      char: "*"
      keep_last: 4
      salt: "@aq.secret('PII_SALT')"   # resolved before the factory is called
```

Param values support `${ctx.*}`/`${ENV}` (Tier 0) and `@aq.*` including
`@aq.secret()` (Tier 1): they are fully resolved at compile time, so the
factory receives concrete values, never tokens. The factory must return a plain
callable (or a Spark UDF object). Omitting `params:` keeps the static behaviour
above (no factory call). UDF **bodies** remain out of scope for self-healing,
`params` change *configuration*, not code.

**Java/Scala UDFs** point at a JAR + class, pure JVM bytecode, no Python
serialization:

```yaml
udf_registry:
  - id: geohash
    lang: java                     # or scala
    jar: libs/geo-udfs.jar         # JAR path (relative paths anchor to the Blueprint dir)
    class: com.example.GeoHashUDF  # fully-qualified class name
    return_type: STRING
```

| Field | Applies to | Description |
| :- | :- | :- |
| `id` | all | UDF name: called by this name directly in Channel SQL. Required. |
| `lang` | all | `python` (default), `java`, or `scala`. |
| `return_type` | all | Hub type spelling (§9): Aqueduct's own portable vocabulary, not raw engine DDL (default `string`). |
| `module` | python | Importable module path. Required for python. |
| `entry` | python | Function name in `module` (defaults to `id`). With `params`, treated as a factory `entry(**params) -> callable`. |
| `params` | python | Optional keyword map passed to the `entry` factory. Values resolve `${ctx.*}`/`${ENV}` and `@aq.*` (incl. `@aq.secret()`) at compile time. |
| `deterministic` | python | Default `true`. `false` marks the UDF nondeterministic on both engines: Spark builds it via `asNondeterministic()` before registration, so the optimiser does not constant-fold, cache, or re-order calls; DuckDB inverts it onto `side_effects` (`side_effects = not deterministic`) for the same optimizer-safety reason. |
| `jar` | java/scala | JAR file path (relative paths anchor to the Blueprint dir). |
| `class` | java/scala | Fully-qualified class name. |

## **5.5 Dependencies (`dependencies:`, 2.66)**

A top-level Blueprint block, sibling of `udf_registry:`; not engine-scoped, no capability leaf, no `aqueduct.yml` allowlist surface. A flat list of PEP 508-lite requirement strings the Blueprint author declares the runtime environment must already satisfy:

```yaml
dependencies:
  - holidays>=0.40
  - geopy[extra]>=2.3,<3
```

`name`, `name>=1.2`, `name[extra1,extra2]>=1.2,<2` are accepted; environment markers (`; python_version < "3.12"`) are rejected rather than silently ignored, and a malformed string is a `ParseError` naming it at parse time.

This is a **compile-time preflight, not an installer**: Aqueduct never installs anything. Each declared requirement is checked against the installed environment via `importlib.metadata`; anything missing or version-conflicting raises `DependencyError` (a `CompileError` subclass), naming every failing requirement and the copy-pasteable `pip install` command, instead of the author meeting a mid-run `ImportError`. A package whose installed version string the PEP 440-lite comparator cannot read reports `unknown_version` and passes: the preflight exists to catch definitely-unsatisfied requirements, never to reject an install it merely does not understand. Satisfied requirements are silent.

See `aqueduct/dependencies.py` for the parser/comparator and §8.5 for `declare_dependency`, the healing-time counterpart that appends to this block.

> **FROZEN: declare-and-check only.** `dependencies:` and its healing-time counterpart
> `declare_dependency` are feature-complete and closed to extension. They declare what the
> environment must already provide and check whether it does. Neither will ever install,
> resolve, pin, or vendor a package, and no flag will be added to make them do so: installing
> into the environment a pipeline runs in is the operator's job, not the engine's.

---

