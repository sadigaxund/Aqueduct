# Extending Aqueduct: adding an execution engine

Aqueduct compiles a Blueprint into an engine-agnostic `Manifest`, then hands
that Manifest to an execution engine. `spark` is the reference engine, and
`duckdb` is the second engine, single-node and implementing a declared
subset of the grammar. This guide is written from the experience of building
`duckdb` through the real scaffold-and-fill workflow — it documents mechanics
that shipped, not a design proposal.

For the reference-level contract (verdicts, leaf sets, the compile gate, the
version check), see `docs/specs.md` §10.9. For the published per-engine
capability matrix, see `docs/compatibility.md`. This document is the
how-to; those are the what-is.

## Stability warning — read this first

**`ExecutorProtocol` is not a stable public API.** It has survived exactly
one second engine (DuckDB) and was reshaped more than once while that engine
was being built — `engine_options` was added to `SessionSpec` mid-build,
`DeferRules` was split out of `PromptRules` to stop one engine's advice
leaking into another's, and the required/optional split on
`ExecutorProtocol`'s fields was tuned against what DuckDB actually needed.
One data point is not enough to call a shape final.

Aqueduct does not yet market "bring your own engine" as a supported feature,
and this guide is not an invitation to build one for production against a
frozen contract — there isn't one yet. If you build a third-party engine
today, expect the protocol to change under you between minor releases, and
watch `CHANGELOG.md` for changes to `aqueduct/executor/protocol.py` and
`aqueduct/executor/capabilities.py`. The mechanics below are honest about
what exists, not a promise about what will keep working.

## What an engine is in Aqueduct

An engine is a Python module that, as an import side effect, registers two
things into two registries:

- an `EngineCapabilities` declaration (`aqueduct/executor/capabilities.py`) —
  a verdict for every leaf of the Blueprint grammar and every `aqueduct.yml`
  config key;
- an `ExecutorProtocol` (`aqueduct/executor/protocol.py`) — the callables
  core uses to run a Manifest on the engine and to fold the engine into the
  healing loop.

The module is wired in through the `aqueduct.engines` setuptools entry-point
group, declared in `pyproject.toml`. Nothing in `aqueduct/compiler/`,
`aqueduct/config.py`, or any other core module imports an engine's package by
name. `aqueduct/executor/capabilities.py::load_engines()` resolves and
imports every entry point in that group exactly once per process; each
engine module registers itself as an import side effect of being imported.
This is why adding DuckDB required zero edits to the compiler, the config
loader, or the CLI's engine dispatch — the seam already existed and DuckDB
plugged into it.

Two consequences follow directly from "registration is an import side
effect":

- Constructing an `ExecutorProtocol` object must never require the engine's
  actual runtime dependency to be installed. `execute`, `make_session`, and
  friends are thin wrappers that defer their real (`pyspark`- or
  `duckdb`-importing) implementation to call time. See
  `aqueduct/executor/spark/engine.py`'s module docstring — module-level
  `import pyspark` there would break every install without the `spark`
  extra, including the DuckDB-only install path.
- A broken or half-installed third-party engine surfaces as a clean,
  typed error (`EnginePluginError`) the moment its entry point is resolved,
  not as a bare `ImportError` deep inside `aqueduct.yml` loading. See
  "The two failure modes" below.

## Step by step

### 1. Scaffold the capability table

```
aqueduct dev capabilities scaffold --engine <name>
```

This writes a complete `capabilities.yml` under
`aqueduct/executor/<name>/` with **every** governed leaf present and set to
`undeclared` — currently 261 rows: ~160 Blueprint-grammar leaves (module
types, Channel ops, Egress write modes, Junction/Funnel fan modes, feature
flags) plus ~101 `aqueduct.yml` config leaves. The leaf set is derived live
from the grammar and config walkers (`capability_leaves.py`,
`config_leaves.py`), so the scaffold cannot go stale the way a checked-in
template would, and it cannot be gamed — there is no default-verdict sweep
anywhere in the framework.

**Do not copy another engine's `capabilities.yml`.** Spark's table has 261
`supported` rows because Spark actually implements 261 things. Cloning it
onto a new engine is a silent claim that the new engine supports the entire
grammar — precisely the blind spot this framework exists to catch. Read
Spark's table as a reference for what a verdict/`requires`/`hint` row looks
like; never paste it wholesale. DuckDB's own table
(`aqueduct/executor/duckdb_/capabilities.yml`) is a better model of an
*honest* subset declaration: parquet/csv/json ingress, a handful of Channel
ops, both Junction modes and both Funnel modes, parquet/csv egress —
everything else `unsupported`, with a `hint` on the rows that matter.

### 2. Work through every leaf

Every `undeclared` row must become one of:

| Verdict | Means |
|---|---|
| `supported` | The engine runs this leaf. Optionally carries `requires:` (a version floor, checked by `aqueduct doctor`, not at compile time) and `hint:`. |
| `unsupported` | The engine cannot run this leaf. A Blueprint grammar leaf becomes a compile-time `CompileError`; a config leaf only ever warns. |
| `ignored_with_warning` | The engine accepts the leaf but it has no effect. Suppressible warning (`engine_key_ignored`). |

`undeclared` itself is **not a fourth verdict** — it is a sentinel meaning
"nobody has decided yet," and it is deliberately distinct from
`unsupported` ("we decided this engine cannot do it"). A row stuck on
`undeclared` fails the build at engine registration
(`CapabilityDeclarationError`) and fails
`tests/test_capabilities/test_closure.py`. Earning `supported` means a real
handler plus a test exercising it on that engine — not an assumption.

### 3. `sync` and `check`

```
aqueduct dev capabilities check    # read-only, what CI runs
aqueduct dev capabilities sync     # appends any newly-derived leaves as `undeclared`
```

`sync` never invents a verdict and never deletes a row — a human decides
what a new leaf means for an existing engine, and an orphaned row (one that
no longer matches a real leaf, e.g. after a rename) is reported for review
rather than silently dropped. Run `sync` whenever the grammar or config
schema grows a new leaf on `main` while your engine is mid-build; it keeps
your table current without doing the deciding for you.

### 4. Generate the docs matrix

```
aqueduct dev capabilities docs
```

Regenerates the engine matrix in `docs/compatibility.md` between its
`<!-- ENGINE_MATRIX_START -->` / `<!-- ENGINE_MATRIX_END -->` markers,
straight from the YAML declarations. The published matrix and the enforced
one are the same data by construction — there is no separate hand-written
compatibility table to keep in sync.

## Implement `ExecutorProtocol`

`aqueduct/executor/protocol.py` defines the contract. Every engine
constructs exactly one `ExecutorProtocol` instance and calls
`register_protocol()` on it, mirroring the capability registration pattern.
`__post_init__` enforces the required members structurally — a malformed
engine cannot register, it raises `EnginePluginError` at construction time
rather than degrading silently.

### Required members

**`execute: (manifest, session, ...) -> ExecutionResult`**

The uniform contract is "compiled `Manifest` and an engine session handle
in, a frozen `ExecutionResult` out." Beyond that, `execute`'s kwargs are
intentionally `Callable[..., ExecutionResult]` rather than a fixed
signature: the shared CLI run path (`aqueduct/cli/run.py`) calls every
engine's `execute` with the same superset of run options
(`run_id`, `store_dir`, `checkpoint_root`, `surveyor`, `depot`,
`resume_run_id`, `from_module`, `to_module`, `block_full_actions`,
`warnings_*`, plus Spark-only options like `parallel`, `use_observe`,
`sampling`, `observability_store`, `explain_capture`). An engine with no
equivalent for a Spark-only option drops it in its own protocol adapter —
see `_DUCKDB_EXECUTE_KWARGS` in `aqueduct/executor/duckdb_/engine.py`, a
frozenset the adapter filters kwargs against before calling the engine's
real `execute()`, so that function's actual signature never has to accept
an option it cannot honour.

**`extract_error: engine exception (or None) -> FailureContext field dict (or None)`**

Maps an engine's own exception vocabulary onto `FailureContext`'s
structured fields (`error_class`, `root_exception`, `sql_state`,
`suggested_columns`, `object_name`). This is required, not optional,
because without it `FailureContext.error_class`/`root_exception` stay
silently `None` for every failure on that engine, and the healing LLM loses
the structured root-cause block it relies on for diagnosis — the exact
"heal quality dies on a new engine's errors" failure mode this member
exists to prevent. Spark's implementation
(`aqueduct/executor/spark/engine.py::_extract_error`) is a straight
delegation to the pre-existing
`aqueduct.surveyor.error_extraction._extract_structured_error` — naming
existing behavior through the new seam, not new code. DuckDB's
(`aqueduct/executor/duckdb_/error_extraction.py::extract_duckdb_error`) is
written fresh against DuckDB's own exception types.

**`prompt_rules: PromptRules`**

The engine's half of the composed healing system prompt. The healing
prompt is **composed**, not monolithic: an engine-independent scaffold
(PatchSpec schema, op-selection table, provenance rules, output contract,
generic defer categories — `aqueduct/agent/prompts.py`) is combined at
build time with the target engine's `PromptRules` pack, pulled through the
registry. `PromptRules` has four fields:

- `persona` — the prompt's opening line ("You are an expert `<engine>`
  blueprint repair agent...").
- `root_cause_note` — the prose description of what the engine's structured
  root-cause block contains; it is the human-readable counterpart of what
  `extract_error` actually returns, so the two need to stay honest about
  each other.
- `rules` — the engine's own error idioms and advice (DuckDB's pack, for
  example, warns the model that `repartition`/`coalesce`/`cache` have no
  DuckDB equivalent and are `unsupported`, and that a Binder Error naming a
  missing column is often a wrong Ingress `format` rather than a bad
  Channel query).
- `defer` — a `DeferRules`, the engine's slice of the "when to defer to a
  human" section: `infra_examples` (the engine's actual infrastructure
  failure modes — Spark names Hive metastore locks and cluster config;
  DuckDB, single-process and embedded, names a concurrent writer holding
  the database file lock instead), `udf_languages` (the languages the
  engine's UDF registry accepts), and an optional `extra_bullets` for a
  whole defer category unique to the engine.

**Why this cannot be generic strings.** A `DeferRules`/`PromptRules` field
left blank does not fail closed — it falls back to reading like advice for
a different engine. A DuckDB heal told to defer over "Hive metastore locks"
is actively wrong: DuckDB has no metastore. This is why `DeferRules` is a
separate frozen dataclass with its own non-empty validation rather than a
free-text field on `PromptRules`, and why the guard test
(`tests/test_agent/test_prompt_composition.py`) composes the full prompt
for a synthetic non-Spark engine and asserts no Spark vocabulary
(`Spark`, `pyspark`, `AnalysisException`, `Py4J`, `Hive`, `Scala`,
`metastore`) survives anywhere in it — including in the parts of the prompt
assembled at *runtime* (the defer section only renders when `allow_defer`
is on), not just the static template constant. When you write a new
engine's `PromptRules`, write it fresh against that engine's real error
vocabulary; do not adapt another engine's copy.

### Session lifecycle: `make_session` / `close_session` / `SessionSpec`

Optional at registration — a compile-only engine or a test double has no
session to build — but required for `aqueduct run` to actually run on the
engine. `session_factory()`/`session_closer()` raise a clean
`EnginePluginError` naming the engine (never `NotImplementedError`) if a
runnable engine reaches the CLI without one.

`SessionSpec` is the engine-agnostic construction request:

```
blueprint_id: str
engine_config: dict[str, Any] = {}
master_url: str = ""
quiet: bool = False
quiet_startup: bool = False
engine_options: dict[str, Any] = {}
```

The fields are the *union* of what registered engines need, not Spark's
constructor signature verbatim — an engine reads the fields it understands
and ignores the rest. DuckDB's `make_session` (`_make_session` in
`aqueduct/executor/duckdb_/engine.py`) ignores `master_url`/`quiet*`
entirely and always opens a fresh `:memory:` connection; `engine_config`
and `engine_options` are accepted for cross-engine parity but currently
unused (there is no `duckdb_config` knob yet). `engine_options` exists
specifically so a future engine's session needs — anything the four named
fields don't cover — can be threaded through without a breaking change to
this frozen dataclass; nothing in core populates it today.

### Optional diagnostic readers: `read_source_schema` / `sample_source_rows`

`(module, session) -> {column: type}` and
`(module, session, n, base_dir) -> [{col: val}, ...]` back two of the
healing agent's toolbox diagnostics (`get_source_schema` and `sample_rows`
in `aqueduct/agent/toolbox.py`). Both must be metadata-only /
bounded-by-a-pushed-down-LIMIT — never a full scan or a materializing read.

If an engine registers without them (`None`, the default), the toolbox does
not crash and does not fall back to another engine's reader — it degrades
to the same structured "unavailable" response it already returns when there
is no live session. This closed a real coupling: before the protocol
existed, the toolbox imported Spark's schema reader unconditionally, so a
heal running against a non-Spark engine still read source schemas through
Spark. DuckDB implements both (`aqueduct/executor/duckdb_/schema_reader.py`)
so this degradation does not apply to it in practice, but a new engine that
skips them ships a working (if less diagnostic) healing loop rather than a
broken one.

### Optional type mapping: `render_type`

`(HubType | NativeType) -> str` — renders one parsed hub type
(`aqueduct/typehub.py`) to this engine's own native type-system spelling.
Backs Channel `op: cast`, Ingress `schema_hint`, and (Spark only, so far) UDF
`return_type` — the three places a Blueprint type string reaches an engine at
runtime.

Optional at registration, the same class as `read_source_schema` /
`sample_source_rows` above, not the required class: shipping a complete type
mapper for the full hub vocabulary is not a reasonable bar to clear before a
new engine's first release. The degrade contract when it is missing
(`render_type=None`, the default) is narrow and must stay honest rather than
silent: your engine still runs a Blueprint that uses only ITS OWN
native-namespace escape hatch (`"<your-engine>:<spelling>"`, unwrapped to
`.spelling` verbatim — no mapping needed) and any spelling its own runtime
parser accepts raw, but a hub spelling (`bigint`, `array<int>`, `decimal(10,2)`,
…) used against your engine is refused with a clean `EnginePluginError`, not
silently forwarded to a parser that was never going to understand it. That
refusal is `aqueduct.executor.protocol.render_native_type(engine, spelling)`'s
job — route your engine's cast/schema_hint/return_type consumption through
it (or through your own `render_type` directly once you've written one)
rather than reimplementing the parse-and-dispatch logic:

```python
from aqueduct.executor.protocol import render_native_type
from aqueduct.typehub import TypeSpellingError

def normalize_type_spelling(spelling: str) -> str:
    try:
        return render_native_type("your-engine", spelling)
    except TypeSpellingError:
        # Not a hub spelling at all — hand it to your own parser raw,
        # same fallback both shipped engines use for a spelling the hub
        # does not recognize (their own native DDL written directly).
        return spelling
```

`render_native_type` also defensively rejects a `NativeType` naming a
DIFFERENT engine (`"other-engine:<spelling>"` reaching your engine) — the
compile-time `type.native.*` capability gate should already have refused
that Blueprint for your engine, so this only fires on an ungated call path,
and it must fail loudly rather than forward a foreign engine's native
spelling to your parser.

Look at `aqueduct/executor/spark/type_render.py` and
`aqueduct/executor/duckdb_/type_render.py` for two real, complete mappers.
Both are pure string logic — no `pyspark`/`duckdb` import — which is the
preferred shape: `render_type` does not need the lazy-import discipline
`execute` does, since it never has to touch the engine's actual runtime.
When you do need composite-type spellings (`array<T>`, `map<K,V>`,
`struct<name:type,...>`), verify them empirically against your engine's real
parser rather than guessing from documentation — DuckDB's mapper was written
this way (`CAST(x AS INTEGER[])`, `CAST(x AS MAP(VARCHAR, INTEGER))`,
`CAST(x AS STRUCT(a INTEGER))`, including nested combinations, all checked
against a real `duckdb.connect()` before being declared `supported`).

## Entry-point declaration

`pyproject.toml`:

```toml
[project.entry-points."aqueduct.engines"]
spark = "aqueduct.executor.spark.engine"
duckdb = "aqueduct.executor.duckdb_.engine"
```

The key is the engine name (matches `deployment.engine` in `aqueduct.yml`
and the `engine` field on `ExecutorProtocol`/`EngineCapabilities`); the
value is the dotted path to the module whose import registers both. A
third-party engine ships this same table in its own package's
`pyproject.toml` — `aqueduct` never needs to know about it in advance;
`load_engines()` discovers it via `importlib.metadata.entry_points()` at
process start.

## The two failure modes — never conflate them

Two error types cover two genuinely different situations on this seam, and
callers distinguish them by exception **type**, never by matching message
text:

- **`EnginePluginError`** — the `aqueduct.engines` entry point failed to
  **import**. The plugin is broken or half-installed (a syntax error, a
  missing transitive dependency, a stale wheel). The fix is reinstall.
  This is also what `ExecutorProtocol.__post_init__`/`PromptRules`/
  `DeferRules` raise for a structurally incomplete registration attempt —
  the same error type, because a plugin author hitting one of those guards
  is, from the framework's point of view, shipping a broken plugin.
- **`CapabilityDeclarationError`** — the `capabilities.yml` declaration is
  **incomplete or invalid**: a leaf with no row, a row still on
  `undeclared`, a row naming a leaf that no longer exists, an illegal
  verdict string, a malformed `requires` specifier. This is a dev-time
  build failure. The fix is `aqueduct dev capabilities sync` followed by a
  real verdict — reinstalling fixes nothing, because the package installed
  fine; the *data it ships* is the problem.

Conflating these two once already made the closure guarantee meaningless in
an earlier iteration of this framework: a message telling a developer who
had just added a schema key to "reinstall the package" fixes nothing and
sends them down the wrong path. If you write code that catches errors from
this seam, branch on `isinstance(exc, EnginePluginError)` /
`isinstance(exc, CapabilityDeclarationError)`, never on substring matching.

## Layer rules

- **Engine runtime deps stay inside the engine's own subpackage.**
  `pyspark` is imported nowhere outside `aqueduct/executor/spark/`; `duckdb`
  is a base dependency (the observability/depot stores already need it —
  see `aqueduct/stores/duckdb_.py`) but the engine's own code still lives
  entirely under `aqueduct/executor/duckdb_/`. A third-party engine's own
  heavy dependency (a database driver, a distributed-compute client)
  follows the same precedent: confined to the engine's own subpackage,
  imported lazily inside function bodies wherever the *module itself* must
  stay importable without that dependency (constructing the
  `ExecutorProtocol`/`EngineCapabilities` objects must never require it).
- **Prompt-pack purity.** An engine's `PromptRules` pack names no other
  engine, ever — see the anti-bleed discussion above. This is enforced by a
  test, not just a convention (`tests/test_agent/test_prompt_composition.py`).
- **Core never imports an engine by name.** Not the compiler, not
  `aqueduct/config.py`, not the CLI. Everything reaches an engine through
  `get_capabilities(engine)` / `get_protocol(engine)`, both backed by
  `load_engines()`. If you find yourself writing
  `from aqueduct.executor.duckdb_ import ...` anywhere outside
  `aqueduct/executor/duckdb_/` itself or its own tests, that is very likely
  a layering violation — the one documented exception in the whole codebase
  for a similar cross-layer reach is `aqueduct/parser/parser.py` importing
  the engine-agnostic `path_keys.py` registry, which is deliberately not
  engine-specific code.
- **The agent layer imports no engine specifics.** Spark's `engine.py`
  module docstring states this explicitly: "this module imports NOTHING
  from `aqueduct/agent/`... Core never imports a `spark.*` module by name."
  The dependency only ever flows one way — an engine's module imports from
  `aqueduct.executor.protocol`, never the reverse.

## Testing expectations

The capability declaration is the single source of truth for what an
engine is *allowed* to be tested doing — there is no separate,
hand-maintained per-engine skip list anywhere in the test suite. Two
mechanisms keep this honest:

- `tests/test_capabilities/test_closure.py` is the anti-drift proof: it
  reads every registered engine's `capabilities.yml` straight off disk
  (not through the loaded registry — the two must stay independent sources
  that *can* disagree, or the test cannot fail) and compares it against the
  live-derived leaf set. A leaf with no row, a row still `undeclared`, or a
  row naming a leaf that no longer exists fails the build. This is what
  guarantees a new engine's declaration is complete before you write a
  single functional test against it.
- The compile-time gate (`aqueduct/compiler/capability_check.py`) means an
  `unsupported` leaf simply cannot appear in a Manifest compiled for that
  engine — a test blueprint using an op DuckDB does not support fails to
  *compile* for `duckdb`, loudly, rather than needing a runtime skip
  decorator to route around it.

Given that, engine-specific *mechanics* tests stay engine-owned: DuckDB's
own test directory (`tests/test_executor_duckdb/`, marked
`pytest.mark.duckdb`) exercises DuckDB-specific behavior (`COPY TO`,
`sqlglot` transpilation, DuckDB's mutable-catalog relation lifecycle) that
has no Spark analog and does not belong in a shared parametrized suite.
Behavior that is genuinely engine-invariant (hook firing, retry policy —
see `tests/test_cli/test_cli_duckdb_engine_invariant.py`) gets its own
proof, run concretely against DuckDB, rather than an assumption that
"engine-agnostic orchestration" needs no engine-specific verification at
all. When you add a new engine: write its own marked test directory for its
own mechanics, run `aqueduct dev capabilities check` before you claim a
leaf `supported`, and let the closure test and the compile gate do the
work of keeping the declaration and the grammar from drifting apart — do
not hand-write a skip list that duplicates what the capability table
already says.

**A `supported` EXECUTION verdict is not just a claim — it is a claim with a
test id attached.** `capability_leaves.py::execution_leaves()` is the
in-scope subset (`module.type.*`, `channel.op.*`, `ingress.format.*`,
`egress.format.*`/`.mode.*`/`.on_new_columns.*`, `junction.mode.*`,
`funnel.mode.*`, `feature.*` — never `config.*`, never `module.field.*` or a
`<block>.field.*` leaf, which are warn-only or engine-invariant with nothing
per-engine to exercise). For every leaf in that set your engine declares
`supported`, add a `tests:` key naming the pytest id(s) (or a bare file path
for a whole-file link) that actually run the handler on your engine — a
compile-only or mocked test is a weak link, not a real one; prefer a test
that drives the module/op end to end. `tests/test_capabilities/
test_verdict_test_links.py` is the build-breaking gate: it fails, naming the
engine and leaf, if a `supported` execution row has no `tests:` id, or if a
declared id does not resolve against the real test tree (missing file,
or a `::name` node id pytest would never collect). `aqueduct dev
capabilities check` reports the same gaps without failing its own exit
code, so you can see them before wiring CI. Never invent an id to make the
gate go green, and never downgrade a verdict just to dodge it — an honestly
unbacked `supported` leaf should fail this test until you either write the
missing test or admit the leaf is not really proven yet.

## What this guide is not

This is not a parity claim between engines. DuckDB is single-node,
implements a declared subset of the grammar, and some capabilities the two
engines do share still behave differently in ways a verdict cannot express
— see `docs/specs.md` §10.9's "Engine notes" for the current list (DuckDB's
`append` write is not atomic; several Channel ops materialize eagerly
rather than staying lazy). Read the matrix in `docs/compatibility.md`
before assuming a Blueprint that runs on one engine runs unchanged on
another.
