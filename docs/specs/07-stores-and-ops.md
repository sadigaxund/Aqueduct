# 9-10. Type System, Deployment and Engine Integration

# **9. Type system**

## **9.1 Aqueduct invents no types: it adopts Arrow's**

Wherever a Blueprint names a column type (Ingress `schema_hint`, Channel `op: cast`, UDF `return_type`), it writes a spelling from Aqueduct's own type vocabulary (`aqueduct/typehub.py`, "the hub"), not a raw engine-native string. The hub is not invented: it borrows Apache Arrow's type semantics for the constructors it defines, because Arrow already solved the one distinction that matters most here (an instant vs. a naive wall-clock value, below). It is deliberately a **subset** of Arrow's full taxonomy, not a mirror (no unions, no dictionary or run-end encoding, no fixed-size lists), just the constructors both a distributed engine (Spark) and a single-node columnar engine (DuckDB) can implement. There is no `pyarrow` dependency anywhere in this: the hub borrows Arrow's semantics, not Arrow's code.

Every hub type is a value type describing comparison and storage semantics, never an engine spelling:

| Constructor | Canonical spelling | Semantics (Arrow-borrowed) |
| :- | :- | :- |
| Boolean | `boolean` | True/false. Arrow `bool`. |
| Tiny int | `tinyint` | 8-bit signed, -128..127. Arrow `int8`. |
| Small int | `smallint` | 16-bit signed. Arrow `int16`. |
| Int | `int` | 32-bit signed. Arrow `int32`. |
| Big int | `bigint` | 64-bit signed. Arrow `int64`. |
| Float | `float` | 32-bit IEEE-754. Arrow `float32`. |
| Double | `double` | 64-bit IEEE-754. Arrow `float64`. |
| String | `string` | Variable-length UTF-8, unbounded. Arrow `string`/utf8. |
| Binary | `binary` | Variable-length raw bytes, no encoding implied. Arrow `binary`. |
| Date | `date` | Calendar date, no time-of-day, no zone. Arrow `date32`. |
| Decimal | `decimal(p,s)` | Fixed-point, `p` total digits, `s` after the point: exact arithmetic, no binary-float rounding. Arrow `decimal128`. |
| **Timestamp (tz)** | `timestamp_tz` | An **INSTANT**: a UTC point independent of any wall clock. Arrow `timestamp[us, tz=UTC]`. |
| **Timestamp (ntz)** | `timestamp_ntz` | A **NAIVE** wall-clock value with no zone attached. Arrow `timestamp[us]` (no tz). |
| Duration | `duration(unit)` | A span of time, stored as a plain signed 64-bit integer count of `unit` ticks (`s`/`ms`/`us`/`ns`: Arrow's own `TimeUnit` granularities). Modeled on Arrow `duration[unit]` VALUE semantics, deliberately rendered as a plain integer on every engine rather than either engine's native `INTERVAL` type: see "Why `duration` is integer-backed" below. |
| Array | `array<T>` | Ordered, variable-length list, one element type. Arrow `list<T>`. |
| Map | `map<K,V>` | Unordered association, one key/value type pair. Arrow `map<K,V>`. |
| Struct | `struct<name:type,...>` | Ordered, fixed set of named, independently-typed fields. Arrow `struct<...>`. |

`timestamp_tz` / `timestamp_ntz` is the load-bearing pair the whole hub exists to make explicit. Two `timestamp_tz` values from different source zones compare and sort correctly against each other, because both are already normalized to the same instant line. Two `timestamp_ntz` values compare as plain numbers: there is no instant they correspond to without an externally supplied zone. This is exactly the distinction Arrow's own type system already draws, which is why the hub borrows it rather than inventing a third scheme.

A small set of familiar unambiguous aliases canonicalize silently at parse time (`long` → `bigint`, `integer` → `int`, `varchar`/`char` → `string`, `short` → `smallint`, `byte` → `tinyint`, `bool` → `boolean`). `decimal` with no precision/scale defaults to `decimal(10,0)`, matching Spark's own DDL default: a well-defined default, not an ambiguity.

**Why `duration` is integer-backed.** Every other composite/parametrized constructor above (`decimal(p,s)`, `array<T>`, ...) renders to a real native type on both engines. `duration(unit)` deliberately does not follow that pattern: Spark's day-time `INTERVAL` and DuckDB's `INTERVAL` do not share a Parquet representation either engine's writer/reader can round-trip against the other, so a hub constructor built on top of either engine's native interval type would inherit exactly the cross-engine fragility the hub exists to prevent. `render_type` renders `duration(unit)` as a plain `BIGINT`/`bigint` on both engines instead: a signed 64-bit integer count of `unit` ticks, with no logical-type ambiguity a Parquet reader/writer could disagree about. `unit` is metadata the hub carries (which tick size the integer counts); neither engine's cast machinery ever consults it. An author who wants one engine's own native interval semantics (calendar arithmetic, month/day/microsecond components) uses the `<engine>:` native namespace directly instead of `duration(unit)`.

Four surfaces carry a type spelling: Ingress `schema_hint`, Channel `op: cast`'s type map, UDF `return_type`, and Flow Report / lineage's reported column types (rendered in hub spellings). `parser/schema.py` still types these fields as plain strings (the grammar itself is unchanged) but `aqueduct/compiler/compiler.py` now parses every one of them through `typehub.parse_type()` at compile time, so an unrecognized spelling is a compile-time `TypeSpellingError` naming the nearest valid spellings, not a runtime parser crash three layers down.

## **9.2 Two kinds of ambiguity, two different rules**

The hub draws a hard line between two kinds of "this spelling could mean more than one thing", because the correct response is opposite for each:

- **Semantic ambiguity → reject at parse time, naming the alternatives.** A spelling is semantically ambiguous when the *value it describes* differs across engines: bare `timestamp` is the only one the hub currently defines this way. Spark's `timestamp` is an instant; DuckDB's `TIMESTAMP` is naive. The same Blueprint, unmodified, would silently mean a different value depending on which engine ran it. That is not a spelling problem the hub can quietly resolve: it is a genuine ambiguity in what the author meant, so the hub refuses to guess in silence and asks the author to say `timestamp_tz` or `timestamp_ntz` (or a native spelling naming one engine on purpose).
- **Representational ambiguity → canonicalize silently.** A spelling is representational when several familiar strings name the *same* value type: `long` and `bigint` are both a 64-bit signed integer everywhere; there is no engine on which they diverge. These fold to one canonical spelling with no warning, because there was never a real choice being hidden.

Misapplying this rule in either direction breaks the hub's contract: silently resolving `timestamp` (a semantic ambiguity) reintroduces exactly the bug the hub exists to prevent, and refusing `decimal` with no precision/scale as if it were semantically ambiguous (a representational default, not a real one) would make ordinary Spark-style DDL fail for no reason.

**Bare `timestamp` is REJECTED at compile time.** There is no deprecation window: bare `timestamp` never parses. It raises a `TypeSpellingError` (surfaced as a compile-time `CompileError` at whichever surface used it: `schema_hint`, `cast`, or `return_type`) naming both explicit spellings and the single-engine native escape hatch. Write `timestamp_tz` or `timestamp_ntz` explicitly, or a native spelling (`spark:timestamp`) if the Blueprint intentionally targets one engine only. There is no suppress mechanism for this: it is not a warning.

**The native namespace: an explicit, capability-gated escape hatch.** `<engine>:<spelling>` (e.g. `duckdb:HUGEINT`, `spark:interval day to second`) names a type in one engine's own vocabulary directly, bypassing the hub entirely. It is not validated for meaning, only for shape (non-empty engine token, non-empty spelling): whatever that engine's own runtime parser accepts, it accepts. This is governed by the capability framework (`type.native.<engine>`, §10.9), not exempt from it: writing `duckdb:HUGEINT` into a Blueprint compiled for `spark` is a compile-time `CompileError` naming the spelling, because `type.native.duckdb` is `unsupported` on the Spark engine. Docs and templates **recommend the portable hub spellings** for anything that has one; the native hatch exists for spellings the hub genuinely has no equivalent for (DuckDB's `HUGEINT`, Spark's `interval`/`variant`), and using it is an explicit, honest statement that this Blueprint is written for one engine.

## **9.3 The hub is a superset: it surfaces divergence, it does not hide it**

Every constructor in the table above is now a capability leaf (`type.<constructor>`, one per registered engine, plus `type.native.<engine>` for the escape hatch: see §10.9), checked recursively against every inventoried type surface at compile time. `channel.op.cast` being `supported` says an engine implements a cast operation; `type.array` being `supported` on that same engine says it can additionally cast to a composite spelling. Both engines shipped today declare all seventeen constructors `supported`, each backed by a real runtime mapping (`ExecutorProtocol.render_type`, §10.9) from the hub's canonical spelling to that engine's own native DDL: `array<int>` renders to DuckDB's `INTEGER[]`, `timestamp_tz` renders to Spark's plain `timestamp`, `duration(unit)` renders to a plain `BIGINT`/`bigint` on both. A hub spelling used against an engine with no verdict for that constructor, or no `render_type` mapper at all, is refused at the seam it would otherwise reach a parser through: never silently forwarded to a parser that was never going to understand it.

Read this plainly rather than as a portability guarantee: the hub does not make two engines equivalent, and it is not trying to. What it changes is the FAILURE MODE. Before the hub, an engine mismatch on a type: Spark's `timestamp` vs. DuckDB's `TIMESTAMP`, a composite spelling DuckDB's alias table didn't know: reached that engine's own parser raw and failed there, if it failed at all, as an engine stack trace with no Aqueduct context, or (worse, `timestamp`) didn't fail and just silently meant something different. The hub is a **superset** of what any one engine natively spells, chosen so that divergence between engines becomes a **visible refusal** (a `CompileError` or a named compiler warning, at the surface where the Blueprint is read) rather than a value that quietly means something else three layers downstream. That is a strictly honest trade, not a simplification: a Blueprint that only uses portable hub spellings on constructors every target engine declares `supported` runs the same way everywhere; one that reaches for a native escape hatch or a still-ambiguous bare spelling is now told so at compile time instead of finding out at 2am from a wrong row count.

---

# **10. Deployment & engine integration**

## **10.1 Engine configuration file**

Aqueduct reads a project-level `aqueduct.yml` configuration file from the working directory (or path specified by `--config` flag). This file sets deployment target, store backends, agent config, and engine defaults.

The canonical field reference with descriptions and defaults lives in the `aqueduct.yml.template` file shipped with the engine. The config blocks that `aqueduct.yml` can contain:

| Block | Owns |
| :- | :- |
| `deployment` | Engine selection (`engine: spark` or `engine: duckdb`), cluster target |
| `engine` | Per-engine settings, namespaced by engine name (2.0: see below): `engine.spark.master_url`, `engine.spark.conf`, `engine.duckdb` |
| `stores` | Backend selection for observability, depot, blob, and benchmark (DuckDB / Postgres / Redis / local / s3 / gcs / adls) |
| `probes` | Default probe signal limits |
| `danger` | Safety-gate overrides |
| `secrets` | Secrets provider (env / aws / gcp / azure / custom) |
| `webhooks` | Outbound webhook endpoints for run lifecycle events |
| `agent` | LLM connection defaults (provider, base_url, model, api_key, cascade, timeout, budget), CI webhook URL |
| `warnings` | Compiler/executor warning suppression rules |
| `checkpoint_root` | Local filesystem path overriding the derived `<store_dir>/checkpoints/` location for module checkpoint/resume state (2.8) |
| `handoff` | Cross-engine handoff spill location + failure-retention policy (`root`, `keep_on_failure`) for the compiler-synthesized Handoff module (2.35): see §10.9 |
| `timezone` | Universal session time zone applied to every registered engine's session at creation (2.38): see §10.3.1 |

### The `engine:` block (2.0)

Per-engine configuration is namespaced by engine name, mirrored between `aqueduct.yml` (engine-level defaults: the full per-engine field set below) and the Blueprint (`engine:` block, per-Blueprint overrides; see §4.2). The two levels are NOT always field-identical: each engine's Blueprint-level block carries only the fields where a per-pipeline override is meaningful, deliberately excluding deployment/connection concerns that describe how THIS installation runs rather than what this pipeline needs (Spark's Blueprint block has always excluded `master_url` for exactly this reason; DuckDB's Blueprint block (2.54) similarly excludes `database_path`, `extension_repository`, and the `s3_*` credential/endpoint fields; see the `engine.duckdb:` block below). A key under `engine.<name>.` belongs to that engine; every other engine accepts and ignores it (a suppressible `engine_key_ignored` warning, never an error; see §10.9 "Config-leaf governance"). Adding a new engine's settings is a new sub-block here, never a new top-level `<engine>_config` dict.

```yaml
engine:
  spark:
    master_url: "local[*]"           # SparkSession.builder.master() — validated against deployment.target
    conf:                            # per-run Spark session configuration
      spark.sql.shuffle.partitions: 200
  duckdb:
    memory_limit: "4GB"              # SET memory_limit — unset keeps DuckDB's own default
    threads: 4                       # SET threads — unset keeps DuckDB's own default
    database_path: "/data/run.duckdb"  # persistent file, replacing the default :memory: connection
    extension_repository: null       # SET custom_extension_repository — airgapped-mirror escape hatch
    s3_key_id_secret: null           # secret KEY NAME (resolved via secrets:), fed into CREATE SECRET
    s3_secret_access_key_secret: null  # secret KEY NAME — must be set together with s3_key_id_secret
    s3_region: null                  # not sensitive — given literally
```

This is the full `aqueduct.yml`-level field set. A Blueprint's own `engine.duckdb:` block (§4.2) accepts only `memory_limit`/`threads`; see the `engine.duckdb:` block section below for why the rest stay `aqueduct.yml`-only.

**2.0 BREAKING: moved off two pre-2.0 locations.** `deployment.master_url` (it is Spark's own cluster-connection string, not a cross-engine deployment concern; `deployment.target` stays where it is) and the top-level `spark_config` dict (named after one engine, with nowhere for a second engine's knobs to live) both move under `engine.spark:`. `aqueduct_config` bumps `"1.0"` → `"2.0"`. Both are now hard-rejected (`extra="forbid"`) at their old location: a pre-2.0 file fails at config-load naming the rejected key directly (`ConfigError`, exit code `CONFIG_ERROR`), never silently accepted or auto-migrated:

```text
# Before (1.0)                          # After (2.0)
aqueduct_config: "1.0"                  aqueduct_config: "2.0"
deployment:                             deployment:
  engine: spark                           engine: spark
  target: local                           target: local
  master_url: "local[*]"                engine:
spark_config:                             spark:
  spark.sql.shuffle.partitions: 200         master_url: "local[*]"
                                             conf:
                                               spark.sql.shuffle.partitions: 200
```

The Blueprint-level `spark_config:` block moves the same way, into a Blueprint-level `engine:` block with the identical inner shape (§4.2):

```text
# Before (1.0)                     # After
spark_config:                      engine:
  spark.sql.shuffle.partitions: 200  spark:
                                        conf:
                                          spark.sql.shuffle.partitions: 200
```

`engine.spark.conf` at both levels merge the same way `spark_config` always did (Blueprint wins on conflict). The `set_spark_config` PatchSpec op (§8) initially carried over unchanged by name, with only its write target moved to `engine.spark.conf.<key>`: it was later replaced outright by the engine-agnostic `set_engine_config` (§8), which addresses any registered engine's block, not just Spark's.

**This merge is engine-generic, not a Spark special case (2.53), and has THREE layers (2.64).** `aqueduct.executor.session_config.resolve_session_engine_config` layers a Blueprint's `engine.<name>:` block over that engine's `aqueduct.yml`-level `engine.<name>:` config, and this invocation's `-s/--set` overrides over both; lowest to highest: `aqueduct.yml` < Blueprint < `--set`. That holds for EVERY registered engine: the same rule Spark has always documented above, implemented once and shared. Through 2.52 the internal Manifest carrier for this was still named `spark_config` and read only on Spark's session-build path, so a Blueprint-level `engine.duckdb:` override had nowhere to go: DuckDB always got its `aqueduct.yml` config only, with no way for a Blueprint to override it. The internal carrier (never a YAML-facing name: this Blueprint/Manifest field is plumbing, not part of the grammar documented here) is now `engine_config: dict[str, dict]`, keyed by engine name, populated for every engine named in the `engine:` block, empty for one with nothing set. DuckDB's Blueprint-level block (2.54) carries `memory_limit`/`threads`; a future field added there participates in the same Blueprint-wins merge automatically.

**Why `--set` is the top layer (2.64).** `-s/--set` is documented as the highest-precedence source (`--set > blueprint > aqueduct.yml > defaults`, see the CLI reference), but engine config is not resolved by that plain overlay: it has its own merge, and `--set` used to be applied only to the `aqueduct.yml` layer of it. A value a self-heal had written into the Blueprint's `engine.<name>:` block therefore beat the flag a user typed at the prompt, inverting "explicit beats default" for the one source that is the most explicit statement a user can make about a run. `--set` is now a genuine third layer above the Blueprint rather than a mutation of the layer beneath it, stated once in `resolve_session_engine_config` and never re-implemented per call site. It is safe for a CLI flag to outrank a heal precisely because it is per-invocation and never written back to any file: it overrides a healed value for one run, it cannot undo one. Two visible consequences. First, `session_config_fingerprint` separates a session built with `--set` from one built without it, for free: the flag is inside the function whose output the fingerprint hashes, so nothing had to be added there; within a run, a heal writing a Blueprint value the flag shadows produces the SAME fingerprint and correctly triggers no session rebuild. Second, Gate 1's inert-write refusal (§8) becomes reachable from a source the Blueprint cannot outrank: a `set_engine_config` patch writing a key the invocation pins is refused with a message naming the exact `--set` path and its pinned value, rather than the ordinary "write a different value" advice, which would be false there.

### The `engine.duckdb:` block: session config + remote storage (2.41, Blueprint-level fields 2.54)

Every field is read by `_make_session`: none is a silent no-op (see AGENTS.md's "no silent no-ops" rule).

- **`memory_limit`/`threads`**: `SET memory_limit=...`/`SET threads=...`, applied right after connecting. Unset keeps DuckDB's own defaults. **Available at BOTH levels** (2.54): `aqueduct.yml`'s `engine.duckdb.*` sets the deployment default, a Blueprint's own `engine.duckdb:` block (§4.2) may override it per-pipeline (Blueprint wins); a pipeline plausibly needs more memory or more threads than the machine default, and that is a property of the pipeline, not the deployment.
- **`database_path`**: a persistent file, replacing the default `:memory:` connection. LOCAL PATHS ONLY (a remote URI scheme is rejected at config-load, mirroring `checkpoint_root`): DuckDB's own database file is always local even when the tables it reads/writes point at remote storage. Two independent reasons to set it: it raises a receiving cross-engine handoff island's RAM ceiling (a bare `:memory:` connection hard-caps it at available RAM), and it lets large intermediates spill to disk instead of aborting. `aqueduct.yml`-only: not on the Blueprint's `engine.duckdb:` block (a Blueprint doesn't pick which local file this installation's DuckDB process writes to, any more than it picks a Spark `master_url`).
- **`extension_repository`**: `SET custom_extension_repository=...`, applied before any extension install. The airgapped/hermetic-CI escape hatch: `httpfs` (below) autoinstalls over the network on first use by default, which fails on a cluster with no route to DuckDB's public extension repository. The other escape hatch, a pre-populated `~/.duckdb/extensions` directory, needs no config at all: DuckDB checks its local cache first. `aqueduct.yml`-only, same reasoning as `database_path`.
- **`s3_key_id_secret`/`s3_secret_access_key_secret`/`s3_region`**: S3/GCS credentials for remote ingress/egress/handoff paths. The first two are secret KEY NAMES (never a literal credential), resolved through the EXISTING `secrets:` block resolver (`aqueduct.secrets.resolve_secret`; the same function `@aq.secret()` calls) at session creation, and fed into DuckDB's own `CREATE SECRET (TYPE S3, KEY_ID ?, SECRET ?, REGION ?)` via parameter binding; never string-interpolated into SQL, so a credential value can never end up in a logged or rendered statement. `s3_region` is not sensitive and is given literally. The two secret-name fields must be set together (config-load validation error otherwise). `aqueduct.yml`-only: a Blueprint overriding a credential secret name or connection endpoint is a footgun, not a feature; same reasoning `master_url` has always had on the Spark side.

**Why no new capability leaf.** The Blueprint-level `memory_limit`/`threads` override reaches the exact same `_make_session` code path, through the exact same `engine_config` dict key, as the `aqueduct.yml`-level field: `aqueduct.executor.session_config.resolve_session_engine_config` merges them before either one is read. The capability question ("can this engine's session accept a memory_limit override at all") is already asked and answered once, by the existing `config.engine.duckdb.memory_limit`/`config.engine.duckdb.threads` `aqueduct.yml`-level leaves (§10.9); the Blueprint-level field is a second value SOURCE for the identical capability, not a second capability. This mirrors the existing precedent of Spark's Blueprint-level `engine.spark.conf` block, which has likewise never had its own grammar leaf distinct from `config.engine.spark.conf`.

**`httpfs` is a DuckDB EXTENSION, not a Python package**: nothing enters `pyproject.toml`, no new dependency or extra. On duckdb>=1.0, `autoinstall_known_extensions`/`autoload_known_extensions` both default to `True`, so any module touching an `s3://`/`gs://` path already makes DuckDB install and load `httpfs` on its own with zero Aqueduct code. `_make_session` proactively `INSTALL`s/`LOAD`s `httpfs` only when S3 credentials or `extension_repository` are configured (a deliberate signal of remote-storage intent) so an airgapped-install failure surfaces LOUDLY at session creation as `aqueduct.executor.duckdb_.extensions.DuckDBExtensionError` (an `AqueductError` naming both escape hatches), not as a bare `duckdb.IOException`/HTTP error buried inside a later query. When neither is configured, a DuckDB session-startup warning (`duckdb_httpfs_availability`, mirroring Spark's `jar_availability` rule: same diagnostic shape, not the same mechanism: a jar ships to Spark's executor fleet at session creation, a DuckDB extension installs per-connection in-process) fires if the compiled Manifest reads/writes a remote path and `httpfs` is not yet loaded, naming the network requirement and both escapes.

`aqueduct doctor`'s `handoff-access:duckdb` check (§10.4.3) attempts a real round trip against a remote `handoff.root` the same way it always has for a local one; it no longer unconditionally reports `skip` for a remote root.

## **10.2 Environment variables & .env**

- Aqueduct automatically loads `.env` from the directory of the config or blueprint file.
- Override with `-e KEY=VAL` (highest precedence) or `--env-file <path>`.
- Disable entirely with `AQ_NO_ENV_FILE=1`.

**Config overrides (`-s/--set`, 1.2).** `aqueduct run -s agent.approval=human -s stores.observability.backend=postgres …` sets dotted-path keys in the loaded `aqueduct.yml` config in memory for that invocation, repeatable, applied after the file is read and before validation. Distinct from `--ctx` (which sets Blueprint Context Registry values, not engine config). Values are parsed as YAML scalars (`true`/`123`/strings).

## **10.3 SparkSession lifecycle**

- The Executor creates one SparkSession per pipeline run.
- Session configuration from the Blueprint `engine.spark.conf` block is merged with `aqueduct.yml`'s `engine.spark.conf` (Blueprint takes precedence).
- **`spark.sql.parquet.outputTimestampType` defaults to `TIMESTAMP_MICROS` (2.36+),** set by the session factory at creation time, in place of Spark's own default (`INT96`, a legacy Hive-interop encoding whose Parquet files carry no logical-type annotation distinguishing an instant-aware timestamp from a naive one). This changes the on-disk encoding of any `timestamp` column an Egress module writes with Spark: values are unchanged, `INT96` is deprecated in the Parquet spec, and `TIMESTAMP_MICROS` is annotated correctly regardless of which engine reads the file back. An explicit `engine.spark.conf.spark.sql.parquet.outputTimestampType` value always overrides this default.
- On self-healing patch and resume: the SparkSession is preserved if the failure was application-level; recycled if JVM/network-level.
- On run completion or abort, the one-shot CLI relies on process-exit teardown to release the JVM, it deliberately does **not** call `session.stop()`, because `getOrCreate()` may have returned a shared/long-lived cluster (or test) session that other code still depends on. Short-lived helper commands that create a throwaway session (`doctor`, scaffolding) do stop theirs.

### **10.3.1 Universal session timezone (`timezone:`, 2.38)**

A top-level `aqueduct.yml` key, resolved through the engine registry (never a hardcoded engine list) and applied to EVERY registered engine's session at creation: Spark's `spark.sql.session.timeZone`, DuckDB's `SET TimeZone`, and whatever the equivalent is for any engine registered later.

```yaml
timezone: "UTC"   # IANA/Olson name, e.g. "UTC", "America/New_York"
```

Engine-native session-timezone settings already work standalone (`engine.spark.conf: {spark.sql.session.timeZone: UTC}`); a shared key only earns its keep once a Blueprint spans more than one engine (§10.9's cross-engine handoff). There, a divergent per-engine session time zone is a WRONG-ANSWER bug, not a config annoyance: `to_timestamp` on a naive string resolves to a different instant per engine, and a `timestamp_tz → date` cast lands differently. Two engines reading one key makes that divergence unrepresentable; two independent engine-native keys make it silent.

**Precedence.** An explicit engine-native override always wins for that engine: the same "explicit beats default" rule applied everywhere else in this project (`engine.spark.conf.spark.sql.parquet.outputTimestampType` over the session factory's own default, `engine.spark.conf` over `aqueduct.yml`'s copy of it, ...). `timezone:` is applied only when the target engine's own resolved config doesn't already set its native equivalent; when it does AND the two values disagree, a suppressible warning fires (rule id `engine_timezone_conflict`) naming the divergence; the whole point of the universal key is making cross-engine timezone divergence VISIBLE, so silently letting one engine drift defeats it. DuckDB has no `engine.duckdb.*` conf knob yet (§10.1), so this precedence fork is exercised on Spark today; a future DuckDB session-timezone knob would participate in the same rule.

## **10.4 Path resolution (1.1.0+)**

Every relative path inside a YAML file resolves to **that YAML file's parent directory**, never the CWD of the `aqueduct` command. See [CLI Reference](../cli_reference.md) for details.

### **10.4.1 Observability store routing (DuckDB)**

`stores.observability.path` (DuckDB backend) is always a **routing base
directory** (2.0: the earlier single-shared-file layout was removed; a
`.db`-suffixed path is now a config-load error):

| `path` value | Layout | Parallelism |
| :- | :- | :- |
| *(unset: default)* | **Per-blueprint routing**: each blueprint writes its own file at `.aqueduct/<blueprint_id>/observability.db` | ✅ Safe to run different blueprints in parallel, separate files |
| A directory, e.g. `/mnt/aqueduct/obs` | **Location-only routing**: same per-blueprint split, but under your directory: `<dir>/<blueprint_id>/observability.db` | ✅ Safe: separate files, custom location |
| ~~A file, e.g. `/mnt/aqueduct/obs.db`~~ | **Removed in 2.0**: DuckDB is single-writer, so one shared file was never parallel-safe, and a custom basename split reads from writes. Config load fails with a pointer here. | Use **Postgres** for one shared concurrent store |

**Caveats:**
- DuckDB takes an **exclusive lock** per file. Launching the *same* blueprint twice concurrently (one routed file) will block/fail, rare, but real.
- Want **one merged store for every blueprint** (shared file semantics)? Use the **Postgres** backend (MVCC, concurrent writers). Cross-blueprint *reads* over routed DuckDB files already work, the fleet commands (`report`, `runs`) aggregate across `<base>/*/observability.db`.
- **Reading while running:** `aqueduct report`/`runs` open short-lived read-only connections, so they don't block writers; a file mid-write is momentarily skipped by the fleet view. You do **not** need to stop pipelines to inspect, but for conflict-free continuous monitoring, use Postgres.
- *Planned:* dynamic templating in the path (e.g. `.aqueduct/obs-@aq.date.month().db` for time-partitioned stores).

### **10.4.2 Checkpoint root override (2.8)**

`checkpoint: true` (module- or manifest-level, see §4) writes module output to
Parquet for `--resume` support. By default this lands under the derived
`<store_dir>/checkpoints/<run_id>/` directory: the same routing base used by
the observability store (§10.4.1).

`checkpoint_root` (top-level `aqueduct.yml` key) overrides that derived
location entirely: when set, checkpoints for **both** a fresh run and a
`--resume` reload live directly under `<checkpoint_root>/<run_id>/`, bypassing
`store_dir` for this purpose only (observability signals still use
`store_dir`). Use it to point checkpoints at faster local disk, or a directory
explicitly shared between driver and workers on a Docker-based Spark
Standalone cluster.

**LOCAL FILESYSTEM PATHS ONLY.** A `checkpoint_root` value containing a remote
URI scheme (`s3://`, `s3a://`, `gs://`, `hdfs://`, `abfss://`, ...) is rejected
at config-load with an actionable error: remote checkpoint roots require
Hadoop-FS-API bookkeeping that Aqueduct does not yet implement. A relative path is
resolved against the project root (the `aqueduct.yml` directory).

**`--resume` fails closed at the CLI, then stays permissive at the engine
(2.68).** Every checkpointed run writes a `_manifest_hash` file alongside its
checkpoints. Before `aqueduct run --resume <run_id>` builds any engine
session, the CLI itself reads that stored hash back and compares it against
this run's freshly-compiled Manifest hash; on a mismatch it refuses
outright (`CONFIG_ERROR`), naming both hashes and pointing at `--force`.
Pass `--force` to reuse the checkpoints anyway — with `--force` (or on a
matching hash), execution proceeds exactly as before 2.68: both engines'
`execute()` independently re-compare that same stored hash against the
current Manifest's hash and, on a mismatch, emit a suppressible
`runtime_resume_hash_changed` warning through `aqueduct.warnings.emit()`
(suppressible via the engine-level `aqueduct.yml` `warnings.suppress` /
`--suppress-warning`, §4.2, the same mechanism session-startup warnings
use) and then PROCEED anyway, reusing whatever checkpoints exist. That
engine-level comparison is unchanged and, on its own (i.e. calling
`execute()` directly rather than through `aqueduct run`), is still purely
permissive — it is the CLI's fail-closed check, not the engine, that makes
`aqueduct run --resume` refuse by default. This is the direct counterpart
to the handoff spill's fail-closed detection below: see §10.4.3's callout.

### **10.4.3 Cross-engine handoff spill (2.35)**

`handoff:` (top-level `aqueduct.yml` block) configures WHERE the compiler-synthesized Handoff module's storage-spill parquet lands, and whether it survives a failed run. See §10.9 for what a Handoff module is and when the compiler inserts one; this section is the config surface only.

```yaml
handoff:
  root: ".aqueduct/handoff"   # default; any URI both engines can read+write (s3://…)
  keep_on_failure: true       # default — the resume story
  prune_eagerly: true         # default, see the same-run pruning paragraph below
```

Unlike `checkpoint_root`, `root` is **not** local-filesystem-only: a handoff spill must be reachable by BOTH engines on either side of a boundary, so a remote URI scheme (`s3://`, `gs://`, `abfss://`, ...) is accepted with no rejection. `handoff:` borrows `checkpoint`'s LIFECYCLE semantics (kept on failure, cleaned up on success); not its location or its local-only constraint, and not its config key (`handoff:` is its own top-level block, never nested under `checkpoint_root`). The two diverge past that shared lifecycle shape, though: a module checkpoint resumes across a CHANGED Manifest too, and (as of 2.68) `aqueduct run` itself refuses that by default before proceeding permissively under `--force` (§10.4.2, above). A handoff spill has no engine-level hash to compare in the first place: the Manifest hash is part of the spill's own directory (see layout below), so at the orchestrator layer a changed Manifest just resolves to a different directory and the prior spill is never looked at — no comparison, no warning, nothing to suppress. `aqueduct run --resume <run_id>` (2.68) closes that gap one layer up, the same CLI check described in §10.4.2: before building any engine session it scans `handoff.root` for *run_id* under every OTHER manifest-hash directory. Finding it there means the run_id exists but under a stale hash — refused (`CONFIG_ERROR`, both hashes named), unless `--force`. Finding nothing anywhere is not a mismatch (a run_id nobody has used, or one whose only checkpoints are the module kind above) and is left exactly as permissive as before: that island simply executes fresh. On the DuckDB side, a remote root is reached the same way any other remote path is (§10.9's `engine.duckdb:` subsection); `httpfs`, autoloaded on first touch, plus `engine.duckdb.s3_*` credentials (including the `s3_endpoint`/`s3_url_style`/`s3_use_ssl` non-AWS escape hatch) if the target requires authentication or is not AWS S3 itself. For an S3-flavored root touched by BOTH engines, use `s3a://`; Spark's bundled Hadoop FS registers `s3a://` (via the `hadoop-aws` package, resolved through `engine.spark.conf.spark.jars.packages`), not the legacy `s3://` scheme; DuckDB's `httpfs` accepts either scheme identically. See `docs/production_guide.md`'s "Object storage: MinIO / other non-AWS S3-compatible stores" for verified settings.

Directory layout: `<root>/<manifest_hash>/<run_id>/<edge_id>/`, one subdirectory per boundary per run. Deleted when the run succeeds; kept when it fails and `keep_on_failure` is true (the default), so a manual `aqueduct run --resume <run_id>` after a plain failure (with no Blueprint edit in between) can read the upstream island's already-materialized spill instead of recomputing it. A heal-triggered rerun does NOT get this: a heal patches the Manifest, which changes the whole-Manifest hash (and therefore `<manifest_hash>` in the path above) even when the patch touched only a downstream island, and the CLI's heal-retry path passes no `resume_run_id` at all once a patch has been applied; the two mechanisms never line up. Parquet is a fixed internal transport detail: there is no format knob, on either the Blueprint or the config side.

**Same-run eager pruning (`prune_eagerly`, default true).** Within one polyglot run, a boundary's spill does not have to wait for the whole run to end: it is deleted as soon as every island that reads it has finished successfully, since a handoff edge has exactly one reader island and nothing later in the same run will ever touch that directory again. This bounds peak spill storage on a long same-run chain instead of holding every boundary's output until the final island finishes. It only ever removes a spill whose reader already succeeded in THIS run. A spill feeding an island that has not run yet, or one this run resumed from a PRIOR run via `--resume`, is left alone, so a run that later fails at island N still has every spill feeding island N and everything after it intact on disk, and `--resume` behaves exactly as it did before this existed. `keep_on_failure` and the end-of-run deletion described above are unaffected either way; an eagerly pruned boundary is simply already gone by the time the end-of-run cleanup runs over it. Set `prune_eagerly` to false to defer every deletion to the run's own end instead.

**Keeping a spill is bounded by a release event, not by a clock.** `keep_on_failure: true` acquires disk; two deterministic actions give it back. First, a successful `--resume` deletes the spill it consumed: that spill was kept for exactly the rerun that has now read it, so its purpose is served (a FAILED resume keeps it, since it is still resumable). Second, the orphan sweep reclaims any kept-failure spill once a LATER run of the same blueprint has succeeded, which means the failure is resolved and nothing will ever resume from it again. "Succeeded" includes the `patched` run status, not only `success`: a heal that fixes the pipeline records `patched`, and that is the most common way a failure gets resolved. `finished_at` is used only to order two `run_records` rows against each other; there is no retention window, no age threshold, and no configurable number of days anywhere in this. Two consequences are stated rather than hidden. The first is closed: a blueprint that fails and is never run again used to keep its spill indefinitely with no way to reclaim it; `aqueduct handoff sweep --older-than <duration>` (e.g. `--older-than 7d`) is that explicit operator/watchdog action, additionally reclaiming a kept-failure spill whose run finished longer ago than the given age even though no later success has superseded it yet, never automatically and never without the flag. The second is decided and accepted: a failure under active investigation loses its spill if an unrelated scheduled run of the same blueprint succeeds in the meantime. Aqueduct builds no protection against that, neither an exemption for the most recent failure nor an opt-out setting. The hazard is unmeasured, and everything an operator actually debugs from survives the sweep: the `run_records` row, the `failure_contexts` row, and the stack trace are store records, not spill directories. A handoff spill is an intermediate parquet materialisation of one island's output, so losing it costs a rerun rather than a diagnosis, and guarding it would mean carrying a second retention rule here or a config key on every engine to protect a cost nobody has measured.

### **10.4.4 Depot mount routing (DuckDB)**

A depot mount under `stores.depots` (DuckDB backend) resolves in one of two
ways, decided by whether `path` is set.

| `path` value | Layout | Key isolation |
| :- | :- | :- |
| *(unset: default)* | **Per-blueprint routing**: the mount gets its own file at `.aqueduct/<blueprint_id>/depot.db`, next to that blueprint's `observability.db` and never inside it | None needed: keys are raw, because the FILE is already per blueprint |
| A file, e.g. `/mnt/aqueduct/depot.db` | One shared file for every blueprint that names it | Keys are prefixed with `<blueprint_id>:`, unless `shared: true` asks for raw keys |

`--store-dir` replaces the routing base for a per-blueprint mount, the same
way it does for the observability store (§10.4.1).

`shared: true` requires an explicit `path`. A mount with no `path` lives in a
file no other blueprint reads, so asking to share it is a contradiction:
config load fails naming the mount. The `postgres` and `redis` backends also
require an explicit `path`, because there the value is a DSN or URL, not a
file this routing can derive.

### **10.4.5 Watermark crash-consistency**

A watermark-driven incremental pipeline is normally two Egress modules:
one that appends the rows (`mode: append`), and a second, downstream one
that writes the watermark gating the next run's read range
(`format: depot`). If the process dies after the append commits and before
the watermark write commits, the next run resolves the same read range
again and appends it a second time. Nothing else in Aqueduct notices this
on its own, because the append and the watermark write are two independent
module writes with no shared transaction.

**The intent row.** An append Egress may declare `watermark_key: <depot
key>`, naming the key the downstream `format: depot` Egress writes. Before
that append's write starts, the executor writes an intent row to the depot
at key `__intent__:<watermark_key>`, with a JSON value carrying `run_id`,
`module_id`, and `started_at` (ISO-8601 UTC). The downstream `format: depot`
Egress that writes `<watermark_key>` clears that intent row in the SAME
transaction as its watermark upsert (`kv_put_and_clear`, atomic on the
DuckDB and Postgres depot backends; best-effort, non-atomic on Redis, since
Redis exposes no transaction spanning two independent commands here).

A Blueprint is rejected at parse time (`ParseError`) if an Egress declares
`watermark_key` without `mode: append`, if it is set on `format: depot`
itself, if no other Egress in the Blueprint writes that key via
`format: depot`, or if such an Egress exists but is not topologically after
the append Egress in the blueprint's edge graph (i.e. not reachable from it
— wrong order or simply unconnected).

**The run-start refusal.** At the start of every run, before the
incremental read range is resolved, Aqueduct checks every `watermark_key`
in the Blueprint for a leftover intent row. Finding one means a prior run's
append may have committed without its watermark ever landing, so the run
refuses to start with a loud error naming the depot key, the run_id that
left the intent row, and when it started. Resolving it has two paths:

1. De-duplicate the target range for that run, then run `aqueduct depot
   clear-intent <key> --blueprint <blueprint.yml>`.
2. Or, having confirmed the append never actually landed, just run
   `aqueduct depot clear-intent <key> --blueprint <blueprint.yml>`.

`aqueduct depot clear-intent` clears the `__intent__:<key>` row on the
default depot mount and reports whether a row was actually there (see
`docs/cli_reference.md`). It needs `--blueprint` because a depot is per
blueprint either way: a pathless mount routes to its own file, and a mount
with an explicit `path` prefixes its keys with the blueprint id. Pass the
same `--store-dir` the run used, if it used one.

## **10.5 Deployment targets**

The `deployment.target` field selects the Spark cluster type. Aqueduct validates
that `engine.spark.master_url` matches the declared `target` at config-load
(for `engine: spark` only), and `aqueduct doctor` provides target-specific
reachability and configuration guidance.

| Target | Status | Required `engine.spark.master_url` shape | Doctor checks |
| :- | :- | :- | :- |
| **local** | Supported (in-cluster) | Starts with `"local"` (e.g. `local[*]`) | In-process session: always ok |
| **standalone** | Supported (in-cluster) | Starts with `"spark://"` (e.g. `spark://host:7077`) | TCP probe to master host:port |
| **yarn** | Supported (in-cluster) | Exactly `"yarn"` | Warns if `HADOOP_CONF_DIR` / `YARN_CONF_DIR` env var is unset |
| **kubernetes** | Supported (in-cluster) | Starts with `"k8s://"` (e.g. `k8s://https://apiserver:443`) | TCP probe to API server host:port; warns if no `spark.kubernetes.*` keys in `engine.spark.conf` |
| **emr** | Deferred | n/a | Rejected at config-load with a "not yet supported" error |
| **dataproc** | Deferred | n/a | Rejected at config-load with a "not yet supported" error |

`emr` / `dataproc` are **remote-submit** targets planned for a future
release; in the current release they are rejected with a "not yet
supported" error at config-load. There is no built-in remote-submit target
today. To run on Databricks, wrap `aqueduct run` in a Databricks Workflows
`spark_python_task` (see the Production Guide).

See the **[Production Guide](../production_guide.md)** for per-target cluster setup,
required env vars, `engine.spark.conf` keys, and the production readiness checklist.

## **10.6 `aqueduct test`: Isolated module testing**

`aqueduct test <test_file.yml>` runs Channel, Junction, Funnel, and Assert modules against inline data with no external I/O. Ingress and Egress are never executed. The session always runs on `local[*]`, `engine.spark.master_url` is deliberately ignored for cluster-pointed configs.

## **10.7 Orchestrator integration contract**

Aqueduct stays orchestrator-agnostic. Schedulers (Airflow, Dagster, Prefect) wrap `aqueduct run` and consume two stable surfaces: the **exit-code contract** and the **patch CLI JSON**. Both are part of the v1.0 stability guarantee.

| Exit code | Name | Meaning |
| :- | :- | :- |
| 0 | SUCCESS | Command completed successfully |
| 1 | CONFIG_ERROR | Configuration or schema error |
| 2 | DATA_OR_RUNTIME | Runtime / Spark / data error (includes remote job failure) |
| 3 | HEAL_PENDING | Patch staged for human review |
| 4 | VALIDATION_GATE | Patch rejected by validation |
| 64 | USAGE_ERROR | Invalid command usage |

Note: `USAGE_ERROR` is 64 (sysexits `EX_USAGE`), covering both an Aqueduct-detected usage mistake raised explicitly by a command (e.g. an unsupported `--store` value) and Click's own `UsageError` (unknown command, unknown flag, missing required argument, a bad `click.Choice` value) — `aqueduct/cli/__init__.py` repoints `click.exceptions.UsageError.exit_code` to this constant at import time, so both sources exit the same code. `5` was `USAGE_ERROR`'s value before this unification; it is retired and never reused.

## **10.8 Remote-submit targets**

`emr` and `dataproc` are **rejected at config‑load** in the current release.
Setting `deployment.target` to either of these values raises a `ConfigError`.
There is no built-in remote-submit target today; see §10.5 for the
Databricks migration path.

## **10.9 Engines and the capability framework**

> **A new engine is not required to support cross-engine handoff.** The handoff/island
> machinery (§4.3, §11.4) is experimental. What an engine must implement to be complete is the
> `ExecutorProtocol` plus its own capability declaration; taking part in a polyglot Blueprint is
> optional and unsupported territory.

The Blueprint grammar (module types, Channel ops, Egress write modes, feature flags) is engine-agnostic by design. `deployment.engine` selects which engine runs a compiled Manifest. No engine is required to implement the whole grammar, and a leaf an engine does implement may still need a minimum dependency version. The capability framework makes both facts explicit and enforced, so a Blueprint that asks an engine for something it cannot do fails at compile time with a specific message instead of at runtime with an engine stack trace.

### Engines that ship today

| Engine | What it is | Install | Entry point |
| :- | :- | :- | :- |
| `spark` | The reference engine. Distributed, cluster or local. Implements the full grammar. | `aqueduct-core[spark]` | `aqueduct.executor.spark.engine` |
| `duckdb` | Single-node, in-process. Implements a declared subset. | `aqueduct-core[duckdb]` | `aqueduct.executor.duckdb_.engine` |

The two are not interchangeable, and Aqueduct does not present them as such. A Blueprint that compiles for both engines is one whose leaves both engines have declared `supported`. That is a property the compiler checks per Blueprint, not a property of the product.

The DuckDB engine currently reads `parquet`, `csv`, and `json`; runs Channel `sql`, `join`, `filter`, `select`, `deduplicate`, `cast`, `rename`, `sort`, and `union`; runs every Junction mode and every Funnel mode; runs every Assert rule type (including `null_rate`, `custom`, and quarantine via the spillway port) and every `on_fail` action; runs Probe (8 of Spark's 9 built-in signals plus `custom`; see §4.4 for the two places its behavior genuinely diverges from Spark's); runs Python UDFs (`conn.create_function`); and writes `parquet` and `csv`, including the `on_new_columns` schema-drift write contract. Channel `sql`/`join`, Assert `sql`/`sql_row`, and Probe `threshold`/`custom` SQL are authored in Spark SQL and transpiled to DuckDB SQL with `sqlglot`. `execution_partitions` and Java UDFs are declared `unsupported` rather than silently accepted. Any of those paths may point at remote storage (`s3://`, `gs://`, ...); DuckDB's `httpfs` extension autoloads on first touch, and `engine.duckdb.*` config (below) wires memory/thread limits, a persistent database file, and S3 credentials reconciled with the `secrets:` resolver. The per-leaf verdicts are published as a generated matrix (see below) rather than restated here.

### How an engine registers

An engine registers itself through the `aqueduct.engines` setuptools entry-point group (`pyproject.toml`'s `[project.entry-points."aqueduct.engines"]` table maps an engine name to a module, e.g. `spark = "aqueduct.executor.spark.engine"`). Importing that module registers the engine's capability declaration as a side effect. `aqueduct/executor/capabilities.py::load_engines()` resolves and imports every entry point in the group exactly once per process, and `get_capabilities()` calls it before looking an engine up. Core never imports an engine's package by name: a new engine (e.g. DuckDB) ships its own entry point and needs no edit to `aqueduct/compiler/`, `aqueduct/config.py`, or any other core module to become a valid `deployment.engine` value. `deployment.engine` is validated against the set of registered engines at config-load time, not a fixed list of literals.

### Failing closed

`get_capabilities()` raises `UnknownEngineError` (an `AqueductError`, subclassing `CompileError`) for an engine with no registered capability declaration: an unknown name, a typo, or an engine whose package/extra is not installed. The message names the engine and lists what is registered. The compile-time gate and the doctor capability check both let this propagate rather than degrading to an empty result: a misconfigured or unregistered engine is a loud, actionable failure, never a silently-skipped gate. Callers that must tell an unregistered engine apart from an ordinary compile failure do it by exception type, not by matching on the message.

Two adjacent failure modes get their own diagnosis:

- **No engines registered at all.** An empty registry means aqueduct's own entry points are invisible to `importlib.metadata`, in practice a stale install whose metadata predates the entry-point declaration. Since engine validation is fail-closed, that state would otherwise hard-fail every `aqueduct.yml` load with a misleading "Registered engines: []". The error says what it actually is: reinstall the package.
- **A broken engine plugin.** An `aqueduct.engines` entry point that fails to import raises `EnginePluginError` (an `AqueductError`) naming the entry point, its target, and the underlying cause. A half-installed third-party engine surfaces as a clean Aqueduct error, not as a raw `ImportError` out of config loading. The plugin is broken or half-present, so the message ends in reinstall advice.
- **An incomplete or invalid declaration.** A `capabilities.yml` with a leaf that has no row, a row still on `undeclared`, a row naming a leaf that does not exist, an illegal verdict, or a malformed version specifier raises `CapabilityDeclarationError` (an `AqueductError`). This is a dev-time build failure, typically a developer who has just added a schema key that every engine now owes a verdict for, so reinstalling the package fixes nothing. The message names the offending leaves (also carried on the exception as `.leaves`) and gives the fix that works: run `aqueduct dev capabilities sync`, then declare a verdict per engine.
- **An undecided config-leaf scope.** A `config.*` field living under an `engine.<name>.*` block with no `engine_scoped: True` tag raises `CapabilityScopeError` (an `AqueductError`, a SIBLING of `CapabilityDeclarationError`; deliberately not a subclass, so a shared `except CapabilityDeclarationError:` cannot swallow it). See "Config-leaf scoping" below. Raised by the walker at every engine's registration time, never CI-only.

These three states are distinguished by exception type, never by matching message text.

### Verdicts

A verdict answers one question: if a Blueprint uses this leaf on this engine, what happens? Every engine declares one of four for every leaf.

| Verdict | Meaning | Effect |
| :- | :- | :- |
| `supported` | The engine runs this leaf. May carry a `requires` version constraint, for example `format: custom` needs `pyspark>=4.0`. | Compiles. |
| `unsupported` | The engine cannot run this leaf. | Blueprint leaf: `CompileError`. Config leaf: warning. |
| `ignored_with_warning` | The engine accepts the leaf and it has no effect. | Suppressible warning under `engine_key_ignored`. |
| `undeclared` | Nobody has decided yet. | Build failure at engine registration. |

`undeclared` is a sentinel rather than a verdict, and it is deliberately distinct from `unsupported`. "We have not decided" and "we decided the engine cannot do it" are different states, and a framework that conflates them cannot tell an honest refusal from an oversight. It is what the sync tool writes for a newly discovered leaf, and the build stays red until a human replaces it.

An engine earns `supported` rather than assuming it. For a leaf the engine executes (a Channel op, a format, a write mode, a module type, a feature flag) that means a real handler plus a test exercising it on that engine. For a leaf the engine never touches, where core orchestration behaves identically whichever engine is selected (the `agent:` block, webhooks, hooks, retry policy), it means an end-to-end test proving that on this engine, rather than an assumption that it must be so.

### Declarations are data, one explicit row per leaf

Each engine ships a YAML capability declaration alongside its package (`aqueduct/executor/spark/capabilities.yml`, `aqueduct/executor/duckdb_/capabilities.yml`) carrying one row for every capability leaf: a verdict, plus the optional `requires` constraint and `hint` text. Spark's file holds 208 rows (191 Blueprint-grammar leaves + 17 engine-scoped config leaves); DuckDB's holds 213 (191 + 22; 15 shared/DuckDB-only engine-scoped leaves plus the 7 `engine.duckdb.*` leaves from the 2.41 config-surface work, minus the 2 `engine.spark.*` leaves that are positionally Spark's to declare, not DuckDB's; see "Config-leaf scoping" below). Every engine's table also has 88 config leaves it is never asked about at all: see that section for why the two counts differ from a single flat total.

There is no default-verdict sweep. An engine states which leaves it supports, one row at a time, and never "everything, by assumption". A third-party engine author ships reviewable data rather than Python.

`load_declaration()` hard-validates the file at registration and raises `CapabilityDeclarationError` on: a row for a leaf that does not exist (a typo or a stale rename), an illegal verdict string, a malformed version specifier, a leaf with **no row at all**, or a row still parked on `undeclared`. An engine cannot register half-declared.

### Config-leaf scoping

Blueprint-grammar leaves (`module.type.*`, `channel.op.*`, formats, modes, `feature.*`) are engine-invariant by construction: every registered engine declares every one, because whether an engine can run a module type or a Channel op is genuinely a question every engine has to answer. `config.*` leaves (the `aqueduct.yml` surface) are not all that shape. Of the 105 `config.*` leaves `AqueductConfig` derives, ~88 run entirely in core code paths (`webhooks.*`, `secrets.*`, `stores.*`, most of `agent.*`, most of `danger.*`, …) that never dispatch through an engine at all: asking DuckDB whether it "supports" webhook retry backoff is a category error, not a governance win, and the framework used to force an answer anyway because the closure test needed something to compare against.

This is a **scoping** change, not a fourth verdict: `Support` stays `supported` / `unsupported` / `ignored_with_warning` / `undeclared`, and `verdict()` callers are unchanged in meaning. What changes is the **checklist**: which leaves an engine is asked about at all.

**The tag is mandatory and explicit: there is no "untagged means core" default.** Every `config.*` field carries `json_schema_extra={"engine_scoped": True}` or `{"engine_scoped": False}` in `aqueduct/config.py`:

```python
max_sample_rows: int = Field(..., json_schema_extra={"engine_scoped": True})
api_key: str | None = Field(..., json_schema_extra={"engine_scoped": False})
```

A field carrying **neither** key raises `CapabilityScopeError` naming the field and both legal resolutions, the moment the walker runs. An earlier design let an absent tag fall back to "core" implicitly; that was rejected; it let a brand-new field (or a genuinely engine-scoped one someone forgot to mark) disappear into the core bucket with nobody deciding, silently deleting the `engine_key_ignored` warning path it should have had. Requiring the `False` half explicitly is what makes "core" a decision instead of an omission. `aqueduct/executor/config_leaves.py::all_config_leaves()` yields the `True`-tagged fields (the checklist every engine must have a verdict for); `core_config_leaves()` yields the `False`-tagged complement (leaves that never appear in any engine's table at all; not a question that gets asked). Both are derived from the SAME per-field tag, so they cannot drift apart, and no committed snapshot file exists to go stale either: see "Why no snapshot file" below. Reclassifying a key is now a one-word `True`↔`False` diff at the field itself, reviewable in a pull request: the property the dropped snapshot file existed to provide.

**`engine.<name>.*` is positionally owned.** A leaf under a per-engine namespaced block (`engine.spark.master_url`, `engine.spark.conf`, `engine.duckdb.*`) can only ever mean something to that ONE engine, so it appears ONLY in that engine's own checklist: `all_config_leaves(engine="spark")` excludes every other engine's `engine.<name>.*` leaves, and `capability_tooling.governed_leaves(engine=...)` threads the same filter through `check`/`sync`/`scaffold`/`docs`. Spark's table therefore has 208 rows (191 grammar + 17 engine-scoped config, including its own two `engine.spark.*` leaves) and DuckDB's has 213 (191 + 22: 15 shared engine-scoped leaves, minus Spark's two, plus its own seven `engine.duckdb.*` leaves: `memory_limit`, `threads`, `database_path`, `extension_repository`, `s3_key_id_secret`, `s3_secret_access_key_secret`, `s3_region`). Because a field namespaced to one engine has no coherent "core" reading, a field discovered there tagged `False` (or untagged) is ALSO a contradiction and raises `CapabilityScopeError` at the walker.

**Why no snapshot file.** An earlier design considered committing a generated `core_config_leaves.yml` and diffing it in CI, the same pattern `docs/compatibility.md` uses. It was dropped: the per-field tag already IS the single source of truth, and it sits AT the field it describes rather than in a generated copy. What replaces the snapshot is one build-enforced invariant plus a check that already existed:

| someone does this | caught by | how loud |
| :- | :- | :- |
| untags a key some engine declared `unsupported`/`ignored_with_warning` | the invariant test (`tests/test_capabilities/test_config_scope_invariant.py`) | red build, names the leaf |
| untags a key but leaves the rows behind | the existing orphaned-row check (`dev capabilities check`) | red build, names the leaf |
| untags a key every engine declared `supported` | nothing, and nothing needs to: a `supported` verdict emits no warning, so there was no warning path to delete | inert by construction |
| adds a config field anywhere, forgets the tag | the walker raises `CapabilityScopeError` naming the field | every command fails locally |
| tags a field under `engine.<name>.*` as `False` (or leaves it untagged) | the walker raises `CapabilityScopeError`: a contradiction, not a valid state | every command fails locally |
| tags a new field `True` | new `undeclared` rows | red build |

The load-bearing row is the first: a leaf some engine declares non-`supported` has a live user-visible warning path (`_warn_ignored_config_keys` emits `engine_key_ignored` for any explicitly-set leaf whose verdict isn't `SUPPORTED`). Reclassifying such a leaf to core would silently delete that warning path with nothing else noticing: the only keys whose reclassification can destroy user-visible behavior are exactly the keys the invariant test forbids reclassifying.

**`explicitly_set_config_leaves()` is narrowed too.** `aqueduct/config.py::load_config()` calls it to find which leaves the user actually wrote, then calls `caps.verdict(leaf_id)` for each; once a core leaf leaves the checklist there is no row for it in any engine's table, so this walker narrows to the same tag; otherwise it would ask `verdict()` about an id no engine declares.

### Where the per-engine differences are published

There is one place to look up what an engine does with a given leaf: the engine matrix in `docs/compatibility.md`. It is generated from the same YAML declarations the compiler enforces, by `aqueduct dev capabilities docs`. Reading a verdict there and reading the gate's behaviour are the same act, so the published matrix cannot drift from the enforced one.

This document therefore describes the grammar once, engine-neutrally, and does not annotate each feature with per-engine footnotes. When you need to know whether your target engine runs a feature, the matrix answers it, including the `hint` explaining why an `unsupported` leaf is unsupported and whether that is permanent or not yet built.

### Type leaves

The hub type vocabulary (§9.1's `aqueduct.typehub`) is itself governed by the capability framework. One `type.<constructor>` leaf exists per hub type constructor (`type.boolean`, `type.array`, `type.decimal`, `type.timestamp_tz`, and so on, derived from the hub's own constructor enumeration rather than hand-listed) plus one `type.native.<engine>` leaf per registered engine for that engine's `<engine>:<spelling>` escape hatch. The native namespace is governed, not exempt: `type.native.spark` is `supported` on Spark and `unsupported` on DuckDB, and `type.native.duckdb` the reverse, so writing a DuckDB-only spelling into a Blueprint compiled for Spark is a compile-time error naming the offending spelling, not a runtime parser crash on the wrong engine. The gate walks every inventoried type surface (Channel `cast` columns, Ingress `schema_hint` fields, UDF `return_type`) recursively, so a composite spelling like `array<map<string,int>>` checks `type.array`, `type.map`, `type.string`, and `type.int` individually: the leaf-verdict question ("does the engine implement this constructor at all").

`ExecutorProtocol.render_type` (below) and `aqueduct.executor.protocol.render_native_type()` close the runtime half: they map a compiled spelling to each engine's own native type-system spelling at cast/schema_hint/UDF-return_type execution time, so a `supported` `type.*` leaf is backed by a real, working runtime path on both shipped engines; a composite spelling like `array<int>` renders to DuckDB's own `INTEGER[]` before the cast reaches DuckDB's parser. See §9.3 for the vocabulary's honest framing: a superset that surfaces engine divergence as a compile-time refusal rather than hiding it.

### Engine notes: differences a verdict cannot express

A verdict answers "does this engine run this leaf". Some differences are not of that shape: the engine runs the leaf, and the result differs in a way a reader needs to know. Those are listed here because there is nowhere in the data model to put them.

- **DuckDB `mode: append` is not atomic.** It reads the existing file, appends with `UNION ALL BY NAME`, and rewrites the target. A failure part-way through can leave the target damaged. Spark's `append` adds files to a directory and does not rewrite what is there.
- **DuckDB's Probe `row_count_estimate` is EXACT, not an estimate**: see §4.4. `execution_partitions` has no DuckDB equivalent at all (single-process, no partition concept) and is skipped with its own runtime warning rather than a value.
- **DuckDB materialises some Channel ops eagerly.** `sql`, `join`, `deduplicate` with a key, and every Funnel mode write into a uniquely named temp table at once instead of staying lazy. DuckDB's `register()` binds a name in a mutable catalog rather than capturing a value, and module ids are reused as registration aliases across a run, so a relation left unevaluated could resolve against the wrong binding later.
- **Bare `timestamp` is a hard compile-time rejection, not a warning** (§9.2): there is no deprecation window and no suppress mechanism; an author must write `timestamp_tz` or `timestamp_ntz` explicitly, so a Blueprint's zone semantics can never differ across engines by silent accident.

### The engine and the healing loop

Self-healing is engine-aware in two respects and engine-neutral in the rest. The engine supplies the healing prompt's persona and rules through `ExecutorProtocol.prompt_rules`, so an LLM diagnosing a DuckDB failure is told about DuckDB rather than about Spark. The engine also declares how its own exceptions map to `FailureContext` fields, through `ExecutorProtocol.extract_error`. Everything downstream of the prompt (the PatchSpec grammar, the apply gates, the budget, the patch lifecycle) is shared, because a patch is a Blueprint edit rather than engine-specific code.

Two consequences are worth stating plainly. A Blueprint patched after a failure on one engine carries no record of which engine produced the patch: if you heal on one engine and deploy on another, the patch travels with the Blueprint and gets no check beyond the ordinary compile gate. And the error signature that backs budget accounting (§8.5) is computed from the error class, location, and message, so it does not distinguish two engines that fail the same way.

### The compile gate

`aqueduct/compiler/capability_check.py` runs as the last step of `compile()` (see §3, the compiler pipeline). A module using an `unsupported` leaf fails compilation with a `CompileError` naming the module, the leaf, the engine, and the capability's hint. A module using an `ignored_with_warning` leaf gets a suppressible warning under rule_id `engine_key_ignored`, following the same `warnings.suppress` mechanism as every other compiler warning (see §4.2). A `requires` version constraint does not fail compilation: compile time has no way to know which dependency versions are installed in the environment that will run the job.

The gate checks three kinds of leaf:

1. `module.type.<Type>`, the module kind, emitted for every module. An engine that does not run a whole module type fails compilation cleanly instead of crashing mid-run.
2. The per-module config-dispatch leaves: Channel op, Egress mode, format and on-new-columns policy, Ingress format, Junction and Funnel fan mode.
3. The `feature.*` leaves the compiled Manifest actually exercises, derived from real Manifest fields rather than a hardcoded list. `feature.python_udf` and `feature.java_udf` come from each `udf_registry` entry's `lang`, so a Blueprint that declares no UDF exercises no UDF feature.

On an engine that declares every leaf `supported`, all three kinds are a no-op and the gate stays silent.

**Per island (2.34).** A Blueprint compiled with one or more modules pinning `engine:` (§4.3's "Cross-engine handoff" subsection) is partitioned into engine islands before this gate runs, and each island's modules are checked against its OWN engine; never against a different island's engine. This is what makes "an island whose engine is not registered" a `CompileError` (the same `UnknownEngineError` above) rather than only ever checking the single `deployment.engine` default. For a single-engine Blueprint (no module pins `engine:`) there is exactly one island, and this degenerates to the pre-2.34 single-gate call exactly.

The manifest-scoped `feature.*`/`type.*` leaves a `udf_registry` entry drives (UDF language, UDF return type) are NOT owned by one module (a UDF is registered once and referenced from SQL text by name) so `aqueduct/compiler/udf_attribution.py::attribute_udfs_to_islands` attributes each UDF to the island(s) whose SQL can actually reference it before the gate runs, reusing the same sqlglot parse `aqueduct/compiler/lineage.py`'s column lineage already does (never a second SQL parser). This is what makes the phase's flagship shape work: a Java UDF used only inside a Spark island's Channel SQL compiles cleanly even with an unrelated DuckDB island present elsewhere in the same Blueprint, because DuckDB's `feature.java_udf: unsupported` verdict (DuckDB is not on the JVM; a permanent gap, unlike `feature.python_udf`, which both engines support) is only checked against islands that actually reference the UDF. Attribution is fail-closed: a SQL-bearing construct sqlglot cannot parse keeps its island in that UDF's checked set rather than dropping it, and a UDF with no positively-attributed island AND no unparseable construct anywhere falls back to every island; the same conservative behavior as before per-island UDF attribution existed. The scanned surfaces are Channel `op: sql`'s `query`; `op: join`/`op: filter`'s `condition`/`expr`; `op: deduplicate`'s `order_by`; `op: sort`'s `order_by`/`columns` (either spelling, a string or a list; a UDF call there is legal on both engines even though Spark's own sort implementation only honors a trailing `ASC`/`DESC` token and would fail such a call at runtime, since the same field genuinely invokes the UDF on a DuckDB-resolved island); any Channel's `spillway_condition`; a Junction `conditional` branch's `condition`; and an Assert `sql`/`sql_row` rule's `expr`. Channel `op: select`'s column list is deliberately excluded (it names columns, not expressions, so a UDF call written there fails at runtime on either engine regardless of the gate) as are Probe/Assert `type: custom`'s Python callables, which carry no SQL text to scan.

The synthetic Handoff module and its runtime execution are specified in [Engine scope and boundaries](08-polyglot.md).
### The version check

`aqueduct doctor` validates the constraint the compile gate cannot. `aqueduct/doctor/checks_io.py::check_capabilities` walks a compiled Blueprint's used capabilities and, for each one carrying a `requires` constraint, compares the installed dependency version (via `importlib.metadata`) against the declared specifier, reporting `ok`, `fail`, or `skip` (dependency not installed) per capability. `docs/compatibility.md` lists the version constraints each engine currently declares.

### How the leaf set stays honest

The canonical leaf set is derived rather than hand-maintained. Module types and pydantic schema fields come from `aqueduct/parser/schema.py` introspection; Channel ops, Probe built-in signal types, Egress modes, and Junction and Funnel fan modes come from named constants sitting next to their dispatch code (`channel_ops.py`, `probe_plugins.py::BUILTIN_SIGNAL_TYPES`, `spark/egress.py`, `spark/junction.py`, `spark/funnel.py`); a small hand-curated set covers cross-cutting feature flags and the few formats with a dedicated code path (`aqueduct/executor/capability_leaves.py`). Every `aqueduct.yml` key comes from `AqueductConfig` introspection (`aqueduct/executor/config_leaves.py`).

The closure test (`tests/test_capabilities/test_closure.py`) compares that derived set against each engine's YAML read straight from disk, and fails the build if a derived leaf has no row, if a row is still `undeclared`, or if a row names a leaf that no longer exists.

Reading the YAML from disk rather than from the loaded registry is what makes the test meaningful. The walker is code and the declaration is data, and the test is only worth running if the two are independent sources that can actually disagree. A test that compares the registry against the walker its own table was generated from cannot fail, whatever the table says.

### Verdict-to-test linking

"`supported` requires a test" (see Verdicts, above) was policy rather than mechanism until every `supported` **EXECUTION** row: the leaves `aqueduct/executor/capability_leaves.py::execution_leaves()` derives (`module.type.*`, `channel.op.*`, `probe.signal.*`, `ingress.format.*`, `egress.format.*`/`.mode.*`/`.on_new_columns.*`, `junction.mode.*`, `funnel.mode.*`, `feature.*`); gained an optional `tests:` key: a list of pytest node ids (`tests/test_executor_duckdb/test_executor.py::test_channel_filter`) or bare file paths where a whole file exercises the leaf. `config.*` leaves and the schema-authoring leaves (`module.field.*`, every `<type_lower>.field.*` and `<block>.field.*`: 2.42's per-module-type split, §4.3) are out of scope; they are warn-only or engine-invariant, with no per-engine runtime dispatch to exercise, so requiring a test id there would be busywork; `execution_leaves()` derives the in-scope set from the same per-category walkers `all_leaves()` unions, so the boundary is code, not a hand-maintained list.

`tests/test_capabilities/test_verdict_test_links.py` enforces two things per engine, reading each declaration from disk the same independent-sources way `test_closure.py` does: every `supported` EXECUTION row names at least one test id, and every declared id resolves against the real test tree (the file exists; a `::name`/`::Class::method` node id names something pytest would actually collect). A row failing either check is a genuine gap, not a formatting error: the fix is to link a real test or leave the leaf unbacked and let the build say so loudly, never to invent an id or quietly downgrade the verdict. `aqueduct dev capabilities check` also reports missing/dangling test links (informational; it does not gate this command's exit code, which stays keyed to leaf completeness) so the same signal is visible outside pytest. `sync`/`scaffold` never touch an existing row's bytes, so a `tests:` block survives a sync unchanged; a freshly scaffolded leaf gets a bare `undeclared` string with no `tests:` key at all.

### Adding a leaf: the workflow

1. Add a field to `parser/schema.py` or `config.py`, or a Channel op, write mode, fan mode, or feature flag.
2. The build breaks. Engine registration raises `CapabilityDeclarationError`, and the closure test names the offending leaf.
3. Run `aqueduct dev capabilities sync`, which appends the new leaf to every engine's YAML as `undeclared`. The build stays red, because `undeclared` is not a verdict.
4. A human replaces each `undeclared` with a real verdict for that engine.
5. The build passes.

`aqueduct dev capabilities check` reports drift without writing, which is what CI runs. `aqueduct dev capabilities docs` regenerates the engine matrix in `docs/compatibility.md` from the declarations.

The four commands live in the installed package (`aqueduct/executor/capability_tooling.py`, exposed through `aqueduct/cli/dev.py`), not in the repository's `scripts/` directory, which is not in the wheel. An engine registers only once every leaf on ITS OWN checklist (grammar leaves plus its own engine-scoped config leaves: see "Config-leaf scoping" below) carries a verdict, so an author who cannot generate the table cannot ship an engine: hand-writing ~200 rows does not scale. `scripts/capabilities.py` is a thin wrapper that forwards to the same code, so there is exactly one implementation.

### Starting a new engine

An engine author needs nothing but `pip install aqueduct-core`. `aqueduct dev capabilities scaffold --engine <name>` writes a complete `capabilities.yml` with every leaf present and every verdict set to `undeclared`, so the author is walked through the entire grammar and config surface one leaf at a time and the engine will not register until each row is a real decision. The scaffold is generated from the walkers, so it cannot go stale the way a checked-in template would.

Do not copy an existing engine's declaration. Cloning Spark's table hands a new engine 206 `supported` rows, which is a silent claim to implement the whole grammar and precisely the blindness the framework exists to prevent. Read it as a reference.

### `ExecutorProtocol`: the execution contract

A capability declaration says what an engine supports. `ExecutorProtocol` (`aqueduct/executor/protocol.py`) says how core talks to it. Every engine registers exactly one `ExecutorProtocol` instance, alongside its `EngineCapabilities`, as an import side effect of its `aqueduct.engines` entry-point module. The contract has three required members and an optional session-lifecycle pair.

**`execute`**: `(manifest, session, ...) -> ExecutionResult`. A compiled `Manifest` and an engine session handle in, a frozen `ExecutionResult` out. The common run options (`run_id`, `store_dir`, `checkpoint_root`, `surveyor`, `depot`, `resume_run_id`, `from_module`, `to_module`, `block_full_actions`, `warnings_*`) are the uniform part every engine accepts. A second group (`OPTIONAL_EXECUTE_KWARGS` in `aqueduct/executor/protocol.py`: `parallel`, `use_observe`, `sampling`, `observability_store`) is optional: the shared run path passes them to every engine, and `ExecutorProtocol.execute_kwargs` (a `frozenset[str] | None`, `None` meaning "consumes everything") names which of them the engine's real `execute()` accepts. Every caller that might pass one of these (`aqueduct/cli/run.py`, the patch sandbox gate (`aqueduct/patch/preview.py::run_sandbox_gate`)) routes through `call_execute()`/`filter_execute_kwargs()` (same module), which drops anything outside the target engine's allowlist and emits one suppressible `engine_kwarg_ignored` warning per dropped kwarg (see "Config-leaf governance", below) instead of forwarding an option the real `execute()` would raise on, or dropping it with no signal at all. An engine's real `execute()` therefore never receives an option it cannot honour, and the caller is told when that happened.

**`extract_error`**: an engine exception (or `None`) mapped to a `FailureContext` field dict (`error_class`, `root_exception`, `sql_state`, `suggested_columns`, `object_name`). Required, so an engine cannot register without a way to turn its own failures into the structured root-cause block the healing LLM reads.

**`prompt_rules`**: a `PromptRules` pack, the engine-specific half of the healing system prompt. It carries `persona` (the prompt's opening line), `root_cause_note` (what the engine's structured root-cause block contains, the prose counterpart of `extract_error`'s output), `rules` (the engine's error idioms, its advice, its API and config references), and `defer` (a `DeferRules`: the engine's slice of the defer-to-human section, naming the infrastructure it can actually fail on and the languages its UDFs are written in). All are required except `DeferRules.extra_bullets`, where "this engine has no extra defer category" is a complete answer.

**`make_session` / `close_session`**: `(SessionSpec) -> session` and `(session) -> None`, how an engine builds the handle `execute` runs against and tears it down. `SessionSpec` is the engine-agnostic construction request (`blueprint_id`, `engine_config`, `master_url`, `quiet`, `quiet_startup`, `engine_options`), the union of what registered engines need; an engine reads the fields it understands, so a single-node engine ignores `master_url`. `engine_options` is an opaque per-engine bag reserved for session needs the named fields don't cover: unpopulated by core, read only by an engine that understands its keys. Both members are optional at registration, because a compile-only engine or a test double has no session. The run path resolves them through `session_factory()` and `session_closer()`, which raise `EnginePluginError` naming the engine if a runnable engine reached the CLI without a factory.

**`render_type`**: `(HubType | NativeType) -> str`, one parsed hub type (§9.2's `aqueduct.typehub`) rendered to the engine's own native type-system spelling. Optional at registration, the same optionality class as `make_session`/the diagnostic readers below, not the required class: a third-party engine can register without a complete type mapper. The degrade contract is narrow and explicit rather than a silent fallback: an engine with no `render_type` still runs a Blueprint using only its own native escape-hatch spellings (`"<engine>:<spelling>"`, rendered as `.spelling` verbatim; no mapping needed) and any spelling its own runtime parser accepts raw, but a hub spelling (`bigint`, `array<int>`, …) against it is refused, not silently forwarded to a parser that cannot read it. `aqueduct.executor.protocol.render_native_type(engine, spelling)` is the one seam every engine's cast / schema_hint / UDF-return-type runtime consumption routes through: it parses `spelling` via `typehub.parse_type`, unwraps a same-engine `NativeType` verbatim, raises `EnginePluginError` for a foreign-engine `NativeType` (defensive; the compile-time `type.native.*` gate should already have refused it on any gated path) or for a hub type with no `render_type` registered, and otherwise calls the engine's `render_type`. Both shipped engines register a mapper: Spark's (`aqueduct/executor/spark/type_render.py`) is mostly identity, since the hub's canonical spellings already match Spark DDL character-for-character, except the timestamp pair (`timestamp_tz` renders to Spark's plain `timestamp`, `timestamp_ntz` to Spark's own `timestamp_ntz`); DuckDB's (`aqueduct/executor/duckdb_/type_render.py`) renders every constructor to DuckDB's own SQL spelling, including the composite constructors (`array<T>` → `T[]`, `map<K,V>` → `MAP(K, V)`, `struct<name:type,...>` → `STRUCT(name TYPE, ...)`, recursively). Both mappers are pure string logic (no `pyspark`/`duckdb` import) so neither needs the lazy-import discipline `execute` does.

### The healing prompt is composed

The healing system prompt is composed, not monolithic. The engine-independent scaffold (the PatchSpec schema, the op-selection table, the provenance rules, the output contract, the generic defer categories, the coaching and history sections) lives in the agent layer and holds no engine-specific text. At prompt-build time the agent pulls the target engine's `PromptRules` pack through this registry and renders it into the scaffold's engine slots. The agent layer imports no engine specifics, and the engine layer imports nothing from the agent layer. A new engine therefore ships its own healing persona and rules with its executor: it cannot inherit another engine's advice by accident, and it cannot register with none at all.

The scaffold is not one string. Parts of the prompt, including the defer-to-human section, are assembled at request time and only appear under certain settings. The guard against engine text leaking across therefore composes the whole prompt for a non-Spark engine, across every combination of those settings, and checks that no Spark vocabulary survives. Scanning the template constants alone would miss every fragment built around them.

Both requirements are enforced structurally rather than by convention. `ExecutorProtocol.__post_init__` raises `EnginePluginError` at construction time if `execute`, `extract_error`, or the `PromptRules` pack is missing or incomplete. `get_executor(engine)` (`aqueduct/executor/__init__.py`) and `get_protocol(engine)` (`aqueduct/executor/protocol.py`) resolve through the same `load_engines()`-backed registry that `get_capabilities()` uses, and raise the same `UnknownEngineError` for an unregistered engine. Every failure on this seam is an `AqueductError`, never a bare builtin.

Constructing an engine's protocol object never imports the engine's own heavy dependencies. For Spark, only calling `execute(...)` imports `pyspark`.

### Config-leaf governance

Everything above governs the Blueprint grammar. `aqueduct.yml`, the engine config, has its own leaf set: every `AqueductConfig` pydantic field, derived the same way (`aqueduct/executor/config_leaves.py::all_config_leaves()` walks the real `aqueduct/config.py` models rather than a hand-written list) and namespaced `config.*`, giving leaf ids like `config.engine.spark.master_url`, `config.stores.observability.backend`, and `config.agent.sandbox_master_url`. A field typed as a list or dict of sub-models (`stores.depots`, `agent.cascade`) is one atomic leaf rather than one per dynamic key. These leaves fold into the same closure test as the grammar leaves, so a registered engine must carry an explicit verdict for the union of both sets.

The gate exists because a key that means nothing on the selected engine, `engine.spark.master_url` on a single-node engine for instance, was otherwise a silent no-op: accepted, validated, then ignored with no signal. At config-resolution time the gate checks every leaf the user explicitly set (pydantic's `model_fields_set`, at each nesting level, so untouched defaults never warn) against the target engine's verdict.

Config-leaf verdicts always warn and never error. That asymmetry with the Blueprint gate is deliberate. A Blueprint is written for one pipeline on one engine, so an `unsupported` leaf there is a `CompileError`. An `aqueduct.yml` is not: the same file is expected to stay valid across engines, deployment profiles, and test overrides, and hard-failing config load over one inert key would make that impossible. So a config leaf resolving to anything other than `supported` emits one suppressible `engine_key_ignored` warning, using the same rule id and suppression machinery as the compile-time gate, and loading proceeds. `aqueduct/config.py::load_config()` runs the check once `deployment.engine` is validated, immediately before returning the resolved `AqueductConfig`.

One limit is worth knowing: the rule covers keys that are inert on an engine, not keys that mean something different on one. A key that changes results rather than being ignored is not something a warning can describe.

### `engine_kwarg_ignored`: the sibling rule for `execute()` kwargs

`engine_key_ignored` covers Blueprint/`aqueduct.yml` **keys**. `execute()` **kwargs** (the optional run-option arguments a caller passes to `ExecutorProtocol.execute`: `parallel`, `use_observe`, `sampling`, `observability_store`, listed as `OPTIONAL_EXECUTE_KWARGS` in `aqueduct/executor/protocol.py`) are a different surface with the same failure mode: a kwarg meaningful to one engine and meaningless to another. `engine_kwarg_ignored` is its own rule id, using the identical `aqueduct.warnings.emit` suppression machinery (`warnings.suppress` in `aqueduct.yml`, or `--suppress-warning` on the CLI), so it composes with every other warning rule the same way.

`call_execute(engine, ...)` / `filter_execute_kwargs(engine, kwargs, ...)` (`aqueduct/executor/protocol.py`) are the single mechanism: they look up the target engine's `ExecutorProtocol.execute_kwargs` allowlist, drop any `OPTIONAL_EXECUTE_KWARGS` name outside it, and emit one `engine_kwarg_ignored` warning per dropped kwarg naming both the kwarg and the engine. An engine that declares `execute_kwargs=None` (Spark today: its real `execute()` has a parameter for every name in `OPTIONAL_EXECUTE_KWARGS`) gets no filtering at all. Every caller that might pass one of these kwargs routes through this seam: `aqueduct/cli/run.py`'s run loop and the patch sandbox gate (`aqueduct/patch/preview.py::run_sandbox_gate`, which also resolves its target engine's own `ExecutorProtocol` for its session and `execute()` rather than hardcoding Spark); so a kwarg the target engine cannot honour is never silently dropped and never raises `TypeError` out of a mismatched signature.

---

