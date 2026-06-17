# Changelog

*June 2026 — history rewritten from a single 211-commit main branch into
7 feature-phase branches with per-commit surgical splits, preserving
all file content, release tags, and GPG signatures. Original history
preserved at `backup/2026-06-06-original`.*


All notable, consumer-facing changes to Aqueduct (`aqueduct-core`) are
recorded here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [SemVer](https://semver.org/). The stability contract
applies from v1.0.0 — during alpha/RC, breaking changes may land in any
release and are marked **BREAKING**.

## [Unreleased]

## [1.3.0] — 2026-06-17

### Added
- **Phase 53a — Pluggable object store for driver artefacts.** The driver no longer hard-writes observability blobs and the patch lifecycle to its local filesystem — both now route through a new `stores.blob` object store with a `local` default (byte-identical to the previous on-disk layout) plus `s3` / `gcs` / `adls` backends served by one `fsspec` handle (the new `[object-store]` extra, folded into `[stores]`). New `aqueduct/stores/object_store.py` introduces an `ObjectStore` transport over a pluggable `local`/`fsspec` backend with two semantic stores on top: `BlobStore` (zstd externalisation of `manifest_json` / `stack_trace` / `provenance_json`, absorbing the former `surveyor/blob_store.py`, which stays as a back-compat shim) and `PatchStore` (the `pending`/`applied`/`rejected` lifecycle). Groundwork for running on an ephemeral k8s pod that leaves no local-FS artefacts under its cwd.
- **Phase 53b — Patch lifecycle wired through the object store + `patch_index`.** Patch bodies are now written to the `PatchStore` (`pending`/`applied`/`rejected`) and their status + signature metadata recorded in the `patch_index` table. The heal cache (pending-reuse, zero-token replay, signature-matched coaching, "do not repeat" history) now resolves via SQL queries against `patch_index` instead of scanning the local `patches/` directory — identical behaviour, but backend-blind, so it works when patches live on s3/gcs/adls. The replay path fetches the one body it needs from the object store by `object_key`. Heal-cache content the LLM sees is unchanged (no `PROMPT_VERSION` bump). Local-checkout commands (`patch apply` / `patch reject`) stay on the filesystem but now flip the index status so the cache stays consistent.
- **`aqueduct patch pull <id> --blueprint <bp>`.** Fetch a patch body from the object store into a local checkout for review (Profile C: heal on a cluster, `git diff` + apply on a laptop). With a `local` object store it copies the file; with s3/gcs/adls it downloads the body.
- **`patch_index` observability table.** New relational table recording the status + signature metadata of every patch (`pending` → `applied` | `rejected`) plus the body's `object_key` into the object store. The backend-blind truth behind the heal cache.
- **Extras policy codified.** `AGENTS.md` now documents the two-axis rule (per-vendor leaves + capability aggregates, no feature-named extras); the object store became the `object-store` leaf inside `[stores]`.
- **Phase 55 — OpenLineage emission.** New top-level `lineage:` config block (`openlineage_url`, `openlineage_namespace`). When `openlineage_url` is set, the Surveyor emits OpenLineage RunEvents — `START` on run begin, `COMPLETE` on success, `FAIL` on terminal failure (with an `errorMessage` facet) — to an OpenLineage backend (Marquez / DataHub / Atlan) over `httpx` in a daemon thread (async, non-blocking, best-effort; never blocks or fails the run). Output datasets carry the column-level **`columnLineage`** facet, serialized from the same compile-time lineage as the `column_lineage` table (`compiler.lineage.compute_lineage_rows`, factored out for reuse) — field-to-field arrows in Marquez/DataHub/Atlan; sqlglot resolves ~90% of SparkSQL, unresolved columns → `UNKNOWN`. Disabled by default (zero cost when `openlineage_url` is unset). **Naming note:** the new `lineage:` block is NOT `stores.lineage`, which has been inert since Phase 38 — flagged in `aqueduct.yml.template` and `docs/specs.md` §7.4.
- **Phase 54 — CI kit for `approval: ci`.** New `aqueduct patch import <patch.json> --blueprint <bp>` applies a received patch and commits it in one atomic step (`patch apply` + `patch commit`; `--no-commit` stages only) — the entry point a CI runner calls after receiving the `on_patch_pending` webhook. The engine stays serverless: no published/versioned GitHub Action. The CI webhook payload schema (envelope + `_aq_meta`) is documented in `docs/production_guide.md`, and a copy-paste example workflow wiring `import` + `gh pr create` ships at `docs/templates/ci-heal-workflow.yml` — a snippet you own. The structured commit-message builder (`---aqueduct---` trailer) is now shared between `patch commit` and `patch import` via `aqueduct/patch/ci.py`.

### Changed
- **Code-audit remediation.** A full intent-vs-implementation audit (`.dev/audits/`) surfaced doc-drift and half-wired config; this batch fixes the actionable items. (1) **UDF docs corrected:** specs §5.4 now documents the real registration contract — python UDFs import `module:`/`entry:`, java/scala load `jar:`/`class:` — replacing the never-implemented inline `fn:` / `pandas` / `spark_sql` form (that form is now a roadmap item in TODOs). (2) **`udf_registry` is now schema-validated** (`UdfSchema`, `extra="forbid"`) — a typo in a UDF entry bounces at parse instead of failing late. (3) **`agent.sandbox_master_url` now applies during self-healing** — it was silently ignored in the heal-staging sandbox gate (hardcoded `None`); the configured value is now threaded through. (4) **`aqueduct patch import` accepts a CI webhook envelope** (`{…envelope, "patch": {…}}`), validating it via `validate_ci_payload` and unwrapping the body; it also pre-flights `git rev-parse` so a non-repo checkout fails *before* the Blueprint is mutated. (5) **OpenLineage emits a START for healed re-runs** — a terminal event for a run that bypassed `start()` (a heal iteration's fresh run_id) now lazily emits a paired START first, so consumers never see a START-less COMPLETE/FAIL. (6) docs: §10.3 session-stop behavior corrected; §7.4 documents the two OpenLineage v1 facet/namespace limitations. (7) the "no Phase artefacts" AGENTS rule was amended to match practice (allowed in code comments + CHANGELOG/TODOs; forbidden on user-facing surfaces) and the 31 doc/template/gallery mentions were cleaned.
- **Incremental-Channel watermark is persisted to the Depot only.** The local `watermarks/<bp>__<channel>.json` sidecar was dropped; a one-time migration reads any sidecar left by an older release into the Depot and deletes it. **Behavior change:** an incremental Channel now requires a configured Depot to persist its position — without one, each run re-scans all source data (a warning is logged). Configure `stores.depot`.
- **`schema_snapshot` probe payloads live only in `probe_signals`.** The redundant local `snapshots/<run_id>/<probe>_schema.json` sidecar was removed; the payload was already written to the observability store.


## [1.2.2] — 2026-06-14

### Added
- **Benchmark v2.** The benchmark history store gains a **Postgres backend** (`stores.benchmark.backend: postgres`), mirroring the observability/lineage store pattern — rows live in a `benchmark` schema, reusing the dedup'd connection pool and `?`→`%s` cursor. New `stores.benchmark` config block (`backend`, `path`, `persist`, `gate_on_regression`) gives `--set` real fields to target. New **`aqueduct benchmark-stats`** command aggregates the store into a model leaderboard (best model), a hardest-scenario ranking, and a by-day pass-rate trend — computed from the latest row per `(scenario, model)`, `--format table|json`. Parallel benchmark runs (`--workers > 1`) now print one clean progress line per completed pair from the main thread (previously silent — `logger.info` only), and the results-table rule width is fixed (was one char wider than the rows).
- **`-s/--set PATH=VALUE` — universal config override.** One repeatable flag replaces the scattered one-off override flags. Dotted-path grammar overlays any `aqueduct.yml` or Blueprint value for a single invocation — in-memory, never persisted — as the highest-precedence layer (`--set` > blueprint `agent:` > `aqueduct.yml` > defaults). One flat namespace routes each path to whichever schema owns it: blueprint-side `agent.*` (e.g. `agent.approval_mode`) lands on the Blueprint, engine-side (`agent.budget.*`, `deployment.*`, `danger.*`) on `aqueduct.yml`. Values coerce to bool/int/float/null else string; `PATH:=JSON` parses structured values. Unknown paths error with a nearest-sibling suggestion; `--set danger.*` prints a loud single-run warning. Available on `run`, `benchmark`, `heal`. Example: `aqueduct run bp.yml --set agent.approval_mode=auto --set deployment.master_url=spark://h:7077`.

- **`aqueduct lint` — static Blueprint linting.** A new command for style + correctness checks beyond `validate` (which only parses/schema-checks) and beyond the compiler's perf warnings. Runs a registry of rules with stable `AQ-LINT<NNN>` ids over the parsed graph (orphan modules, duplicate edges, non-descriptive labels) and over Channel SQL via sqlglot (cartesian joins, `SELECT *` into an Egress, aggregate/`GROUP BY` mismatch, un-aliased self-joins). All initial rules are advisory (`warn`) so a result exits `0`; `--strict` promotes findings to errors so a non-empty result exits non-zero for CI gating. `--format json` emits a versioned document. Rules are conservative — unparseable SQL is skipped rather than flagged.
- **`--format json` on `doctor` and `validate`.** Both commands now emit a stable, versioned JSON document (`schema_version` field) for CI consumption — `doctor` reports every check (`{summary, checks[]}` with name/status/group/detail/elapsed), `validate` reports per-file results (`{summary, files[]}`). Text-mode `doctor` additionally groups checks into labelled sections (Config, Stores, Spark, Agent, …) instead of one flat list.
- **`aqueduct run --sandbox` — dev dry-run.** Compile and execute a Blueprint against sampled inputs with every Egress skipped — no writes, no self-healing, no observability persistence. `--sample N` caps rows per Ingress (default `1000`, `0` = no limit). Reuses the patch-validation sandbox transform (Gate 3) so behaviour matches the heal sandbox: Egress modules are stripped and snapshotted, each Ingress is read through `.limit(N)`. A fast feedback loop for iterating on transforms without touching sinks. Requires `engine: spark`.
- **Linear-edge sugar.** `edges:` may now be omitted entirely. When it is — and every module is a single-input/single-output type (Ingress, Channel, Egress, Assert) — the compiler chains modules in declaration order, injecting `main`-port edges flagged `injected: true` in the Manifest (`edges[].injected`). Blueprints that omit `edges:` while using Junction / Funnel / Arcade / Probe / Regulator fail compilation with a clear error, since those ports can't be inferred in a flat chain. Single-module Blueprints need no edges.

### Changed
- **`agent.approval` is now the canonical Blueprint key** (was `agent.approval_mode`). Values are unchanged (`disabled`/`human`/`auto`/`ci`). `agent.approval_mode` keeps working as an input alias with a parse-time `[deprecated]` warning; when both keys are present, `approval` wins. Templates, gallery examples, and docs now use `approval`. The internal field/attribute name is unchanged, so existing tooling reading the Manifest is unaffected.

### Deprecated
- **`agent.approval_mode`** (Blueprint key) → use `agent.approval`. Parses with a warning until 2.0.
- **`benchmark --provider` / `--base-url` / `--timeout`** → use `--set agent.provider=…` / `--set agent.base_url=…` / `--set agent.timeout=…`. The flags still work but print a deprecation warning; they will be removed in 2.0.
- **`benchmark --no-persist` / `--store-path` / `--gate-on-regression`** → use `--set stores.benchmark.persist=false` / `--set stores.benchmark.path=…` / `--set stores.benchmark.gate_on_regression=true`. The flags still work but warn; removed in 2.0.

## [1.2.1] — 2026-06-11

### Added
- **Phase 47 — `replace_macro` patch operation.** Bad SQL often lives in a macro: the agent is instructed to preserve `{{ macros.* }}` references in module queries, so a root cause inside the macro body was previously unreachable by any patch op. The new op replaces the body of an **existing** macro (`{op: replace_macro, name, value}`) — replace-only, so hallucinated macro names are rejected at apply time with the available list. Re-expansion runs through the existing compile + lineage gates, catching parameter mismatches and broken columns in every consuming module before the patch lands. Multiline bodies are written as YAML `|` block scalars. Because one macro change affects all modules referencing it, the blueprint template now recommends `replace_macro` in `guardrails.forbidden_ops` (alongside `set_spark_config`) so it always gets human review. Grammar is now 14 ops; PROMPT_VERSION bumped to 1.3.
- **Phase 46 — Provider + budget hardening.** Four reliability fixes for the healing loop. **Provider retry:** transient errors (HTTP 429/503/529) from both providers now retry with exponential backoff + jitter per the new `agent.retry: {max_retries: 2, backoff_seconds: 2.0}` block — the server's `Retry-After` header is honored, and sleeps are always capped by the remaining per-call budget deadline so a retry can delay an attempt but never overrun `agent.budget.max_seconds`; shared by production heal and `aqueduct benchmark`. **Anthropic parity:** `agent.base_url` (gateways/proxies) and `provider_options` are now honored by the anthropic provider too — previously accepted by config but silently ignored (`ollama_*`-prefixed options are filtered out, as the block is shared with openai_compat). **Gate-time exclusion:** `agent.budget.max_seconds` now caps LLM-conversation time only — deep-loop validation gates (sandbox replay, lineage, explain) run on a paused clock, so a slow sandbox can no longer exhaust the heal budget; `BudgetTracker.summary()` reports `excluded_gate_seconds`. **Cascade residue:** `max_tokens_total` now spans the whole cascade (each tier gets the remaining allowance; previously every tier received the full cap), `healing_outcomes` gains a `model_cascade_position` column and its `model` column now records the producing tier's model instead of the top-level `agent.model`, `aqueduct doctor <blueprint>` checks each cascade tier's credentials/endpoint (`cascade-tier-N` warns on a missing `ANTHROPIC_API_KEY` or base_url before escalation fails at heal time), and `agent.model: [cheap, expensive]` is accepted as shorthand for a default-settings cascade.
- **Webhook envelope, diagnosis payload, and delivery retry.** All four webhook events now share one standardized default body: `{"event", "timestamp", "run_id", "blueprint_id", "data": {…}}` (sent when `payload:` is omitted). Patch events (`on_patch_pending` / `on_ci_patch`) carry the agent's structured diagnosis — `root_cause`, `rationale`, `confidence`, `category` (plus `diagnosis`/`suggestions` for defer patches and `source: replay` for heal-cache re-stages) — both in the default body and as `${root_cause}`-style template variables for custom payloads (e.g. Slack text); zero extra LLM calls, the fields come from the PatchSpec the heal already produced. Delivery gets one automatic in-thread retry on 429/5xx/network errors (still best-effort, never blocks the run). **Behavior change:** the default (`payload: null`) body shape changed — `on_failure` previously sent the raw FailureContext JSON at the top level; it now lives under `data` in the envelope.
- **production_guide "growing up" section.** Documents when per-pipeline DuckDB observability stops being enough (fleet-wide queries, many pipelines, ephemeral drivers, concurrent readers), the one-line switch to a shared Postgres backend, and an optional history-backfill snippet via DuckDB's `postgres` extension.
- **Phase 45 — Signature memory: Aqueduct never solves the same failure twice.** Every pipeline failure is hashed into a stable signature (error class + failed module + normalized message; a coarse variant drops the module for cross-blueprint matching), and staged/archived patches now carry the signature of the failure they fixed (`_aq_meta.failure_signature` / `failure_signature_coarse` / `source`). Three zero-token paths run before any LLM call: **pending-patch reuse** — when a patch for the same signature already awaits review, the run surfaces it and skips the LLM entirely (`stop_reason: cached`, exit `HEAL_PENDING`), ending the human/ci-mode "re-heal every run while review is pending" token burn; **exact replay** — when an archived patch already fixed this signature (confirmed via `healing_outcomes.run_success_after_patch`), it is re-validated through the normal gate pyramid with zero tokens (`stop_reason: replayed`; in human/ci mode it is re-staged with `source: replay`), and any gate failure falls through to the LLM in the same iteration; **signature-matched coaching** — the prompt's chronological last-3 patch history is replaced by nearest-signature retrieval of past (failure → validated fix) pairs (exact hash → coarse hash → same error class → chronological fill). Config: `agent.memory: {replay: true, coaching: true}` (both default on). Observability: `healing_outcomes` gains `failure_signature` + `resolution` (`llm`/`cached`/`replayed`) columns, and `aqueduct runs --heal-coverage` reports zero-token heal coverage. Benchmark is unaffected by design — it measures model skill, not cache hits. PROMPT_VERSION bumped to 1.2.
- **LLM response recovery hardening (cheap-model output poisoning).** Four recovery-pass fixes in `_parse_patch_spec` and reprompt formatting: an orphan `</think>` closer (opener stripped by the server or lost to truncation) no longer lets reasoning prose poison brace-find; when multiple fenced blocks are present, the first block containing `"operations"` is preferred over an earlier echoed-junk fence; multi-key wrappers (`{"patch": {…}, "explanation": …}`) unwrap like single-key ones when exactly one nested dict carries `operations`; the non-escalated reprompt's raw echo is capped at 4000 chars on top of the 80-line cap so a single minified-JSON line cannot flood the next turn's context.
- **Typed spillway routing — `edges.error_types` is now implemented.** The field existed in the schema since 1.0 (documented in the archived spec as "optional filter — only route these error types") but was never consumed at runtime. A spillway edge can now declare `error_types: [DataQualityViolation, …]` and receives only quarantined rows whose `_aq_error_type` label matches — multiple spillway edges act as typed catch blocks, an edge without `error_types` stays a catch-all (existing behavior unchanged), and the filter is a lazy transformation (zero extra Spark actions). To make custom errors addressable, Assert quarantine rows now stamp `_aq_error_type` with the rule's `error_type` label (falling back to the rule name); Channel rows keep `SpillwayCondition`. Guard rails: `error_types` on a non-spillway edge is now a `ParseError` (previously accepted and silently ignored), and `aqueduct doctor` warns (`spillway_error_type_typo`) when a filter entry matches no label declared in the Blueprint.

### Changed
- **`VALIDATION_GATE` (exit 4) is now emitted.** Previously defined and documented but never raised. When `aqueduct run` is in `auto` (non-interactive) mode and a generated patch is rejected by the validation pyramid (sandbox replay), the command now exits `4` (`VALIDATION_GATE`) instead of `2` (`DATA_OR_RUNTIME`) — letting orchestrators distinguish "a patch was produced but failed validation" from a plain runtime failure. Human/CI staging still exits `3` (`HEAL_PENDING`); precedence is staged → `3`, gate-rejected → `4`, else → `2`.
- **CLI exit codes now honor the documented `exit_codes` contract.** Many CLI failures previously exited `1` (`CONFIG_ERROR`) regardless of cause. They now emit the semantically correct code per `aqueduct/exit_codes.py`: runtime/data failures — git-subprocess errors (`patch commit`/`discard`/`log`/`rollback`), missing observability stores or run records (`report`/`heal`/`lineage`), `test`-suite failures, `heal` failing to produce a patch, benchmark failures/regressions — now exit `2` (`DATA_OR_RUNTIME`); bad-flag/missing-argument cases (malformed `--ctx`/`--execution-date`, missing `run_id`/scenario arg, conflicting `--error`/`--value`) now exit `5` (`USAGE_ERROR`). **Behavior change:** downstream tooling that keyed off the old `1` for these paths (e.g. Airflow retry rules) should re-check against the corrected codes. All `sys.exit()` calls in `cli.py` now use named `exit_codes.*` constants instead of magic numbers (`130` SIGINT excepted).

### Fixed
- **Assert `sql_row` + `min_pass_rate` runs one Spark action instead of two.** The pass-rate check previously executed `df.count()` and `passing.count()` as separate full passes; a single `agg(count(*), count_if(expr))` job now computes both.
- **MERGE SQL backtick-quotes identifiers.** `mode: merge` generated `MERGE INTO` statements with raw table/column names — a reserved-word merge key (`order`, `group`) or special characters broke the statement. Catalog table parts, the path-based target, and every ON-clause column are now backtick-quoted with embedded backticks escaped.
- **Quarantine frames cache when multiple spillway consumers exist.** With typed spillway routing, each consumer edge (per-`error_types` filter or catch-all) is a separate Spark action over the same quarantine frame — the parent DAG re-executed once per consumer, violating Aqueduct's own `perf_multi_consumer_no_cache` guidance. The executor now `cache()`s the quarantine frame when a module has more than one spillway edge (error-row subsets are small; in-memory cache per the Spark guide's caching strategy).
- **Internal LAN IP scrubbed from templates and tests.** Example `base_url`/master values now use placeholder hosts (`ollama.internal`, `spark-master.internal`) instead of a real private address.
- **Blueprint template guardrails example parses now.** `blueprint.yml.template` showed `allowed_paths` / `forbidden_ops` / `heal_on_errors` / `never_heal_errors` directly under `agent:`, but the schema only accepts them nested under `agent.guardrails:` (`extra="forbid"` rejects the flat form) — uncommenting the template example produced a parse error. The example is now correctly nested.
- **Compiler warning hints point to the right file.** Eight warning strings (and one config docstring) referenced `docs/SPARK_GUIDE.md`, which was renamed to `docs/spark_guide.md` in the docs split — the printed "See …" pointers led nowhere on case-sensitive filesystems. Also added the missing `{#jdbc-ingress-parallelism}` anchor those warnings link to, and a rule-id catalog (all 12 `AQ-WARN` ids with their `--suppress-warning` usage) to the Spark guide.
- **`benchmark --timeout` help text claimed the wrong default.** Said "default 120"; the actual `agent.timeout` default is 300 seconds.
- **Documentation corrected against code during the consistency sweep.** specs.md: Egress merge requires `merge_key` (not `key`; `table` accepted as target; `errorifexists` mode and `depot` pseudo-format documented), Probe config takes a `signals:` list with module-level `attach_to` (the old `config.signal:` example was silently ignored by the executor), Assert rule list gains `max_rows`/`spillway_rate`, structural lineage attributed to compile time. observability_guide: `run_records.status` has no run-level `skipped`, `patch_simulation.gate` is `lineage|sandbox|explain` with `pass|fail|warn|skip` status, `depot_kv` is keyed by `key` alone (project-wide, not per-blueprint). Engine template gains the four missing config fields (`webhooks.on_patch_pending`, `webhooks.on_ci_patch`, `agent.ci_webhook_url`, `agent.sandbox_master_url`); blueprint template gains an Arcade example, common module fields (`tags`/`depends_on`), and `provider_options`. README extras table gains the `llm` extra.
- **LLM comment-strip recovery no longer corrupts valid patches.** `_parse_patch_spec` applied its line-comment regex (`//`, `#`) to the raw response *before* attempting strict JSON parsing. The regex cannot distinguish a comment from `//` or `#` inside a JSON string value, so a valid patch like `"value": "SELECT a // 2 FROM t"` was silently truncated to `"SELECT a"` (the broken JSON was then "repaired" by json_repair) — a correct fix mutated into a wrong one with no error. Comment-stripping now runs only after strict parsing fails, and the json_repair last-ditch pass operates on the original text, so valid JSON is never touched.
- **`allow_defer: false` actually hides `defer_to_human` from the LLM now.** The schema-strip in `_build_system_prompt` targeted a `$defs.PatchSpec…anyOf` path that does not exist in pydantic v2 output (the model sits at the top level and renders the discriminated union as `oneOf`), so the strip was a no-op — while the accompanying `$defs.pop("DeferToHumanOp")` left a dangling `$ref` in the schema shown to the model. The strip now scrubs the `oneOf` entry, the `discriminator.mapping` key, and the `$defs` definition together.
- **"Previous patch attempts" prompt section shows patch rationales again.** `_load_previous_patches` read the `description` key from archived patches, but archives are written via `model_dump()` which emits the canonical `rationale` key — so the do-not-repeat coaching section always rendered empty descriptions. Reads `rationale` first with `description` as a legacy fallback.
- **Multi-model cascade escalates on `deferred` as documented.** A defer result carries a non-None patch (the diagnosis), and the cascade's patch-presence check ran before its escalation check — so a cheap tier saying "I can't fix this" ended the cascade instead of escalating to the next tier, making the documented `deferred` escalation trigger dead code. Escalation is now checked first on non-final tiers (the diagnosis is discarded); a defer on the final tier is still returned for staging.
- **Cascade tiers inherit top-level `agent.*` defaults as documented.** `allow_defer`, `deep_loop`, the multi-axis `agent.budget` (token cap, stuck/stall axes), and `last_apply_error` were not forwarded into cascade mode — tiers fell back to hardcoded `False`/defaults regardless of top-level config, and a per-tier `deep_loop: true` was silently ignored because the validation callback was only constructed when the *top-level* flag was set. All four now thread through `generate_cascade_patch`; tier `max_reprompts`/`max_seconds` override just those two axes of the inherited budget.
- **`confidence` omitted by the model no longer breaks heal log lines.** `PatchSpec.confidence` is optional, but two `logger.info` calls formatted it with `%.2f`, raising in the logging layer (and losing the line) whenever the model skipped the field. Renders `n/a` instead.
- **Arcade path anchoring uses parent blueprint's base directory.** When expanding arcade modules, `context_override` path values (e.g. `src_path: data/input/sales.csv`) were anchored relative to the arcade file's directory (`arcades/`) instead of the parent blueprint's directory, producing nonexistent paths like `arcades/data/input/sales.csv`. The expander now parses sub-blueprints via `parse_dict(raw, base_dir=base_dir, …)` with the parent's `base_dir`, so context_override paths resolve correctly.


## [1.2.0] — 2026-06-06

### Added
- **Verbose healing flow logging.** Structured `INFO`-level log lines at each healing step (attempt header, LLM token/latency, parse result with op summary, validation/apply status, final completion summary). Visible with `--verbose` flag (sets `logging.DEBUG` → INFO messages appear). Added to `generate_agent_patch` loop.
- **Model-agnostic positioning in README and specs.md §8.1.** Surfaces Aqueduct's small-model compatibility as a deliberate architectural advantage — constrained PatchSpec grammar (13 ops, no codegen) means even 7B local models heal ~70% of common Spark failures in a single attempt. Larger models unlock `deep_loop` and multi-model cascades for complex cases.
- **Phase 44 — Multi-model healing cascade.** `agent.cascade:` accepts a list of per-tier configs — Aqueduct tries the cheapest model first and escalates to more expensive models on `stuck_signature`, `exhausted_attempts`, or `deferred`. Each tier has its own budget (`max_reprompts`, `max_seconds`) and can override `provider`, `base_url`, `deep_loop`, `allow_defer`. Missing fields inherit from top-level `agent.*` defaults. New `generate_cascade_patch()` orchestrator in `aqueduct/agent/cascade.py`. `model_cascade_position` recorded on every `AttemptRecord`. No `carry_history` — each tier works independently with a clean failure context.
- **Phase 43 — In-conversation sandbox feedback (`deep_loop`).** When `agent.deep_loop: true`, sandbox/lineage/explain gates run inside the LLM conversation via a `validate_callback` injected into `generate_agent_patch`. If the patch fails validation, the rejection feedback is injected as a user message and the model retries immediately — same conversation, learned context. Once validation passes, `apply_callback` runs guardrails + apply as before. Default false preserves current post-hoc gate behavior. SparkSession is reused across reprompts inside the callback.
- **Phase 42 — `set_spark_config` PatchSpec operation.** New op targets the Blueprint `spark_config:` block — seven of the 20 most common Spark errors (OOM, container kills, shuffle fetch failures, Kryo buffer overflow, dynamic allocation thrashing, GC/heartbeat issues, driver MaxResultSize) become healable with a single operation. Auto-creates `spark_config` if absent. Recommended default: add to `guardrails.forbidden_ops` to require human review before auto-apply (spark config changes affect cluster resources).
- **Phase 41 — `op: defer_to_human` with opt-in `allow_defer`.** New PatchSpec operation that signals an unhealable failure (infrastructure, upstream schema change, UDF bug) instead of hallucinating a patch. Opt-in via Blueprint-level `agent.allow_defer: true` — when false (default), `defer_to_human` is hidden from the LLM prompt and rejected if somehow produced. When allowed, the loop terminates with `stop_reason='deferred'` and the full diagnosis (`diagnosis`, `suggestions`, `confidence_reason`) is staged for human review. Scenario grader supports `allow_defer` gating assertion. Bonus: `never_heal_errors` patterns upgraded from exact match to regex (`re.search`) so patterns like `"IllegalStateException.*offsets"` catch error-class variants. Prompt wording simplified: example now uses `description` instead of `rationale` (aliases handle normalization transparently).
- **Phase 40 — Mid-call budget enforcement.** `BudgetTracker.max_seconds` is now enforced during LLM calls, not just at iteration boundaries. The orchestration loop computes a per-call HTTP deadline (`min(agent.timeout, remaining_seconds)`) and threads it through `_call_agent` → provider functions as the httpx timeout. When the deadline fires mid-call, `httpx.TimeoutException` is caught and distinguished from a generic API error: if `deadline < agent.timeout` (budget was the binding constraint), the loop terminates with `stop_reason='budget_seconds_exceeded'`. Pre-call budget exhaustion records a zero-token attempt with `gate_that_rejected='budget'`. New methods: `BudgetTracker.remaining_seconds()`, `BudgetTracker.mark_budget_seconds_exceeded()`.
- **Source Code Navigation Map to AGENTS.md.** Documents internal module structure of `aqueduct/agent/`, `aqueduct/executor/spark/`, and `aqueduct/parser/`, updated alongside package refactors. Acts as a first-filter for grepping — each package table shows which module owns what, with "when adding a feature" guidance per package.
- **Parser `UdfSchema` duplicate `model_config` removed.** `aqueduct/parser/schema.py` had two `model_config` lines in `UdfSchema`; the first was overwritten by the second and dead. Consolidated to a single `ConfigDict(extra="forbid", populate_by_name=True)`.
- **Compiler source map to AGENTS.md.** `aqueduct/compiler/` now documented with module roles and ownership.
- **`compiler.py` and `expander.py` warning logs added.** Silent `except: pass` on YAML provenance load failures replaced with `logger.warning()` — same pattern as the agent/ fixes.
- **Feature-split CI with path-based auto-skip.** CI now runs 9 parallel jobs (parser, compiler, executor, surveyor, agent, patch, CLI, config, stores) instead of a monolithic quick-gate. A `changes` job detects which areas were touched; on branches only matching jobs fire. On `main` every job runs unconditionally. Spark tests are no longer silently deselected — they run in a dedicated executor job.
- **Compatibility matrix CI workflow.** New `.github/workflows/compatibility.yml` tests 3 curated version combos (Latest: Python 3.13 + PySpark 4.1.2 + Postgres 18; LTS: Python 3.11 + Spark 4.1.2 + Postgres 17; Legacy: Python 3.12 + Spark 3.5.8 + Postgres 17) on every main merge. Results are auto-pushed back to `docs/compatibility.md` with per-combo pass/fail status and build reference.
- **Lazy pyspark import in `aqueduct/executor/__init__.py`.** Replaced the eager module-level `from aqueduct.executor.spark.executor import execute` with a `__getattr__` lazy resolver. This prevents ImportError crashes when any executor submodule (path_keys, models) is imported without Spark installed — the parser, surveyor, and patch modules all import from executor and previously failed at collection time in non-Spark CI jobs.

### Changed
- **`aqueduct/agent/__init__.py` split into 5 focused modules.** The 1679-line file was restructured into: `prompts.py` (templates + prompt builders), `providers.py` (HTTP dispatch), `parse.py` (response parsing + reprompt formatting), `loop.py` (orchestration loop + patch I/O), and a thin `__init__.py` re-exporting the public API. Internal `_call_agent` collapsed 11 positional params into a `_ProviderConfig` dataclass. All external imports (`from aqueduct.agent import X`) unchanged.
- **`build_prompt()` now forwards `last_apply_error`.** Debug prompt view matches what the model sees on re-prompt turns.
- **`apply_callback` and `on_attempt` parameters typed** with `Callable` instead of `Any`.
- **`_patch_filename` dropped unused `patches_dir` param.** Cleaner signature.
- **`on_patch_pending_webhook` typed** as `WebhookEndpointConfig | None` (was bare `None` default in comment only).
- **`patches/rules.md` content capped at 4096 chars.** Prevents context overflow from an accidentally-large rules file.
- **`_load_previous_patches` uses `os.scandir` instead of `glob`.** More efficient on directories with thousands of applied patches.
- **Warning logs added** for silent JSON parse failures in `_build_provenance_section` and `_build_user_prompt` (previously returned empty defaults silently).
- **Webhook fire failure now logged at DEBUG** instead of bare `except: pass`.
- **CLAUDE.md agent references updated.** LLM provider addition instructions point to `providers.py` (was `__init__.py`); `PROMPT_VERSION` bump policy points to `loop.py` (was `__init__.py`).

### Documentation
- **Docs lineage references updated.** `docs/observability_guide.md`, `docs/specs.md`, and `docs/production_guide.md` cleaned of stale `lineage.db` references — the lineage store was merged into `observability.db` (1.1.2). Blob externalisation documented in filesystem layout. `stores.lineage` marked as inert with deprecation note in all three docs.

### Fixed
- **Lineage skipped on the surveyor-less `execute()` path.** The post-run lineage write in `executor/spark/executor.py` passed the raw `observability_store` (often `None` when only `store_dir` is supplied), so `write_lineage()` silently no-op'd and no `column_lineage` rows were recorded. It now resolves the store via `_resolve_observability_store(store_dir, observability_store)`, matching every sibling metric writer. Real `aqueduct run` was unaffected (a Surveyor supplies the store); the gap only hit programmatic `execute(..., store_dir=...)` callers.

### Changed
- **`aqueduct/doctor.py` split into the `aqueduct/doctor/` package.** Leaf connectivity checks (config, depot, observability, webhook, agent, secrets, aqtest/aqscenario) moved to `doctor/checks_io.py`; the spark / network / blueprint-source checks and `run_doctor` stay in `doctor/__init__.py` (they call each other by bare name and are monkeypatched via `aqueduct.doctor.<name>`); `CheckResult`/`_ms` live in `doctor/base.py`. All public names re-export from `__init__` — `from aqueduct.doctor import …` and `aqueduct.doctor.<name>` patch targets are unchanged. No behaviour change.

## [1.1.2] - 2026-05-30

### Added
- **Numeric field bounds on all config/schema models.** Every `int`/`float` field in `parser/schema.py` and `config.py` now carries pydantic `Field(ge=…)` / `Field(gt=…)` / `Field(le=…)` bounds. Malformed blueprints (`max_attempts: 0`, `confidence_threshold: 1.5`, `max_sample_rows: -1`) fail `aqueduct validate` with one error per bad field.
- **`scripts/run_snippets.sh`** — automated gallery snippet runner. Copies all snippets to an isolated workspace, creates a shared Python 3.12 venv (reused across runs), and executes each non-live snippet through its full lifecycle: `requirements.txt` install → `populate*.py` → `aqueduct run` → `inspect_results.py`. Prints inline pass/fail per snippet and a final count. Skips snippets requiring live external services (S3, JDBC/Postgres).
- **`gallery/snippets/20_arcade_reusable/populate_data.py`** — creates `data/input/sales.csv` so the Arcade snippet runs without manual setup.
- **`gallery/snippets/03_ingress_delta_incremental/requirements.txt`** — pins compatible `pyspark` and `delta-spark` versions with a Delta ↔ Spark artifact matrix comment.

### Changed
- **Lineage store merged into observability.** `column_lineage` now lives in `observability.db` instead of a separate `lineage.db`. The `stores.lineage` config block is inert — setting a different path emits a `DeprecationWarning`. `write_lineage()` writes through the observability store. `aqueduct lineage` reads from `observability.db`.
- **Blob externalisation for fat observability columns.** `manifest_json`, `provenance_json`, and `stack_trace` are now Zstandard-compressed `.json.zst` blobs stored under `.aqueduct/observability/<bp>/blobs/<run_id>/`. DuckDB rows store only the relative path — row width drops ~10×. Existing inline data continues to work transparently via `blob_store.materialize()`. New dependency: `zstandard>=0.22.0`.
- **Delta artifact for snippet 03** corrected to `delta-spark_4.0_2.13:4.2.0` (was `4.1` artifact, incompatible with PySpark 4.0).

### Fixed
- **Guard null LLM content in both providers.** `_call_anthropic` and `_call_openai_compat` now raise `ValueError` on null/empty content blocks instead of crashing with `'NoneType' object has no attribute 'strip'`.
- **Unwrap single-key PatchSpec wrappers.** `_parse_patch_spec` unwraps `{"patch": {…}}` envelopes before validation, saving a reprompt round for models that get content right but nesting wrong.
- **`test_heal_spend_cap_skipped_when_none`** — mock returned stale `AgentResult` from non-existent `aqueduct.agent.agent` module; fixed to return `AgentPatchResult` from `aqueduct.agent`.
- **`test_run_patch_gates_inline_preflight_and_sample`** — `assert_called_with` included phantom `lineage_store` arg that `run_sandbox_gate` never accepted; removed.
- **`json_repair` ImportError in reprompt loop.** On `json.JSONDecodeError`, the `except ImportError: raise` re-raised the wrong exception type; now re-raises the original `JSONDecodeError`.
- **CI quick-gate** now installs `[llm,redis]` extras so `json_repair` and Redis-backed config tests are available without integration markers.

## [1.1.1] - 2026-05-29

### Added
- **`aqueduct completion {bash|zsh|fish}`** subcommand emits a click-generated shell-completion script. Auto-tracks the command tree — new subcommands and flags pick up without maintenance. Install: pipe the output into your shell's completion dir.

- **`--log-format json` propagates caller-supplied `extra={...}` fields.** The previously-shipped JSON formatter only emitted `ts` / `level` / `logger` / `msg`. It now merges every non-stdlib LogRecord attribute (`run_id`, `blueprint_id`, `module_id`, anything else passed via `logger.warning(..., extra={...})`) into the JSON payload, so Loki / Splunk / Datadog can filter by domain key without grokking text.

- **Benchmark UX overhaul.** Serial `aqueduct benchmark` runs now print a per-pair separator block (header bar auto-sized to longest line, scenario + model name, one-line verdict with `PASS · conf · diag · duration`). Iteration order switched to model-outer / scenario-inner so Ollama keeps each model loaded across its full scenario sweep, with a one-shot `↻ switching models` hint when the model changes. Final results table redesigned with box-drawing `═`/`│`/`─` separators, `·` middle-dot subfield separators, and per-column widths fitted to content. `--format table` mirrors to stderr when stdout is redirected so `> Log.txt` shows the table in both terminal and file. Top-of-run banner: `[benchmark] N scenarios × M models = X pairs`.

- **Interrupt handling for benchmark + healing.** `KeyboardInterrupt` around `run_benchmark` prints a one-line notice and exits 130 (SIGINT convention); completed pairs already in `benchmark.duckdb` survive. Both LLM providers switched from one-shot `httpx.post(...)` to `with httpx.Client():` context managers so the socket is torn down on Ctrl+C, propagating disconnect to the server (Ollama / vLLM abort generation, frees GPU; Anthropic stops billing mid-completion).

- **`PatchSpec.misc: dict[str, Any]`** field — bucket for unknown top-level keys. `extra="forbid"` relaxed to `extra="allow"` with a pre-validator that moves unrecognised top-level keys into `misc` for post-mortem visibility. Operation-level fields stay strict (`extra="forbid"` on each Op model) — a typo in `key:` or `module_id:` still bounces the patch because it would mutate the wrong field. Metadata fields are descriptive, not load-bearing.

- **`_METADATA_ALIASES`** maps common casing/synonym variants to canonical field names: `rootCause` / `rootcause` / `cause` → `root_cause`, `reasoning` / `reason` / `explanation` → `rationale`, `patchId` → `patch_id`, etc. Normalised at parse-time before pydantic validation. Eliminates the `"rootCause" is not a recognized field — remove it.` reprompt loop seen with smaller models.

- **`AgentPatchResult.recovery_applied: list[str]`** records every mechanical recovery `_parse_patch_spec` performed on the raw LLM output (json_repair fallback, etc.). Empty when the response was clean.

- **Recovered patches forfeit auto-apply privilege.** When `agent_result.recovery_applied` is non-empty and `approval_mode` is `auto`/`aggressive`, the CLI downgrades to `human` for that single patch and prints a one-line notice listing the recoveries. Trust boundary moves to the reviewer, not the regex — a patch we had to rescue is inherently lower-trust even if it parsed in the end.

- **Fuzzy module-ID hints in apply-gate errors.** `_find_module` now ranks available IDs by similarity (stdlib `difflib`) and leads with the closest match in the reprompt. `Module 'clean_events_format' not found. Closest match: 'clean_events'. Available: [...]`. Reprompt-side nudge only — never auto-substitute the suggested ID, since that would be interpretive recovery (hallucinating model intent).

- **Top-level shape rescue in the reprompt loop.** `_detect_structural_error` now catches four common small-model shape failures instead of one: (1) top-level JSON array (model emitted operations directly with no envelope), (2) top-level scalar (model returned prose-as-JSON or a single value), (3) wrapper forgotten with op-fields at root (existing case), (4) envelope missing entirely with no op-field signal. All four routes hit the escalated template with the skeleton + a targeted hint, replacing Pydantic's opaque `Input should be a valid dictionary` text. The escalated template additionally shows a short (≤300 char) snippet of what the model actually sent under a `DO NOT edit this — rewrite from the skeleton above` label, so small models that lose track of their own output across turns have evidence without being biased toward small edits. Observed pattern: qwen 2.5-coder 7B-q8 returning operations arrays directly and looping on `stuck_signature` without ever recovering.

- **Grammar-constrained JSON sampling for OpenAI-compat providers.** `_call_openai_compat` now sends `response_format: {"type": "json_object"}` by default. Ollama (0.1.24+), vLLM, LM Studio, and most OpenAI-compat servers honour this — the model is masked at sampling time to emit only valid JSON, eliminating malformed-JSON failure modes that no amount of post-processing can recover (unescaped `"` inside string values, truncated objects, smart quotes). Opt out per-call via `provider_options.response_format = "off"` for backends that reject the field. Anthropic provider unchanged — structured output there is forced-tool-use, a separate change.

- **Optional `json-repair` last-ditch parse pass** via new `[llm]` extra (`pip install aqueduct-core[llm]`). When strict `json.loads` + the built-in cleanups still fail, `_parse_patch_spec` tries `repair_json()` before surfacing the error. Without the extra installed, behaviour is unchanged. Bundled under `[all]`.

- **New documentation surface:**
  - `docs/production_guide.md` — cluster deployment, env config, danger settings, Delta operational notes, production patch lifecycle, security, readiness checklist (lifted from former specs.md §14).
  - `docs/roadmap.md` — deferred / aspirational items: streaming, resume-from semantics, MCP, ML inference, Flink, multi-pipeline orchestration (lifted from former specs.md §8.8 / §12).
  - `docs/compatibility.md` — Python × Spark support matrix, `pyspark>=4.0,<5.0` pin rationale, cloudpickle 3.0 requirement on Python 3.13, production pinning recipe.
  - `blueprints/hello.yml` — matches the README's Getting Started example so `aqueduct doctor blueprints/hello.yml` works as documented out of the box.

### Changed
- **Phase 36 Part B — schema-driven path anchoring.** Two coupled changes kill the hand-maintained "these keys are paths" lists. **New module `aqueduct/executor/path_keys.py`** carries a declarative per-type registry (`Ingress`/`Egress`/`UDF` audited; unregistered types fall back to the pre-Phase-36 blanket tuple for backward compatibility). Parser's `_anchor_paths` consults `get_path_keys(module_type)` instead of the hardcoded `("path", "data_dir", "input_dir", "output_dir", "jar")` tuple — Ingress no longer anchors `output_dir`, Egress no longer anchors `data_dir`, etc. Adding a new module type with path-typed config keys is now one edit at `executor/path_keys.py`, no parser touch. **New marker `aqueduct/parser/fs_path.py::FsPath`** (frozen dataclass with `allow_uri: bool = True` policy slot) annotates path-typed pydantic str fields. `RelationalStoreConfig.path` and `KVStoreConfig.path` use `Annotated[str, FsPath()]`; `config.py::_anchor_fs_path_fields_under_stores` walks `StoresConfig.model_fields` then each store sub-model's `model_fields.metadata`, anchoring any FsPath-marked string field. Replaces the hardcoded `data["stores"][name]["path"]` loop; adding a new path field anywhere under `stores.*` is now one edit at the schema site. Rejected alternatives: register-on-import for path keys (would force parser to import every executor handler, breaks 4-layer rule); bare `class FsPath` marker (locks call sites if policy fields ever land — dataclass-with-empty-body costs one `()` now but stays migration-free later).

- **Phase 36 Part A — `parse_dict(raw, base_dir, profile, cli_overrides) -> Blueprint`** is now the canonical entrypoint for in-memory Blueprint validation; `parse(path)` is a thin wrapper that loads YAML and delegates with `base_dir=path.parent`. Three in-memory patch flows (`cli._apply_patch_in_memory`, `patch/preview.run_sandbox_gate`, `surveyor/scenario._try_apply_patch`) previously round-tripped through `tempfile.NamedTemporaryFile` to feed the old file-only parser; that detour broke 1.1.0 path anchoring whenever the tempfile landed in `/tmp` (relative paths like `../data/events.csv` resolved against `/tmp` → absurd `/data/events.csv`). All three sites now call `parse_dict()` directly with the original Blueprint's parent as `base_dir`. `tempfile` import dropped from `cli.py`.

- **External audit response pass.** Strip every `# Phase NN` scaffolding comment from production code (26 hits across parser / executor / surveyor / cli / templates / tests). Phase references stay in `CHANGELOG.md` and `TODOs.md` per CLAUDE.md; source comments now describe the *what* and *why* instead of the project-management label. Drop the dead Flink `NotImplementedError` stub from `aqueduct/executor/__init__.py` — Flink as an aspiration stays in `docs/roadmap.md` + `TODOs.md` Deferred block.

- **`docs/specs.md` split into specialized guides.** The engine reference shrank from ~2,750 lines as production, CLI-duplicate, deferred-items, and roadmap content moved into the new dedicated docs above. Architecture corrected 5-layer → 4-layer — the phantom `Planner` layer never had a corresponding `planner.py` / `Planner` class / `JobSpec` type; planning (topo sort, Probe insertion, parallel-component detection) is documented as a sub-step inside the Executor. Phase-NN refs and "V1 behaviour" / "post-MVP" language stripped. `§8.5 Patch Grammar` gains a "Metadata field tolerance" subsection covering the alias table + `misc` bucket.

- **`docs/cli_reference.md`** §1 documents the new `aqueduct completion {bash|zsh|fish}` subcommand with install snippets for each shell.

- **`README.md` References list** updated to mirror the new doc inventory (production_guide, compatibility, roadmap).

- **`CLAUDE.md` gains a Documentation Map** — table of which doc owns which surface (specs.md is no longer the catch-all). Change-Trigger Matrix routes edits to the right doc (production_guide for cluster/danger, spark_guide for tuning, compatibility for pins, roadmap for deferred items). Stale `surveyor/llm.py` LLM-provider hook corrected to `agent/__init__.py`.

- **`gallery/aqscenarios/README.md`** gains an "Overnight: every scenario × every local model" recipe — tmux + multi-`--model` invocation, monitoring without attaching, post-hoc duckdb query, runtime estimate.

- **Cloudpickle monkeypatch** in `aqueduct/executor/spark/udf.py` — `FIXME: TEMPORARY hack` replaced with a documented compatibility-matrix block. The patch was already version-gated (short-circuits when bundled cloudpickle ≥ system); the comment now explains *why* and *when it self-deprecates*.

- **System prompt hygiene pass** + `PROMPT_VERSION` bumped `1.0` → `1.1`. Merged the "Patch op disambiguation" and "Provenance-driven op selection" sections into one table-driven block; added explicit "if `No context block` notice present, do NOT use `replace_context_value`" rule so the system prompt and per-failure provenance section stop saying opposite things. Benchmark regression diffs going forward attribute to the new prompt version.

### Changed
- **Stripped 81 lines of pre-1.0 DDL migration code from `surveyor.start()`.** All `ALTER TABLE` / `information_schema` probes for `healing_outcomes`, `failure_contexts`, and `run_records` column additions are removed — no users on pre-1.0 database versions, and the fresh-DDL path is canonical. Also eliminates a data-loss risk: the `healing_outcomes.id` INTEGER→VARCHAR migration used `DROP COLUMN` + `ADD COLUMN` instead of `ALTER COLUMN TYPE`.

### Fixed
- **`_write_merge` guards `spark.catalog.dropTempView` with try/except.** On first merge the temp view may not exist, and `dropTempView` raises `AnalysisException`. Now silently skipped — view is created fresh on the next line regardless.

- **`@aq.depot.get()` emits a log warning when no depot backend is configured.** Previously returned the default (`""`) silently; incremental pipelines without a depot would re-read all source data every run with no diagnostic. The warning surfaces during compilation so misconfiguration is visible before Spark starts.

- **Removed `"root cause"` (space-containing) alias from `_METADATA_ALIASES`.** JSON keys with literal spaces are vanishingly rare from LLM output and the alias misleadingly suggested the normalizer handles free-form prose. All other casing/synonym aliases remain.

- **`docs/specs.md` §5.3 — four missing `@aq.*` functions documented.** `@aq.date.offset`, `@aq.date.month_start`, `@aq.date.format`, and `@aq.runtime.prev_run_id` were implemented but absent from the function reference table. Also corrected `@aq.env` signature: the implementation does not accept a `default` parameter (unlike `${VAR:-default}` in Tier 0).

- **`CLAUDE.md` gains three Change-Trigger Matrix rows** for new `@aq.*` functions, new path-key entries, and new exit codes. New `## Common Pitfalls` section captured five recurring patterns from the audit: silent depot_get, dropTempView guard, parallel frame-store scoping, Probe attach_to, and immutable dataclass mutation. **Testing workflow section expanded** with guidance on when and how to record items in `TEST_MANIFEST.md` (regression tests for bug fixes, `⏳` convention, never self-mark `✅`).

- **`tmp/.prompts/DO_TESTING.md` rewritten** with project context header, 4-layer architecture summary, source-file map by layer, test-file organization table, and critical rules from CLAUDE.md — so models with no prior project knowledge can work immediately without a full codebase read.

- **`tests/TEST_MANIFEST.md` — 17 new ⏳ items** covering gaps from the test audit: `--from`/`--to` selector coverage (5), Delta merge edge cases (2), `metrics_boundary` Channel config (2), `danger.*` settings gate enforcement (4), end-to-end heal flow (1), plus 3 PatchSpec resilience items bridging the original 12 to 15.

- **`missing field` reprompt now echoes the op block the model emitted.**
  `operations[0].set_module_config_key.value: required field missing` used
  to be opaque to tiny models — they could not see which keys they had
  already written. The reprompt now appends `You emitted 'op', 'module_id',
  'key' — add the missing "value" field to this same op block.` Walks the
  pydantic error `loc` into the parsed dict to find the partial op without
  echoing arbitrary content.

- **`replace_context_value` on blueprints without a `context:` block.** The
  LLM was pushed toward `replace_context_value` for path fixes even when the
  blueprint declared no `context:` block; the apply gate then rejected with
  `"Blueprint has no 'context' block."` and the LLM repeated the same op on
  reprompt, burning the heal-attempt budget. Two changes: (1) failure prompt
  now emits an explicit "No `context:` block" section when `prov.context` is
  empty, telling the model to use `set_module_config_key` instead; (2) the
  apply-gate error message names the correct alternative op with a concrete
  example, so apply-callback reprompts steer the model toward the literal
  fix.

## [1.1.0] — 2026-05-28

### Added
- **`stores` aggregate extra** in `pyproject.toml` — `pip install aqueduct-core[stores]` pulls Postgres + Redis backends in one shot, mirroring the existing `secrets` / `schedulers` umbrella pattern. Individual `postgres` / `redis` extras still available. `[all]` now references `[stores]` instead of listing each backend.

- **Unified reprompt loop with multi-axis budget** (Phase 34). The agent's
  two disjoint loops (inner `max_reprompts` and outer aggressive retry) are
  collapsed into a single `generate_agent_patch` loop driven by a stateful
  `BudgetTracker`. The same loop now catches schema/JSON parse failures AND
  apply-time gate rejections (guardrail violation, parse/compile failure on
  the patched blueprint) when callers pass an `apply_callback`. Apply
  rejections feed back into the next reprompt instead of being silently
  dropped in `human` / `auto` / `ci` modes — previously a one-shot wasted
  call.
- **`BudgetConfig` (six axes) + `BudgetTracker`** in `aqueduct.agent.budget`:
  `max_reprompts` (default 5), `max_seconds` (120s), `max_tokens_total`
  (50k), `same_error_consecutive` (2), `same_signature_overall` (3),
  `progress_stalled_window` (3). First axis to trip terminates the loop and
  records the `stop_reason` (`solved`, `exhausted_attempts`,
  `budget_seconds_exceeded`, `budget_tokens_exceeded`, `stuck_signature`,
  `progress_stalled`, `api_error`). Exposed in `aqueduct.yml` as
  `agent.budget:` (frozen pydantic model; same instance shared between
  production heal and benchmark — divergence would silently invalidate the
  leaderboard).
- **Error-signature engine** (`aqueduct.agent.signature`). Stable
  16-char sha1 over `(error_class, where, normalized_message)`. Normalizer
  collapses whitespace, lowercases, strips ANSI, replaces digits with `N`,
  quoted strings with `"X"`, and `/path/like/things` with `/PATH` — two
  failures that differ only in volatile bits hash identically. Six factory
  helpers cover Pydantic `ValidationError`, JSON parse failures, generic
  exceptions, apply-gate rejections, and plain reprompt text. The same
  hashing layer is reused by Phase 33 Part C (coaching loop, post-Phase-34).
- **Stuck-detection escalation BEFORE abort** (Phase 34 #6). When
  `same_error_consecutive` trips (2 identical signatures in a row), the
  loop does NOT immediately abort. Instead it bumps sampling temperature to
  0.8 AND swaps the reprompt template to the skeleton-anchored variant for
  one more attempt. If that attempt also produces the same signature the
  loop terminates with `stuck_signature`. Costs one extra attempt over
  naive abort and recovers most cases where the model would have escaped
  with a different seed.
- **Skeleton-anchored reprompt template** (Phase 34 #5). When the validator
  flags a structural error (top-level `operations` missing AND op-level
  fields at root) OR when escalation triggers, the reprompt shows a minimal
  valid PatchSpec skeleton + a one-line "you forgot the `operations[]`
  wrapper" hint and stops echoing the bad output — which biases small
  models toward small edits of their mistake. Cleaner correction signal
  measurably improves recovery on small-model stuck-signature cases.
- **`heal_attempts` table** in `observability.db` (Phase 34 #3). One row
  per LLM turn (success or failure) — `attempt_num`, `signature_hash`,
  `error_class`, `where_field`, `normalized_message`, `tokens_in/out`,
  `latency_ms`, `gate_that_rejected` (`schema` / `apply` / `provider` /
  None), `escalated`, `stop_reason`, `prompt_version`. Wired through the
  unified loop's `on_attempt` hook so `aqueduct run` populates it
  automatically. Post-mortem can now answer "what did attempt 2 say" —
  which `healing_outcomes` alone could not.
- **`benchmark_results` gains `stop_reason`, `escalated`,
  `tokens_in_total`, `tokens_out_total`** (Phase 34 #7 — benchmark =
  production parity). Idempotent additive ALTER for pre-existing stores so
  pre-1.0.4 benchmark histories survive intact. The leaderboard can now
  distinguish "model gave up" (`stuck_signature`) from "ran out of attempts"
  (`exhausted_attempts`).
- **`AgentPatchResult`** gains `stop_reason`, `tokens_in_total`,
  `tokens_out_total`, `attempt_records`, `escalated` fields. All default
  to safe values so older callers and test fixtures keep working
  unchanged.

- **Structured Spark error extraction** (Phase 35). Surveyor now calls
  `_extract_structured_error(exc)` before stringifying the failure: pulls
  Spark 4.0 `PySparkException.getCondition()` / `getErrorClass()` +
  `getMessageParameters()` + `getSqlState()`, walks `Py4JJavaError.java_exception.getCause()`
  for the innermost JVM throwable (capped at 10 hops), and falls back to the
  Python cause chain. Extraction is lazy-imported and best-effort — any
  failure returns None so a buggy extractor cannot block self-heal.
- **`FailureContext` gains optional fields** `error_class`, `root_exception`
  (`{type, message}`), `sql_state`, `suggested_columns` (parsed from
  `UNRESOLVED_COLUMN.WITH_SUGGESTION` proposal lists), and `object_name`.
  All default to None / empty tuple; legacy callers see no behaviour change.
  Idempotent ALTER on `observability.db.failure_contexts` upgrades existing
  stores in place.
- **Conditional prompt rendering**: structured fields replace the raw stack
  trace block when extraction succeeded, producing a focused "Root cause
  (structured)" section with the offending column name + actual columns
  available. The full trace is shown only when extraction returned None.
  Deleted `_truncate_stack` + `_STACK_TRACE_MAX_LINES` — Phase 35 makes the
  arbitrary line-count cutoff unnecessary.
- **`inject_failure.structured:` block** in `.aqscenario.yml` lets
  benchmark scenarios carry the same structured fields production extracts
  from live exceptions, so the leaderboard exercises the identical prompt
  branch. `gallery/aqscenarios/01_schema_drift_column_rename.aqscenario.yml`
  migrated as worked example. Legacy scenarios with no block fall through
  to the stack-trace path.

### Changed
- **Provider calls return token usage.** `_call_anthropic` and
  `_call_openai_compat` now return `(text, tokens_in, tokens_out)` tuples;
  budget axes can therefore enforce real cost ceilings instead of just
  attempt counts. Anthropic uses `usage.input_tokens`/`output_tokens`;
  OpenAI-compatible endpoints use `usage.prompt_tokens`/`completion_tokens`.
  Zero when the provider does not report usage.
- **`aqueduct benchmark` shares the same `BudgetConfig` as production
  heal** — read from `agent.budget:` in `aqueduct.yml`. Apply-gate
  rejections (guardrail violation, compile failure on the patched
  blueprint) now feed back into the same reprompt loop benchmark and
  production use, closing the "leaderboard cheats" path where the
  benchmark would silently pass on a patch production would reject.

### Documentation
- New `docs/QUERIES.md` — diagnostic SQL cookbook against the observability,
  lineage, depot, and benchmark stores. Recipes organised by use case
  (run post-mortem, token spend, stuck-signature detection, leaderboard,
  regression diff, cross-pipeline aggregates) with reading guides for each
  output. Cross-linked from `ALL_TABLES.md`.
- `ALL_TABLES.md` filesystem-layout block updated for per-pipeline routing
  (`.aqueduct/observability/<blueprint_id>/{observability,lineage}.db`);
  added the aggressive `run_id` vs `parent_run_id` semantics note that
  every multi-table join needs.

### Changed
- **`approval_mode: aggressive` collapsed into `auto` + `max_patches`.** The
  two near-duplicate self-heal modes shared a 90+ LOC code path differing only
  in a danger gate, an explain-gate strictness flag, and the default patch
  count. `approval_mode: auto` is now the single auto-apply mode with a new
  `max_patches: int = 1` knob (default 1 = single-shot, the historical `auto`
  behaviour). Setting `max_patches > 1` opts into the multi-patch reprompt
  loop and requires `danger.allow_multi_patch: true` (alias:
  `allow_aggressive_patching`). The explain gate's hard-block on regression
  now fires whenever `max_patches > 1`, not based on the mode name.

  Backwards compatibility: `approval_mode: aggressive` keeps parsing but
  emits a `[deprecated]` warning at parse time and is normalised to `auto`
  internally. `aggressive_max_patches` keeps parsing as a `validation_alias`
  on `max_patches` and emits the same warning. `danger.allow_aggressive_patching`
  is a `validation_alias` on `danger.allow_multi_patch`. CLI flag
  `--allow-aggressive` is kept as a secondary form of `--allow-multi-patch`.
  All four deprecated names are slated for removal in `aqueduct: "2.0"` schema.
  Default `max_patches` change (5 → 1) flips behaviour for blueprints that
  relied on the previous default; explicit values are unaffected.

### Added
- `PatchSpec._normalize_op_aliases` now auto-derives a `patch_id` slug from
  the rationale (or a short uuid fallback) when the LLM omits the field. Also
  silently strips common hallucinated meta fields (`id`, `name`, `applied_by`,
  `datetime_applied`, `timestamp`, `author`, `version`, `created_at`,
  `updated_at`) instead of bouncing the whole patch via `extra="forbid"`.
  Saves a reprompt round-trip per missing field.
- Regulator modules now accept a `config.poll_seconds: float = 30.0` knob
  controlling the cadence of the gate-poll loop while `timeout_seconds > 0`.
  Replaces a hardcoded 2-second poll interval that wasted driver CPU and
  observability-DB reads on batch-tier signals. Clamped to a minimum of 0.5s.
  Tune lower for interactive use, higher for hour-scale external triggers.
- `agent.sandbox_mode: sample | preflight | off` (blueprint + engine config).
  Controls patch sandbox replay fidelity. `sample` (default) keeps existing
  1000-row replay; `preflight` runs full dataset (no Egress) and requires
  `danger.allow_full_preflight: true`; `off` skips the gate entirely and
  requires `danger.allow_skip_sandbox: true`. Engine prints a startup warning
  for `preflight` / `off` and a `⚠ DANGER COMBO` line when
  `sandbox_mode=off` + `approval_mode=aggressive` are both set.
- `danger.allow_full_preflight` and `danger.allow_skip_sandbox` config flags.

### Fixed
- **`_apply_patch_in_memory` tempfile path anchoring.** Same class of bug as
  `run_sandbox_gate`: the patched Blueprint was written to a `/tmp/...`
  tempfile, so the 1.1.0 path-anchoring rule resolved any `../data/...`
  relative path against `/tmp/`, producing absurd values like
  `/data/input/events.csv`. The tempfile is now created in the same
  directory as the original blueprint (`dir=blueprint_path.parent`).
- **Failed-patch staging message** now reflects the actual on-disk filename
  (`{YYYYMMDDTHHmmss}_{patch_id}.json`) instead of the bare `patch_id.json`,
  matching the timestamp-prefixed name that `_patch_filename` writes.
- **Apply-callback now compile-checks the patched Blueprint** and rejects
  patches that drop required discriminator keys (Channel `op`, Ingress /
  Egress `format`) BEFORE sandbox replay burns 30+ seconds proving the same
  thing. Caught a class of LLM mistakes where `replace_module_config` was
  used for a single-field fix and silently dropped `op: sql` from a Channel
  module. The rejection reason ("Patch leaves Channel module 'X' without
  required 'op' key in config. Use set_module_config_key to update one key
  instead of replace_module_config.") feeds back to the LLM as concrete
  reprompt context in the multi-patch loop.
- **Sandbox replay no longer skips upstream of the failed module.** `run_sandbox_gate`
  invoked `execute(..., from_module=failed_module)`, which made the executor
  skip everything upstream — so the failed module saw `frame_store[upstream]
  = None` and reported `"produced no DataFrame"` even when the patch was
  correct. The full DAG now runs in sandbox; `sample_rows` wrapping keeps
  replay cheap enough that the prior partial-run optimisation isn't worth
  the false-negative rate. Caller-supplied `failed_module` is still accepted
  for back-compat; it's no longer used to slice the run.
- **Sandbox replay path anchoring.** `run_sandbox_gate` wrote the patched
  Blueprint to `tempfile.NamedTemporaryFile(...)` in `/tmp/`, then re-parsed
  it; under the 1.1.0 path-anchoring rule every relative module `path:`
  resolved to `/tmp/...` and the sandbox failed with PATH_NOT_FOUND even when
  the patch itself was correct. The tempfile is now created in the same
  directory as the original blueprint (`dir=blueprint_path.parent`), so
  relative paths resolve to the real data files. Falls back to default
  `/tmp/` only when no original blueprint path is available.
- **Path resolution.** Every relative path inside a YAML file now resolves to
  that YAML's parent directory, not the CWD of `aqueduct run`. Affects
  `module.config.path`, `module.config.data_dir`, `input_dir`, `output_dir`,
  `jar`, and `stores.*.path`. URI-style values (`s3://`, `postgresql://`,
  etc.) and absolute paths pass through unchanged. The on-disk YAML is never
  rewritten — only the in-memory compiled `Manifest`/config carry absolute
  paths, so LLM context (the raw blueprint dict) is unaffected. Fixes
  sandbox replay's `"events_raw produced no DataFrame"` when the blueprint
  is invoked from a sub-dir. Matches Compose / k8s / Terraform conventions.
- `run_records` now writes one row per aggressive-mode iteration, with
  `parent_run_id` linking back to the user-visible outer `run_id`. Previously
  `Surveyor.record()` issued a plain `UPDATE WHERE run_id = ?` against a row
  that only existed for iteration 0 — iterations 1..N silently no-op'd, so an
  aggressive heal with 3 attempts persisted exactly **one** `run_records`
  row (iteration 0's). Two changes: (1) `run_records` schema grows a
  `parent_run_id VARCHAR` column with idempotent ALTER for pre-1.1.0 DBs;
  (2) `Surveyor.record()` switched from `UPDATE` to `INSERT … ON CONFLICT
  DO UPDATE` so each iteration owns its row; (3) new
  `Surveyor.register_iteration(run_id, parent_run_id)` called by the CLI
  before each non-first `execute()` so the iteration row carries its parent.
  Join all iterations of one heal call:
  `WHERE COALESCE(parent_run_id, run_id) = '<outer>'`.
- `healing_outcomes` no longer silently empty when the unified reprompt loop
  exits with `patch=None` (every attempt rejected by `apply_callback`, or
  budget axis tripped before a valid patch landed). The aggressive-mode loop
  in `aqueduct/cli.py` now synthesises one `healing_outcomes` row per
  `agent_result.attempt_records` entry, with `patch_applied=false`,
  `run_success_after_patch=false`, and `failure_category` derived from the
  attempt's signature. Previously `heal_attempts` had the per-attempt log but
  `healing_outcomes` was blank, so `WHERE parent_run_id=<outer>` returned
  zero rows even after 3+ in-loop rejections. Confirmed against
  `02_guardrail_apply_reject.yml --allow-aggressive` (3 heal_attempts rows,
  0 healing_outcomes rows pre-fix).
- `heal_attempts` no longer double-writes per attempt. The unified loop's
  `on_attempt` hook INSERTs one row per attempt with `stop_reason=NULL`;
  the post-loop terminal update now calls a new
  `Surveyor.update_heal_attempt_stop_reason()` that UPDATEs the same row's
  `stop_reason` instead of INSERTing a duplicate. Affects both `aqueduct run`
  self-heal and `aqueduct heal <run_id>` heal-from-store.
- `healing_outcomes` gained a `parent_run_id` column so cross-iteration
  aggressive runs can be queried by the user-visible outer `run_id`.
  In aggressive mode the existing `run_id` column held a per-iteration uuid
  starting at iteration 2 — joins against `run_records.run_id` returned
  partial results. New column NULL on non-aggressive paths; idempotent
  ALTER added in `Surveyor.start()` so pre-1.1.0 stores upgrade in place.
- Phase 34 apply-gate guardrail check now wired into the production heal
  loops (`aqueduct run` self-heal at `cli.py:1625`, `aqueduct heal <run_id>`
  heal-from-store at `cli.py:3565`). Previously only the benchmark path
  (`scenario.py`) passed `apply_callback=` — production heal exited
  `solved` even when the patch then failed `_check_guardrails`, and the
  outer code silently staged the blocked patch. Rejections now feed back
  as reprompts inside the unified loop with `gate_that_rejected='apply'`,
  matching Phase 34's "unified loop" promise on both paths.
- Shared `_resolve_obs_db(cfg, store_dir, run_id)` helper in `cli.py` —
  every read-side command (`runs`, `report`, `lineage`, `heal`) used to
  reinvent observability path resolution with a naive
  `Path(cfg.stores.observability.path).parent`, which never worked when
  the user kept the default and per-pipeline routing put the file at
  `.aqueduct/observability/<blueprint_id>/observability.db`. New helper
  honours `--store-dir`, then explicit `aqueduct.yml` path, then walks the
  per-pipeline dirs to find which DB carries the requested `run_id`,
  falling back to the legacy shared path. Fixes `aqueduct heal <run_id>`
  failing with `observability.db not found at .aqueduct/observability.db`
  on a perfectly valid run_id, plus `aqueduct runs` list mode now unions
  across per-pipeline DBs instead of silently showing zero.
- Phase 35 structured-error extractor now fires on the common failure path.
  The Spark executor catches `IngressError` / `ChannelError` / etc. inside
  its module loop and reports via `ModuleResult`, so the live exception
  never escaped to `Surveyor.record(exc=...)` — the extractor silently
  no-op'd and `failure_contexts.error_class / object_name /
  suggested_columns / sql_state / root_exception` stayed NULL on the
  overwhelming majority of real failures. Fix: `ModuleResult` gained an
  optional `exception: BaseException | None` field; the executor's
  centralised `_on_retry_exhausted` helper + Assert error site now populate
  it; `Surveyor.record()` falls back to the first failed module's
  `exception` when its `exc=` kwarg is None. Chain preservation already
  worked end-to-end (`raise IngressError(...) from exc`) — only the
  hand-off was missing.
- `aqueduct run` final status line + `_last_run_id` Depot key + `on_success`
  webhook payload now report the outer (user-visible) `run_id` instead of
  the LAST aggressive iteration's per-iteration uuid. The printed run_id
  now matches `heal_attempts.run_id` and `healing_outcomes.parent_run_id`,
  so the value users copy from stderr can actually retrieve the heal
  history. Also fixed `_run_patch_gates_inline` to accept the renamed
  `iteration_run_id` kwarg (kwarg-name mismatch crashed aggressive mode
  on the first patch).
- Documented that `stop_reason='solved'` describes LLM loop termination
  only (parseable PatchSpec returned), NOT whether the heal actually fixed
  the pipeline. To check the latter, join against
  `healing_outcomes.run_success_after_patch`. Updates to `aqueduct/agent/budget.py`
  docstring, `docs/ALL_TABLES.md`, and `docs/CLI_REFERENCE.md`.

## [1.0.3] — 2026-05-24

### Added
- **Benchmark persistence + regression detection** (Phase 33 Part A).
  Each `(scenario, model)` `ScenarioResult` from `aqueduct benchmark` is
  now persisted to `<scenarios_dir>/.aqueduct/benchmark.duckdb` so prior
  runs are queryable and CI can gate on regressions. Schema includes
  `prompt_version`, `provider`, `base_url`, `failures`, `soft_failures`,
  and the existing pass/quality metrics.
  - `aqueduct benchmark` gains `--no-persist`, `--store-path PATH`, and
    `--gate-on-regression` flags. `--gate-on-regression` runs the diff
    after persistence and exits non-zero if any `(scenario, model)` pair
    shows a regression — drop-in CI hook for "did a prompt edit silently
    break a scenario?"
  - New `aqueduct benchmark-diff` command: reads the store, compares the
    two most recent runs per pair, prints a status table
    (`NEW` / `= same` / `↑ IMPROVE` / `✗ REGRESS`), and exits non-zero
    on any regression. Supports `--scenario`, `--model`, `--store-path`,
    `--format` filters.
  - Regression metrics: `passed`, `patch_valid`, `patch_applies`
    boolean flips, plus `diag_score` drop > 5pp. LLM-self-reported
    `confidence` is deliberately NOT part of the gate — confidence is
    persisted on every row but excluded from the diff (overconfidence
    bias + cross-model incomparability would produce noise, not signal).
  - Baseline selection prefers exact
    `(scenario, model, prompt_version)` triple and falls back to most
    recent regardless of `prompt_version` with a
    `baseline_prompt_mismatch=True` flag — a `PROMPT_VERSION` bump no
    longer masquerades as a regression.
  - Persistence is best-effort: a locked store or missing `duckdb`
    dependency logs a warning and returns 0, never fails the benchmark
    command.
- **`healing_outcomes.prompt_version` column** (Phase 33 Part A).
  Additive in-place ALTER, idempotent on existing DBs (mirrors the
  `id INTEGER → VARCHAR` migration pattern already in
  `surveyor.Surveyor.start()`). `record_healing_outcome()` populates
  the column from `aqueduct.agent.PROMPT_VERSION` by default; explicit
  override honored. Makes the "version ↔ heal outcome" correlation the
  docs claim finally answerable in SQL.
- **`ScenarioResult` carries `prompt_version`, `provider`, `base_url`**.
  Backward-compatible field additions (all default `None`); populated
  in both the early-exit FailureContext-build-failure branch and the
  normal path of `run_scenario`.
- **Guardrail compliance chain** (Phase 33 Part B Scope C). Three coupled
  changes close the gap where `agent.guardrails` was defined per-blueprint
  but never enforced uniformly:
  1. **Step 1 — prompt injection.** A terse imperative `## Guardrails`
     section is appended to the user prompt whenever
     `manifest.agent.guardrails` has any non-empty field
     (`forbidden_ops`, `allowed_paths`, `heal_on_errors`,
     `never_heal_errors`). The model now sees the constraints upfront
     instead of producing a patch production silently rejects. Threaded
     through `generate_agent_patch(..., guardrails=...)` and `build_prompt`.
  2. **Step 2 — scenario enforcement.** `_try_apply_patch` (benchmark
     code path) now invokes `patch.apply._check_guardrails` before the
     dict apply, matching production. Previously benchmark used
     `apply_patch_to_dict` directly and skipped guardrail checks, which
     made the leaderboard over-report PASS vs production reality.
  3. **Step 3 — guardrail-clean rate metric.** `ScenarioResult` and the
     `benchmark_results` table gain a `violated_guardrails` field
     (`None` when scenario blueprint declares no guardrails — excluded
     from the rate; `[]` when defined-and-clean; non-empty when violated).
     A new leaderboard row "Guardrail-clean" reports the rate. Surfaced
     in `benchmark --format json` and persisted with idempotent ALTER
     migration for pre-existing benchmark stores.
- **Effect-based grader** (Phase 33 Part B Scope C). Replaces the old
  `expected_patch.ops` op-name-equality grader (deleted, see Removed)
  with a post-patch effect check: assert the patched blueprint's target
  module config matches a `config_contains` map. SQL-typed fields
  (`query`, `sql`) are compared via sqlglot AST normalization so
  whitespace, quoting, and alias-case differences no longer trip false
  fails.
- **Sample guardrail scenario** at
  `gallery/aqscenarios/06_guardrail_forbidden_op.aqscenario.yml` — same
  column-rename failure as scenario 01, but the blueprint declares
  `forbidden_ops: [replace_module_config]` so the model must produce a
  surgical patch. Exercises all three guardrail-chain steps end to end.

### Changed
- **`agent.timeout` default 120 → 300 seconds**. The previous default
  was hostile to local-model cold-start (model load into VRAM can take
  30-90s before any inference, eating most of the old 120s window).
  300s tolerates cold-start + inference on small/medium local models;
  hosted APIs (Anthropic) typically respond in <30s so the larger
  ceiling does not affect them. Set explicitly to `120` to restore
  pre-1.0.3 behaviour.
- **LLM API failure log now includes an actionable hint** for the two
  most common transient modes:
  - Timeout → suggests `--timeout 600` and shows a concrete pre-warm
    `curl` against the configured `base_url`
  - Connection failure → suggests connectivity checks against the
    configured endpoint
- **Migrated all gallery scenarios** to the new `expected_patch.effect:`
  syntax. Each scenario's `config_contains` specifies the post-patch
  value the fix must land — accepts any valid op (set_module_config_key,
  replace_module_config, etc.) that reaches the same end state. Scenario
  05 (type_string_vs_numeric) has multiple valid fixes; its
  `expected_patch` is intentionally empty and gates on `patch_applies` +
  `root_cause_contains` only.
- **`ScenarioResult` carries `violated_guardrails: list[str] | None`**.
  Backward-compatible field addition (default `None`).

### Removed
- **`expected_patch.ops:` / `expected_patch.forbidden_ops:` scenario
  syntax**. Op-name equality grading produced false negatives on capable
  models that chose a valid alternative op (e.g. `replace_module_config`
  where the scenario pinned `set_module_config_key`). Replaced by
  effect-based grading (see Added). Old-syntax scenarios now fail with
  a single hard error pointing at the migration path; gallery scenarios
  have all been migrated.

## [1.0.2] — 2026-05-23

### Added
- **External secrets provider dispatch for `@aq.secret('KEY')`** (Phase 32).
  Previously `@aq.secret()` was a silent alias of `${KEY}` — read from
  `os.environ` only — and `secrets.provider: aws|gcp|azure` was validated
  at config load but never dispatched to. `aqueduct.yml` now loads in two
  passes: pass 1 expands `${VAR}` and validates the config (including
  `secrets.provider`); pass 2 calls the configured provider for every
  `@aq.secret('KEY')` token and re-validates. Backends: `env` (default,
  reads `os.environ`), `aws` (Secrets Manager via `boto3`), `gcp`
  (Secret Manager via `google-cloud-secret-manager`), `azure` (Key Vault
  via `azure-keyvault-secrets` + `azure-identity`), `custom` (dotted-path
  callable). Each provider uses its cloud's ambient credential chain
  (boto3 default chain / GCP ADC / Azure `DefaultAzureCredential`) — no
  cloud credentials live in `aqueduct.yml`. `resolve_secret()` no longer
  caches into `os.environ`, so provider-side rotation takes effect on the
  next call and the per-call audit trail is preserved.
- **Secret redaction registry (`aqueduct/redaction.py`)** scrubs every
  resolved `@aq.secret()` value (replaced with `[REDACTED]`) from outputs
  that cross a trust boundary or hit persistent storage. Wired at five
  sinks: console / log output (`click.echo` + root-logger filter),
  observability.db rows, patch sidecar files
  (`patches/{pending,applied,rejected}/*.json`), outbound webhook
  payloads (body only — headers and URL are intentionally untouched
  because user-authored credentials in headers are the intended
  transmission path), and LLM agent request payloads (`system_prompt` +
  `messages` sent to Anthropic / OpenAI-compat endpoints). Defense-in-
  depth, not the primary defense — values shorter than 8 chars or below
  2.5 bits/char of Shannon entropy are NOT registered and emit
  `AQ-WARN [secret-weak-redact]` instead, because substring removal of
  short common identifiers produces too many false positives.
  Token-boundary matching (`(?<!\w)X(?!\w)`) prevents a secret value
  equal to `hunter2` from redacting occurrences inside `hunter2_module`.

## [1.0.1] — 2026-05-23

### Fixed
- **`aqueduct run` no longer crashes when the `git` binary is missing**
  from the runtime environment. The uncommitted-applied-patches check
  shells out to `git log`; previously a missing or non-executable
  binary raised `PermissionError` / `FileNotFoundError` before the
  return-code branch ran, killing the run. The exception is now caught
  and the check falls back to "treat all applied patches as
  uncommitted" — same behavior as a `git`-less directory.
- **`aqueduct patch list --format json` now emits `run_id`,
  `blueprint_id`, `failed_module`** (sourced from the patch file's
  `_aq_meta` block). Without these fields downstream integrations
  (Airflow trigger, CI gates) had no reliable way to match a patch to
  the run that produced it. Also updates the Airflow
  `AqueductPatchTrigger` to match by exact `run_id` first, with the old
  substring heuristic kept as a fallback for pre-1.0.1 patches.
- **`aqueduct run` now exits `3` (HEAL_PENDING) when `approval_mode: human`
  or `ci` stages a patch under `patches/pending/`** — previously the
  command exited `1` regardless of why the run failed, so downstream
  tooling (Airflow operator, CI gates) had no way to distinguish
  "needs human approval" from a hard runtime fault. The exit-code
  contract documented in `aqueduct/exit_codes.py` and
  `docs/specs.md §10.7` was not actually emitted by the CLI; this lands
  the missing wiring. Hard runtime fault now correctly exits `2`
  (DATA_OR_RUNTIME). Exit `1` is reserved for config / parse errors.
- **Tier-0 resolution now applied to the Blueprint `agent:` block**
  (`base_url`, `model`, `provider_options`, `prompt_context`). Previously
  these fields were passed through the parser un-resolved, so
  `${ENV_VAR}` and `${ctx.*}` references stayed literal — for example,
  `base_url: "${AQ_OLLAMA_URL}/v1"` reached httpx as the literal string
  `${AQ_OLLAMA_URL}/v1` (no scheme), surfacing as a confusing URL-protocol
  error. Resolution is wrapped in `try/except ValueError → ParseError`
  for parity with `spark_config` / `macros` — a missing env var now
  raises a clean `parse error: agent config resolution failed: …`
  instead of a raw `ValueError` traceback.

### Added
- **Apache Airflow integration** (`aqueduct.integrations.airflow`, Phase 31):
  drop-in `AqueductOperator` plus deferrable `AqueductPatchSensor` /
  `AqueductPatchTrigger`. The operator subprocesses `aqueduct run` and maps
  the engine's stable exit codes onto Airflow outcomes; `HEAL_PENDING`
  (exit 3) triggers an async patch-approval wait that releases the worker
  slot via Airflow 2.7+ deferrable triggers. Install with
  `pip install aqueduct-core[airflow]`. See
  `aqueduct/integrations/airflow/README.md` for the full DAG example.
- New extras: `[airflow]` (slim, Airflow only) and `[schedulers]`
  (aggregate of scheduler integrations). `[all]` now pulls in
  `[schedulers]`.
- `docs/specs.md §10.7 — Orchestrator Integration Contract`: documents
  the engine-agnostic exit-code + patch-CLI JSON surface that any
  scheduler integration consumes.

## [1.0.0] — 2026-05-18

First stable release. The stability contract (`docs/STABILITY.md`, exit
codes, frozen public API) is now in force; subsequent breaking changes
follow SemVer. Consolidates everything previously staged as Unreleased
and the `1.0.0a2` pre-release (which never shipped separately).

### Added
- `aqueduct benchmark` accepts a single `.aqscenario.yml` (positional or
  `--scenarios`), not only a directory.
- `aqueduct benchmark --provider` / `--base-url` / `--timeout` to override
  the agent connection per run (precedence: flag > `aqueduct.yml` agent >
  default). `--timeout 0` = unbounded read (connect still fails fast).
- `aqueduct init` now writes a `.gitignore` (Spark/Aqueduct runtime
  artifacts, `.env`, ephemeral `patches/{pending,rejected}/`); existing
  files, including a user `.gitignore`, are never overwritten.
- `aqueduct doctor` `--aqtest` / `--aqscenario` pre-flight, combinable
  with a config/blueprint probe in one pass.
- `aqueduct benchmark --format json` now includes the generated
  `patch` (PatchSpec) per result, so a failure can be diagnosed without
  re-running. Table mode prints a `(N failed — rerun with --format
  json …)` hint when any scenario fails.
- Stable exit codes (`aqueduct/exit_codes.py`) and `docs/STABILITY.md`;
  downstream tooling can branch on `$?`.
- `aqueduct schema --target {blueprint,config,patch}` emits the JSON
  Schema for IDE autocomplete / CI gating.
- `--format json` on `aqueduct runs` and `aqueduct patch list`.
- Two-tier suppressible warning system with stable `AQ-WARN [rule_id]`
  IDs and `warnings.suppress` / `--suppress-warning`. New rules:
  `kafka_checkpoint_stale`, `nondeterministic_fanout`,
  `count_col_likely_count_star`, `file_format_no_repartition`,
  `jdbc_missing_partition`, `jar_availability`.
- Post-patch `explain()` regression check and
  `agent.block_on_explain_regression` (warn-only by default).
- `aqueduct patch preview` with lineage + sandbox validation gates;
  `agent.patch_validation: full_run | sandbox`.
- Pluggable observability/lineage/depot store backends (Postgres;
  Redis for depot) with `aqueduct stores info|migrate` and
  `[postgres]` / `[redis]` extras.
- Secrets providers `aws` / `gcp` / `azure` / `custom` with
  `[aws]` / `[gcp]` / `[azure]` extras.
- Global `--log-format {text,json}` for structured log shipping.
- `agent.max_heal_attempts_per_hour` spend-cap on self-healing.
- `aqueduct compile --show {manifest,provenance,inputs,all}`.
- Ingress `partition_filters`; Egress `maintenance:` (Delta
  OPTIMIZE/VACUUM); Channel `metrics_boundary`; Channel
  `materialize: incremental` + `watermark_column`; `Manifest`
  input fingerprinting.
- Assert `error_type` plus `agent.guardrails.heal_on_errors` /
  `never_heal_errors` pre-trigger guards.

### Changed
- `aqueduct benchmark` scoring is now two-tier: **correctness gates**
  PASS/FAIL (`patch_is_valid`, `patch_applies`, `expected_patch`), while
  **diagnosis quality** (`root_cause_contains`, `expected_category`,
  `max_attempts`, `min_confidence`) is recorded as a diagnosis score and
  reported (`diag_score`/`soft_failures` in JSON, `d%` + `Diag score` in
  the table) but **never fails an otherwise-correct fix**.
- Gallery `aqscenarios` benchmark suite reworked for fidelity: each
  scenario now has its own blueprint carrying exactly one real defect,
  `inject_failure.error_message` mirrors authentic Spark output (error
  class, `SQLSTATE`, suggestion list), and scoring is op-agnostic
  (outcome + diagnosis, not a hard-coded patch op). Previously 4/5
  scenarios injected an error unrelated to the shared clean blueprint
  (unsolvable/ungradable).
- `.env` is now auto-loaded by **every** command from the directory of
  the config/blueprint passed (previously only `run`/`doctor`/`validate`).
  A one-line stderr notice reports what was loaded; `-e KEY=VAL`
  (docker-style, highest precedence) and `AQ_NO_ENV_FILE=1` added.
- `aqueduct benchmark` default `--workers` is now `1` (serial); set `>1`
  to parallelize scenario×model pairs.
- `aqueduct test` always runs on `local[*]` and ignores
  `deployment.master_url` (isolated unit tests); `--master` overrides for
  cluster-runtime-dependent modules.
- `aqueduct doctor` default view shows only actionable rows; skip/green
  low-signal rows collapse into one line, `--verbose` expands. The Spark
  check is a fast bounded reachability probe by default; `--preflight`
  runs a full unbounded session check. `agent` no longer warns when
  self-healing is simply unconfigured (opt-in). Convention: ✗ = "will
  break", ⚠ = "runs but fragile". doctor stays advisory.
- Init scaffold directories renamed for consistency: `tests/` → `aqtests/`,
  `benchmarks/` → `aqscenarios/`.
- Default `agent.model` is now `claude-sonnet-4-6`.
- Incremental-Channel watermark is computed from materialized Egress
  output instead of re-scanning the upstream DAG twice.
- Cluster/cloud deployments with relative store paths now error in
  `doctor` and warn at run start.
- Public API frozen to a small `__all__` (`parse`, `ParseError`,
  `AqueductWarning`, `__version__`); everything else is internal.
- **BREAKING:** `aqueduct benchmark --output` renamed to `--format`
  (data-shape selector; consistent with `runs`/`report`/`patch list`).
  `-o`/`--output` is now reserved exclusively for file destinations
  (`schema`); `compile --show` keeps its artefact-slice meaning.

### Deprecated
- _None._

### Removed
- **BREAKING:** `heal --scenario` removed. `heal` is production-only
  (heal a real failed `run_id`); scenario evaluation is
  `aqueduct benchmark <file-or-dir>`.
- **BREAKING:** `heal --print-prompt-format` removed — folded into
  `--print-prompt [text|json]` (bare = text, `--print-prompt json` for
  JSON).
- **BREAKING:** `warnings.silence_all` config field and `--no-warnings`
  flag removed. Use `warnings.suppress: ["*"]` or
  `--suppress-warning '*'`.
- **BREAKING:** `aqueduct check-config` removed — use `aqueduct validate`
  (auto-detects blueprint vs engine config by header, accepts multiple
  files).
- **BREAKING:** `aqueduct doctor --config` / `--blueprint` removed — pass
  the file positionally (header-sniffed).
- **BREAKING:** `agent.llm_timeout` → `agent.timeout`,
  `agent.llm_max_reprompts` → `agent.max_reprompts` (the self-healing
  subsystem is uniformly "the agent"; no aliases).
- **BREAKING:** `probes.block_full_actions_in_prod` →
  `danger.allow_full_probe_actions` (inverted polarity).
- `ollama_options` renamed to `provider_options`.

### Fixed
- Postgres store backend (`stores.*.backend: postgres`) no longer crashes
  on DuckDB-only DDL/upsert SQL; non-DuckDB store `path` (a DSN) is no
  longer mis-created as a local directory. (Full multi-backend
  certification still pending.)
- `--parallel` Probe race that silently dropped Probe signals
  (ISSUE-042).
- Tier-0 tokens (`${VAR:-default}`, `${ctx.*}`) are now resolved in
  top-level `spark_config` and `macros` (ISSUE-027).
- `materialize: incremental` blueprints referencing `${ctx._watermark}`
  no longer fail `validate`/`run`.
- Duplicate missing-env-var error lines collapsed to one.
- pyspark `DataFrame.sql_ctx` deprecation warning from the explain gate.
- Misleading agent "failed after N attempts" log now reports the actual
  attempt count.
- Bundled `aqtest.yml.template` rewritten to match the real
  `aqueduct test` runner schema (it previously documented a
  non-functional format).

## [1.0.0a1] — 2026-05-12

### Added
- `danger:` block (`allow_aggressive_patching`,
  `allow_full_probe_actions`), defaulting `false`, with a startup
  warning when any is enabled.
- `approval_mode: ci` with `agent.ci_webhook_url`; webhooks
  `on_patch_pending` / `on_ci_patch` make `human`/`ci` review viable
  for teams.
- `PatchSpec.confidence|category|root_cause`; `confidence < 0.7`
  auto-escalates to human review regardless of `approval_mode`.
- `healing_outcomes` table persisting every healing attempt.
- Per-pipeline store paths (`.aqueduct/obs/{blueprint_id}/`),
  removing DuckDB write contention across pipelines.
- Java/Scala UDFs (`lang: java|scala`, `jar:`, `entry:`).
- `schema_hint` `strict | additive | subset` modes.
- Git-integrated patch lifecycle: `aqueduct patch
  commit|discard|log|rollback|list`; surgical `set_module_config_key`
  op.
- Provenance layer — smaller/cleaner LLM prompts; `doctor` recurses
  into Arcades.
- `aqueduct init` project scaffold (writes `.gitignore` ignoring the
  ephemeral patch dirs); `aqueduct runs`; `aqueduct report`;
  `aqueduct lineage`; `aqueduct signal`; `aqueduct heal`;
  `aqueduct test`.
- Probe signal types: `value_distribution`, `distinct_count`,
  `data_freshness`, `partition_stats`.
- Compile-time SQL macros; Channel `op: join` with broadcast hint.

### Changed
- `runs.db` + `signals.db` consolidated into a single
  `observability.db`.

### Removed
- **BREAKING:** `agent.validate_patch` field removed — `aggressive`
  mode now always validates in-memory before writing.

## [1.0.0a0] — 2026-04-27

First alpha on PyPI (`aqueduct-core`). Phases 1–9: the core engine.

### Added
- Declarative blueprint pipeline: Parser → Compiler → Executor with
  `aqueduct validate | compile | run`.
- Modules: Ingress, Egress, Channel (SQL), Junction, Funnel, Probe;
  Spillway error-row routing; Assert data-quality module
  (`abort|warn|webhook|quarantine|trigger_agent`).
- Resolution: Tier-0 (`${ENV:-default}`, `${ctx.*}`, env/profile/CLI
  overrides) and Tier-1 (`@aq.date.*`, `@aq.run.*`, `@aq.depot.*`,
  `@aq.secret()`); Arcades (reusable sub-blueprints).
- Depot KV store; Python/Java/Scala UDFs; `RetryPolicy` (exponential/
  linear/fixed + jitter + deadline); per-module `on_failure`;
  opt-in checkpoint/resume.
- LLM self-healing: `approval_mode: disabled|human|auto|aggressive`,
  deterministic guardrails (`allowed_paths`, `forbidden_ops`), patch
  staging/rollback; Anthropic + Ollama + OpenAI-compatible providers
  over plain HTTP (no `anthropic` SDK dependency).
- Observability: Surveyor (DuckDB) run records / failure contexts;
  SparkListener per-module metrics; column-level lineage via sqlglot;
  `aqueduct doctor`.
- Remote Spark (`local[*]`, `spark://`, `yarn`, `k8s://`); partial DAG
  (`--from`/`--to`); `--execution-date` for backfills.
