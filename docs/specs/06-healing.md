# 8. Self-Healing and LLM Agent Loop

# **8. Self-healing & LLM agent loop**

## **8.1 Design philosophy**

The LLM agent operates within a grammar, not in free-form code generation mode. It can only propose structured PatchSpec operations, valid, schema-checked modifications to the Blueprint. This constraint makes every agent action auditable, reversible, Git-diffable, and explainable to a human reviewer.

**Model-agnostic design.** The PatchSpec grammar is deliberately narrow, 14 schema-checked operations with no code generation, so the agent works reliably across model sizes. A 7B parameter local model handles ~70% of production failures (path typos, format mismatches, column renames, simple SQL fixes) in a single attempt. Larger models unlock `agent.deep_loop` (in-conversation sandbox feedback) and multi-model cascading for complex cases like OOM tuning and multi-module restructures. The deterministic guardrails, gate pyramid, and structured prompt apply the same safety guarantees regardless of model size.

**The `agent:` block is split by kind, at both levels (2.59).** A Blueprint's `agent:` block is **POLICY-ONLY**: risk decisions about THIS pipeline; `approval`, `on_pending_patches`, `max_patches`, `guardrails`, `confidence_threshold`, `on_heal_failure`, `allow_defer`, `deep_loop`, `sandbox_mode`, plus a shared subset (`max_reprompts`, `prompt_context`, `max_heal_attempts_per_hour`, `patch_validation`) that overrides the engine's own default when set. `aqueduct.yml`'s `agent:` block (`AgentConnectionConfig`) is the **only** place CONNECTION settings live (`provider`, `base_url`, `api_key`, `model`, `provider_options`, `timeout`, `cascade`) an endpoint fact about the deployment, not a per-pipeline decision. A Blueprint cannot set or override any connection field; writing one into a Blueprint's `agent:` block is a schema-level rejection naming the field (`AgentSchema` uses `extra="forbid"`), not a silent no-op. This is the same split already applied to `engine:` (§10.1): `engine.spark`'s Blueprint-level block omits `master_url`, `engine.duckdb`'s omits `database_path`/`s3_*`; a Blueprint does not get to decide deployment/connection concerns. The security reasoning is sharper here: the healing loop ships `FailureContext` (pruned manifest, provenance, error text) to whichever endpoint is configured, so if a Blueprint could pick that endpoint, any pipeline failure would be an exfiltration opportunity to a host the pipeline's author (not the operator) controls.

**Solo vs cascade.** With a single `agent.model:` in `aqueduct.yml`, healing runs **solo**, one model, the flat `agent.*` connection (`model`, `base_url`, `timeout`, `budget`, …). Configuring `agent.cascade:` (also `aqueduct.yml` only) switches to **cascade** mode: a list of tiers tried in the order you define them.

**Multi-model cascade.** `agent.cascade:` is configured at engine level (`aqueduct.yml`) only; a Blueprint cannot declare or override a cascade (cascade tiers are entirely connection settings: each tier carries its own model/provider/base_url/api_key). Aqueduct tries the tiers strictly in the order you define them, it does **not** reorder by price or capability. The usual convention is to list the cheapest/fastest model first and escalate to a stronger one, but that ordering is the author's responsibility, not the engine's. Escalation triggers on `stuck_signature`, `exhausted_attempts`, or `deferred`; a tier whose provider is simply unreachable (`api_error`) escalates to the next tier and only aborts on the final tier. Each tier has its own budget (`max_reprompts`, `max_seconds`) and can override `provider`, `base_url`, `api_key`, `timeout`, `deep_loop`, and `allow_defer`. **A tier's own fields override the flat `agent.*` only by inheritance, not merge:** a field the tier leaves unset inherits the solo/flat value; a field the tier sets is an independent key that wins for that tier (so `--set agent.timeout` raises the flat default + every inheriting tier, but does **not** reach a tier that declares its own `timeout:`). A tier's budget reuses the top-level `agent.budget` axes with `max_reprompts` / `max_seconds` swapped for the tier's own values. `max_tokens_total` spans the WHOLE cascade: each tier receives the remaining allowance, and the cascade stops with `budget_tokens_exceeded` when it is spent. A defer on a non-final tier escalates (its diagnosis is discarded); a defer on the final tier is staged for human review. The producing tier's model and 0-based index are persisted on `healing_outcomes.model` / `model_cascade_position`. `aqueduct doctor <blueprint>` checks each tier's credentials/endpoint ahead of time.

**Pending-patch short-circuit.** Aqueduct never queues a second unreviewed patch for the same problem. Before any LLM call, `aqueduct run` checks `patch_index` for a `pending` row on the current `blueprint_id` (`patch/index.py::list_by_status`); if one exists, the run stops immediately without calling the model — the message names the existing patch id and how to review it (`aqueduct patch pull <id>` / `aqueduct patch list`), and the run exits `HEAL_PENDING`. No `heal_attempts` row is written (there was no LLM attempt); the failure itself is already recorded by the normal per-iteration `Surveyor.record()` call that runs before this check. Every failure still hashes into a stable signature, `(error_class, failed_module, normalized_message)` plus a coarse variant that drops the module (`aqueduct/agent/signature.py::from_failure_context`), and is still stamped onto `healing_outcomes.failure_signature[_coarse]` and `patch_index.signature[_coarse]` for observability and the reprompt-loop's budget axes (`same_signature_overall`, `progress_stalled_window`) — it no longer keys any lookup. `aqueduct patch list`/`pull` resolve through SQL queries against the **`patch_index`** observability table (status + body `object_key`) rather than scanning the `patches/` directory, backend-blind, so they work when patch bodies live on s3/gcs/adls. Patch bodies are written through the PatchStore (`pending` / `applied` / `rejected`) and every status transition is recorded in `patch_index`; local-checkout commands (`patch apply` / `patch reject`) stay on the filesystem but flip the index status so the two stay consistent.

## **8.2 The healing flow**

```
Pipeline failure → Capture → Pending check → Prune → Generate → Reprompt → Gate → Confirm and write
```

### 1. Capture

Transient errors retry first (per `retry_policy.max_attempts`). Non-transient failures, schema drift, missing columns, bad paths, OOM, trigger the agent. The Surveyor assembles a self-contained failure package:

- Compiled module config
- ProvenanceMap (where every config value came from)
- Sliced lineage neighbourhood
- Structured root-cause block (offending column + Spark suggestions)
- `inputs_fingerprint` (file metadata to distinguish data-drift from code bugs)

### 2. Pending check (zero tokens)

Before any LLM call, `patch_index` is checked for a `pending` row on this blueprint: if one exists, the run ends immediately, exit `HEAL_PENDING`, no LLM call. Otherwise → continue below.

### 3. Prune

A ContextPruner trims the package to the failure's blast radius. Pruning rules:

| Error class | Manifest scope |
| :- | :- |
| `ColumnNotFound`, `TypeMismatch`, `AnalysisException` | Failed module + 2 upstream + 2 downstream |
| `SparkException` with OOM/shuffle | Full manifest |
| All other errors | Failed module + direct upstream |

### 4. Generate

The LLM responds with a structured PatchSpec, a list of typed operations that map one-to-one to Blueprint edits. Anything else is rejected.

### 5. Reprompt

Schema errors, guardrail violations, and gate rejections feed back into the same conversation as annotated, field-level corrections. The loop is bounded by a multi-axis budget:

| Axis | Default | What it guards against |
| :- | :- | :- |
| `max_reprompts` | 5 | Hard ceiling on LLM round-trips |
| `max_seconds` | 120 | Wall-clock cap on LLM-conversation time per heal call |
| `max_tokens_total` | 50,000 | Sum of prompt + completion tokens |
| `same_error_consecutive` | 2 | Stuck on identical error signatures |
| `same_signature_overall` | 3 | Same error signature across the run |
| `progress_stalled_window` | 3 | No new distinct signatures |

When `same_error_consecutive` trips, the loop escalates: temperature is bumped and a skeleton reprompt template is used for one more attempt before honouring the abort.

`max_seconds` counts LLM time only: validation-gate work (deep-loop sandbox replay, lineage) is excluded from the clock, so a slow sandbox cannot exhaust the heal budget. Transient provider errors (HTTP 429/503/529) are retried per `agent.retry` (default 2 retries, exponential backoff with jitter, server `Retry-After` honored); retry sleeps count as LLM time and are always capped by the remaining per-call deadline.

### 6. Gate

Before a patch touches the Blueprint, four numbered gates plus an unnumbered compile-check run in order. The numbering is load-bearing: it matches the module docstrings (`patch/preview.py`, `patch/resolvability_gate.py`) and the test filenames (`tests/test_patch/test_patch_preview_gate3.py` is the SANDBOX gate). The compile-check is a step inside `patch/apply.py::apply_patch_file`, not a gate of its own, which is why it carries no number.

1. **Gate 1, guardrails**: path and operation policy (deterministic, enforced before the LLM response is parsed); including `allowed_paths` and, evaluated after it over the same resolved value, `deny_patterns` (subtract-only)
2. *Compile-check* (unnumbered): the patched dict must re-parse into a valid Blueprint. Runs immediately after Gate 1.
3. **Gate 2, lineage**: column-level diff catches broken references before the engine sees them
4. **Gate 3, sandbox**: sampled or full replay catches "parsed but produces nothing"
5. **Gate 4, resolvability** (2.66, `patch/resolvability_gate.py`): asks whether every `declare_dependency` op in the patch names a requirement that is at least resolvable; never whether Aqueduct can or should install it (Aqueduct never installs anything). Five statuses: `not_applicable` (no `declare_dependency` op in the patch; no check owed), `pass` (already installed; auto-apply eligible), `warn` (resolves on PyPI but is not installed; **a deliberate defer to a human**: install it, then `aqueduct patch apply <id>`; a `warn` here **never auto-applies**), `fail` (no such package on PyPI, or no published version satisfies the specifier; rejection, feeds the reprompt loop), `unavailable` (the PyPI check itself could not run; fail-closed, same posture as Gate 3's `unavailable`). Multiple `declare_dependency` ops in one patch: every requirement is checked and the WORST verdict wins, ranked `fail` > `unavailable` > `warn` > `pass`.

§8.7 below describes the same sequence in full detail. If the two ever disagree, the code wins: `_check_guardrails` is Gate 1, `run_lineage_gate` is Gate 2, `run_sandbox_gate` is Gate 3, `run_resolvability_gate` is Gate 4.

**Depot staleness notice (2.69).** A failing run may itself have written to the Depot (an Egress with `format: depot`) before it failed, so by the time Gate 3 recompiles the patched Blueprint for its sandbox replay, a depot-derived value the failure saw may have moved. When the caller passes the depot reads resolved at failure time (`_CompileResult.depot_reads`, threaded from `aqueduct run`'s own compile), `run_sandbox_gate` recompiles with its own depot-read sink and, for every key present in BOTH maps whose value differs, prints `depot key 'X' changed since failure: 'old' → 'new'` to stderr and folds the same line(s) into a `pass` result's `detail`. A key present on only one side is not a staleness signal (the patch may have added or removed a depot reference) and is skipped. This is purely informational: it never changes the gate's `status`, never blocks auto-apply, and is not built on any new snapshot/versioned-depot store — it is a diff of two already-resolved Tier 1 reads.

**Sandbox gate on a polyglot Blueprint (2.37).** The sandbox gate replays through one target engine's own `ExecutorProtocol`: a single session, a single engine. Against a Blueprint compiled to more than one island (§4.3's cross-engine handoff, §10.9), that shape can only ever validate ONE of the Blueprint's engines, which would look like a real pre-apply check while actually covering nothing about the rest. So it does not attempt a partial or single-engine-shaped replay: it returns `unavailable` immediately, and **that blocks auto-apply** (2.63); like a missing engine dependency, this is a replay that was owed and could not happen, so the patch stops for a human rather than going through unverified. The run prints the reason at the moment it happens (not only into `patch_simulation`) because a user who expects every patch to be sandbox-replayed before it touches their Blueprint needs to be told this one wasn't. `--sandbox`'s whole-Blueprint dry-run refuses a polyglot Manifest outright for the same reason (`CONFIG_ERROR`); it runs on any single-engine `deployment.engine` that declares the `tooling.sandbox_dry_run` capability leaf (both shipped engines do), refusing loudly with the leaf's hint for one that does not. A genuine multi-session polyglot replay is future work, not this release.

### 7. Confirm and write

Only after every gate passes does the patch run against the real pipeline. The on-disk Blueprint is rewritten only if the full re-run succeeds. Failed patches stage to `patches/pending/` for inspection.

### 8. Chained multi-patch healing

`agent.max_patches` (default `1`, Blueprint-only) is the ONE counter for the whole heal: a single-attempt heal at the default, or, with `max_patches: N` (N > 1, `agent.approval: auto`, non-cascade path), chained multi-patch healing — the standard (and only) behavior of the multi-patch loop as of 2.78. There is no separate opt-in flag.

**Motivation.** A naive "N independent retries of the same failure" loop re-diagnoses the SAME first failure on every attempt: it applies a candidate patch in memory, re-runs the pipeline, and, when the re-run still fails, discards the candidate and re-executes against the *original, unpatched* Blueprint. With two or more independent bugs, the model can diagnose bug #1 correctly on every single attempt and it is thrown away every time, because the pipeline still fails downstream at bug #2 and nothing carries bug #1's fix forward. Chaining fixes this.

**Chain semantics.** On a candidate patch that validates in memory but still leaves the pipeline failing, the loop checks *where* the new failure surfaced:

- **Different module than the one just patched** → the candidate was right: it is folded into an accumulating multi-op `PatchSpec` (operations concatenated in link order) and the loop advances to diagnose the new failure.
- **Same module again** → the candidate was wrong: only THAT candidate is discarded (the already-proven accumulated patches are kept) and the SAME failure is retried.

Every LLM diagnosis call spends one unit of `max_patches`, regardless of outcome — advance, same-module discard, or gate rejection. The loop ends when the pipeline is fully solved, `max_patches` is exhausted, the model returns no patch, or `agent.on_heal_failure: abort` fires on a same-module retry.

Each attempt's diagnosis and the pending-patch check (§8.2 step 2) operate independently: an attempt's failure has its own error signature (§8.6) and its own `blueprint_id` check.

**Disk invariant.** Nothing is written to the Blueprint until the FULL accumulated patch passes the pipeline end-to-end. There is never a partial/half-correct Blueprint on disk mid-chain: every intermediate apply is the existing in-memory apply path (`_apply_patch_in_memory`), re-applied against the *original* on-disk Blueprint with the growing operation list, never against a previously-written file. Exactly ONE combined `PatchSpec` is ever staged or written for a given heal — never a chain of separate staged patches.

**Sandbox requirement.** `agent.max_patches > 1` requires `agent.sandbox_mode` other than `"off"` — refused at run-start (`CONFIG_ERROR`) otherwise, since each attempt's advancement test IS the sandbox gate. A single-attempt heal (`max_patches: 1`, the default) is unaffected — `sandbox_mode: off` stays legal there.

**Gates.** Each attempt's validation IS its advancement test: the existing in-memory apply + full pipeline re-run (the same gate `full_run` patch validation already performs for a single patch, just invoked once per attempt against the growing accumulated patch). The final combined multi-op patch that solves the pipeline still runs the standard gate pyramid before being written: no gate is skipped, only the *per-bug* diagnosis loop is new.

**Approval composes once.** Because nothing hits disk mid-chain, `agent.approval: auto` applies the combined patch after the final full-run pass, one write, not N. `human`/`ci` modes stage exactly ONE combined patch (with each attempt's rationale folded into the staged patch's `rationale` field) instead of cycling through N separate pending-patch reviews.

**Cascade scope.** Multi-model cascade (§8's cascade model) never chains — each cascade tier still produces at most one patch per attempt, bounded by `max_patches`, and a rejected cascade-tier patch is not folded into an accumulating multi-op patch. Chaining is exclusive to the single-model (non-cascade) path described above.

## **8.3 Approval modes**

| Mode | Who applies the patch | When it changes the Blueprint |
| :- | :- | :- |
| `disabled` | LLM never fires | Never |
| `human` | Engineer reviews and applies | Only after human accepts |
| `ci` | External CI receives patch and opens a PR | Only after merge |
| `auto` | Aqueduct applies in-memory, re-validates, writes only if the re-run succeeds | Only on a successful re-run |

Low-confidence patches and any guardrail violation auto-escalate to human review.

**`auto` requires an explicit path allowlist (2.2.0, breaking).** Because `auto` is the only mode where a patch writes to the Blueprint with zero human review, Gate 1 refuses every file-touching patch operation when `agent.guardrails.allowed_paths` is unset, instead of allowing any path. Set `allowed_paths` to fnmatch patterns naming where a heal may write, or use `human` so a person reviews the patch first. See §8.7's Gate 1 paragraph and `docs/threat_model.md`.

**Config key.** `agent.approval` is the config key. Values: `disabled`, `human`, `auto`.

**CI hand-off via `on_patch_pending`.** In `human` mode the patch is staged to `patches/pending/` and the `on_patch_pending` webhook fires. The engine ships **no** long-running receiver and **no** versioned GitHub Action, a CI runner you own receives the payload, obtains the patch body (a run artefact, or `aqueduct patch pull`), and applies + commits it in one step:

```bash
aqueduct patch import received-patch.json --blueprint pipeline.yml
```

`patch import` is `patch apply` + `patch commit` atomically (`--no-commit` stages only), writing a structured `---aqueduct---` commit trailer that `aqueduct patch log` / `rollback` read back. The webhook payload schema (envelope keys `patch_id` / `run_id` / `blueprint_id` / `failed_module` / `source` plus the body's `_aq_meta`) and a copy-paste example workflow wiring `import` + `gh pr create` are documented in the **[Production Guide](../production_guide.md)**.

**Heal-as-PR (2.2.0).** `aqueduct patch pr <patch_ref>` branches, applies, commits, pushes, and opens a PR in one command, approval-mode-agnostic, as an alternative to the manual webhook-plus-import flow above. See [CLI Reference](../cli_reference.md#5-patch-management).

## **8.4 Sandbox modes**

| Mode | Sample size | Egress writes | Danger gate |
| :- | :- | :- | :- |
| `sample` (default) | 1000 rows per Ingress | dropped | n/a |
| `preflight` | full dataset | dropped | `danger.allow_full_preflight: true` |
| `off` | no replay | writes for real | `danger.allow_skip_sandbox: true` |

## **8.5 Patch grammar**

A PatchSpec is a JSON document with the following structure:

```json
{
  "patch_id": "fix-yellow-taxi-path",
  "description": "One sentence: what was wrong and what the fix does.",
  "confidence": 0.9,
  "category": "schema_drift | bad_path | format_mismatch | oom_config | sql_column_not_found | type_mismatch | missing_context | permission_error | other",
  "root_cause": "One sentence: root cause.",
  "operations": [
    { "op": "set_module_config_key", "module_id": "my_ingress", "key": "format", "value": "csv" },
    { "op": "replace_context_value", "key": "paths.yellow_path", "value": "data/yellow/*.parquet" }
  ]
}
```

Supported operations: `set_module_config_key`, `replace_module_config`, `replace_context_value`, `replace_module_label`, `insert_module`, `remove_module`, `add_probe`, `replace_edge`, `set_module_on_failure`, `replace_retry_policy`, `add_arcade_ref`, `defer_to_human`, `set_engine_config`, `replace_macro`, `declare_dependency`.

`defer_to_human` signals an unhealable failure. It makes zero Blueprint changes and terminates the loop with `stop_reason='deferred'`. The payload carries `diagnosis`, `suggestions`, and `confidence_reason` for human review, plus a required `defer_reason` enum: a queryable bucket for WHY the failure was deferred, distinct from the free-prose fields: `infrastructure` | `upstream_schema_change` | `data_shape_change` | `insufficient_context` | `other`. An invalid or absent value is a pydantic `ValidationError`, which feeds the normal reprompt loop (not a hard failure). `defer_reason` round-trips into `heal_attempts.defer_reason` (see the [Observability Guide](../observability_guide.md)) and, when `webhooks.on_defer` is configured, into that webhook's payload alongside `confidence_reason`: a dedicated event so defers stop overloading `on_patch_pending`; unset falls back to firing `on_patch_pending` unchanged. Opt-in via `agent.allow_defer: true`, when false (default), the op is hidden from the LLM prompt.

In `agent.approval: auto`, a **defer-only** patch (every operation is `defer_to_human`) short-circuits straight to the pending/defer staging path, skipping the sandbox replay, the gate ladder, and the apply step, since a defer makes zero Blueprint changes and running the full validation pyramid on it is a pure no-op. A **mixed** patch (a Blueprint-mutating op alongside a defer) still runs the full gate ladder.

`set_engine_config` sets a single key in one engine's Blueprint-level `engine.<engine>:` block (**BREAKING, replaces the engine-named `set_spark_config`; removed, no back-compat alias**). It carries an `engine` field plus `key`/`value`, and addresses BOTH shapes an `engine.<name>:` block can take, using the same structural rule the parser applies when reading the block back (`_resolve_engine_block_raw`): does that engine's block schema declare a `conf` field? Spark's does (a free-form bag, e.g. `spark.sql.shuffle.partitions`): `key` is an opaque vendor config name written into `engine.spark.conf.<key>`, auto-created if absent; covers OOM, shuffle fetch failures, Kryo buffer overflow, dynamic allocation thrashing, GC issues, and driver MaxResultSize, seven of the 20 most common Spark errors. DuckDB's does not (its block declares typed fields directly (`memory_limit`, `threads`)) so `key` must name one of those fields exactly; an unrecognised key is rejected rather than silently writing a field nothing reads. A third engine is addressed correctly the moment its own block schema exists, with no change to the apply path. A stored patch body still carrying the retired `set_spark_config` tag raises `RetiredPatchOpError` (an `AqueductError` subclass) when re-parsed by `aqueduct patch apply`, a typed, distinguishable failure rather than a generic parse error.

**Permission model.** `set_engine_config` is **allowlist-gated at Gate 1**, in every approval mode including `auto`: nothing in the compiler or capability framework constrains `engine.<name>.conf`/typed fields otherwise (`engine.spark.conf` in particular carries no capability leaf at all), so the allowlist is the only thing standing between a heal and an arbitrary engine key. A write of `(engine, key, value)` is permitted iff: **(1)** no core deny entry matches `key`, or the value, for a `deny_values` entry; the deny layer ships in each engine's `engine_config_allowlist.yml` inside the wheel, and no configuration surface (Blueprint, `aqueduct.yml`, a future `danger.` flag) may extend, shrink, or override it; **AND (2)** `key` matches that engine's core allowlist (or, when built, a `danger.`-gated operator extension; not yet implemented); **AND (3)** `key` survives operator narrowing (reserved for a future `aqueduct.yml` surface, not yet built); **AND (4)** the Blueprint's own `agent.guardrails` permit the op (`forbidden_ops` can still block `set_engine_config` outright, independent of allowlist membership). Every layer below core may only *subtract* permission from what core allows: nothing but the core allowlist and, once built, the operator extension, ever *adds* to it. Ownership is tiered: **core** owns the envelope and the deny families (engine semantics; cluster placement, credentials, TLS, arbitrary code loading); the **operator** may extend or narrow further in `aqueduct.yml` (a Blueprint must never grant itself power beyond what the operator installed; the same reasoning as `danger.allow_command_hooks`); the **Blueprint author** controls `forbidden_ops`, `allowed_paths`, `deny_patterns` (evaluated after `allowed_paths`, over the same resolved value; subtract-only; applies even when `allowed_paths` is empty), error filters, approval mode, and confidence thresholds; i.e. may only restrict further, never expand what core/operator already permit. Violations raise `PatchError` (a patch problem: the fix is a different patch); a malformed or missing shipped `engine_config_allowlist.yml` raises the distinct `EngineConfigAllowlistError` (a data problem; the fix is repairing/shipping the file, never retried as a patch). See `aqueduct/executor/engine_config_allowlist.py` and `aqueduct/patch/apply.py::_check_guardrails`. The policy this paragraph describes ships inside the wheel with nothing printing it: `aqueduct patch policy [--engine <name>] [--format text|json]` reads the same allowlist Gate 1 evaluates against and prints the allowed key patterns (with type/enum/range) and the denied families (with their `reason`), per engine; a Gate 1 rejection names this command so the policy is always one command away, not a rule a user has to go read the wheel to find. Since operator extension/narrowing of this policy is not yet built (item **(3)** above), the command's output is the complete policy, not a filtered view of one. The same policy is also disclosed to the healing model itself: the composed system prompt carries an "Engine/session config (`set_engine_config`)" section rendering the TARGET engine's whole allowlist (every allowed key with its type and any `enum`/`range`, every denied family with its `reason`) read from the same file Gate 1 evaluates against, so a rejection means the model made a genuine error rather than guessed at a list nobody showed it. An engine shipping no `engine_config_allowlist.yml` (or an explicitly empty one) is told in the prompt that the op is unavailable for it, because no `set_engine_config` write can clear Gate 1 there; rendering an empty table instead would invite a write that is always refused.

**Efficacy check: an inert config write is refused.** Clearing the permission model above says a write is *allowed*, not that it *does* anything. The config an engine runs with is a merge (`aqueduct/executor/session_config.py::resolve_session_engine_config`): that engine's `aqueduct.yml` `engine.<name>` block, with the Blueprint's own `engine.<name>` entry layered on top, so the Blueprint wins on a key both set, and this invocation's `-s/--set` wins over both (§10.4). A write whose value is already what resolves therefore applies cleanly and changes nothing: schema-valid, allowlist-clean, lineage and sandbox gates green, one heal attempt spent, engine behaviour identical. Gate 1 now also resolves the effective session config before and after the patch and **refuses** a patch that writes engine config but produces an empty delta, raising `PatchError` (the same class as an allowlist violation, and for the same reason: the fix is a different patch, with a different key or a different value). Values are compared as an engine session sees them, so re-spelling `400` as `"400"` counts as no change. The check runs on every apply path (`aqueduct run` self-heal, `aqueduct heal`, `aqueduct patch preview`, `aqueduct patch apply`/`import`, benchmark scenario replay) because it lives in the same shared Gate 1 function they all call. **When the nullifier is the user's own `-s/--set`** the refusal says so explicitly, naming the flag path and the value it pins, and does not offer the ordinary "write a different value" advice: no Blueprint value can outrank a `--set`, so the resolution belongs to the user, not to the model. A patch that moves some keys and writes others the invocation pins still passes on the strength of the ones that move, with the pinned ones reported in the gate's detail and its `cli_pinned` field rather than folded into a clean verdict. `aqueduct patch preview` accepts no `--set`, so its engine-config verdict is always measured with no pins (`cli_pinned` is emitted as `{}` rather than omitted, so a consumer can tell that apart from an older report).

Applicability is derived from what the patch writes, never from a list of operation names: the patch's operations are re-applied to a copy of the Blueprint whose `engine:` block has been removed, and whatever appears in that block afterwards is exactly the set of engine-config keys the patch writes. A patch that writes none of them reports `not_applicable`: the same first-class status the lineage gate uses (`aqueduct/patch/preview.py::LineageGateResult`) for the same reason: reporting `pass` for a check that had nothing to look at is a lie. `aqueduct patch preview` renders this gate next to the lineage/sandbox gates, and `--format json` carries it as `engine_config` (`status`, `detail`, `delta`, `write_targets`).

**Where the delta is recorded.** When the effective config does change, the diff is recorded in the `patch_index` table of the observability store, keyed by `patch_id`, shaped `{engine: {key: {before, after}}}`. It is built by `aqueduct/patch/provenance.py::build_heal_provenance` and written by `aqueduct/patch/apply.py::record_heal_facts`. It is recorded there, and not in the patch's own `_aq_meta`, because it is an apply-time fact rather than a generation-time one: the same patch applied against a different `aqueduct.yml` produces a different delta, and the model that wrote the patch saw neither. It is recorded in the patch index rather than the Blueprint because the Blueprint carries only what a travelling artifact's compile-time gate must read, while a before/after config dict is store data that a growing history should not force into the artifact itself. It is surfaced with `aqueduct doctor` (the `healed-config:<patch_id>` rows) instead. No row is written for a patch that writes no engine config, so a pipeline-only heal leaves the patch index's engine-config columns unset.

`replace_macro` replaces the body of an **existing** macro in the Blueprint `macros:` block, the one place bad SQL was previously unreachable, since the agent is told to preserve `{{ macros.* }}` references rather than inline them. Replace-only: unknown macro names are rejected at apply time (also catches name hallucinations). Re-expansion runs through the normal compile + lineage gates, so parameter mismatches and broken columns in *any* consuming module are caught before the patch lands. Because one macro change affects every module referencing it, the recommended default is to add `replace_macro` to `guardrails.forbidden_ops` so it always gets human review.

`declare_dependency` (2.66) carries one PEP 508-lite `requirement` string, validated at construction with the same parser `dependencies:` uses (§5.5); a malformed string is a pydantic `ValidationError`, never something that reaches the gate or apply path. Applying it appends the requirement to the Blueprint's top-level `dependencies:` list; append-stable, deduped on exact string match, creating the block when absent. It writes ONLY that dict key: never `requirements.txt`, never `pyproject.toml`, never the running environment, and never shells out to `pip`; the same declare-and-check story as `dependencies:` itself, at healing time instead of authoring time. Whether the declared requirement is actually resolvable is answered by Gate 4 (§8.2), not by this op.

**`deep_loop`:** when `agent.deep_loop: true`, sandbox/lineage gates run inside the LLM conversation so the model sees rejection feedback and retries in-context before `apply_callback` runs. Default false preserves the current post-hoc gate behavior.

### Metadata field tolerance

PatchSpec is **strict on operations, lenient on metadata.** Operation-level fields (`op`, `module_id`, `key`, `value`, `config`, …) mutate the Blueprint, so each Op model enforces `extra="forbid"`, a typo there bounces the patch. Top-level metadata fields (`rationale`, `root_cause`, `confidence`, `category`, `patch_id`) are descriptive only; the parser tolerates casing variants and synonym aliases so cheap models don't burn reprompt budget on cosmetics:

| Common LLM variant | Normalised to |
|---|---|
| `rootCause`, `rootcause`, `cause`, `rootCauseAnalysis` | `root_cause` |
| `reasoning`, `reason`, `description`, `summary`, `explanation` | `rationale` |
| `patchId`, `patchID` | `patch_id` |
| `runId`, `runID` | `run_id` |
| `Confidence`, `score` | `confidence` |
| `Category`, `failure_category`, `failureCategory` | `category` |

Anything else that doesn't fit a known top-level field is moved into `misc: dict[str, Any]` rather than rejected, the LLM's stray `"examples"`, `"notes"`, or `"verified_by"` field is preserved for post-mortem visibility but does not participate in mutation. The `misc` field is persisted alongside the patch in `patches/applied/*.json`.

## **8.6 FailureContext structure**

```json
{
  "run_id": "run_20240412_143022_a3f9",
  "blueprint_id": "pipeline.orders.daily_aggregate",
  "failed_module": "cast_and_clean",
  "failure_type": "AnalysisException",
  "error_message": "Cannot resolve column 'event_ts' ...",
  "manifest_snapshot": { /* pruned manifest */ },
  "structural_lineage": { /* ColumnLineageGraph for failed Module */ },
  "probe_signals": [ ... ],
  "retry_history": [ ... ],
  "previous_patches": [ ... ],
  "inputs_fingerprint": { ... }
}
```

## **8.7 Why it is reliable**

A generated patch clears four numbered gates plus a compile-check, in order, before it is ever written into the Blueprint, first failure wins and the patch is discarded or escalated to human review:

```
✓ guardrails  →  ✓ compile-check  →  ✓ lineage  →  ✓ sandbox  →  ✓ resolvability  →  patch applied
```

Gate 1 (guardrails) is deterministic policy: `agent.guardrails.forbidden_ops`, `allowed_paths`, `deny_patterns` (evaluated after `allowed_paths`, over the same resolved value; subtract-only, so it applies even when `allowed_paths` is empty), minimum confidence, enforced by `patch/apply.py::_check_guardrails`. **Under `agent.approval: auto` (2.2.0), an empty `allowed_paths` is deny-by-default rather than allow-all.** `auto` is the only mode where a patch applies with zero human review, so a file-touching op (`set_module_config_key`/`replace_module_config`/`insert_module`/`add_probe`/`add_arcade_ref` writing a `path` or `output_path`) is refused outright when no allowlist is configured, naming the offending value and pointing the operator at `agent.guardrails.allowed_paths` or a switch to `human`. `human` keeps the historical empty-means-unrestricted behavior, since a human reviews the patch before it applies. For `set_engine_config` specifically, Gate 1 also enforces the target engine's core `engine_config_allowlist.yml`: deny entries first (a key/value match raises naming the deny entry's `reason`), then allow-list membership (fail closed: a key on no allow entry is refused), then type, then `enum`/`range` when the matched entry declares one; see the permission-model paragraph above and `aqueduct/executor/engine_config_allowlist.py`. The compile-check (`patch/apply.py::apply_patch_file`, re-parses the patched Blueprint) rejects any PatchSpec whose operations produce a Blueprint that no longer passes the Parser; it runs immediately after Gate 1 but is not itself numbered (AGENTS.md and the module docstrings in `preview.py`/`resolvability_gate.py` number only the four gates below). Gate 2 (lineage, `patch/preview.py::run_lineage_gate`) checks whether the patch breaks a downstream column consumer via live `sqlglot` analysis; a patch whose operations touch zero modules (e.g. `set_engine_config`, which carries only `engine`/`key`/`value`) has no lineage surface to check at all, so the gate reports `not_applicable` with a reason rather than the misleading `pass` a patch that WAS checked and found clean also reports: informational only, it never blocks the patch. Gate 3 (sandbox, `patch/preview.py::run_sandbox_gate`) replays the patched Blueprint against representative data (a per-Ingress row sample by default, no live writes), building its owned session's engine config through the SAME resolver (`aqueduct.executor.session_config.resolve_session_engine_config`) `aqueduct run` uses; so a replay against a non-Spark engine sees that engine's real `engine.<name>.*` config (DuckDB's `memory_limit`/`threads`/`database_path`/`extension_repository`/`s3_*` and any httpfs/secrets wiring) instead of a Spark-only default. For the same zero-module patches Gate 2 reports `not_applicable` for, Gate 3 still runs and still reports `pass` on a clean replay, but words the `detail` honestly rather than letting it read as a validated fix: the session built and the sample replayed successfully under the PATCHED engine config, but a small local sample cannot reproduce the cluster-scale resource failure (OOM, shuffle spill) the patch is usually trying to fix; only the full re-run proves that. `gates_passed` is unaffected; only the `detail` string (also persisted to `patch_simulation`, so any downstream reader inherits the same honest wording) changes. Gate 4 (resolvability, `patch/resolvability_gate.py::run_resolvability_gate`, 2.66) is the odd one out in the pyramid: it never touches the Blueprint or the engine, it only asks PyPI whether every `declare_dependency` op's requirement is resolvable. `not_applicable` when the patch declares nothing; otherwise `pass`/`warn`/`fail`/`unavailable` per requirement, worst-wins across multiple requirements. Its `warn` is unlike every other gate's: it is not advisory, it is a hard defer; a `warn` here always routes the patch to pending/human review, never auto-apply, because the requirement genuinely is not yet satisfied in this environment and Aqueduct will not install it. `aqueduct patch preview --sandbox` runs the same pyramid on demand, before an operator decides whether to apply.

- **No silent mutations.** Every patch is a structured diff with a rationale and a confidence score. Low confidence escalates to human review.
- **No production data corruption.** The sandbox validates patches against representative data before they reach live writes.
- **No runaway loops.** Budgets bound wall-clock, tokens, and stuck-signature counts. A rolling rate-limit caps healing attempts per hour per blueprint.
- **No black-box decisions.** Every LLM turn persists with the gate that rejected it, a stable error signature, and the prompt version.

## **8.8 Drift detection (`aqueduct drift`)**

`aqueduct drift` is an **early warning** you can schedule ahead of the batch
(e.g. cron, 30 min before the nightly job) — it detects and reports upstream
schema drift; it does not heal anything. Healing stays entirely with `run`'s
self-heal, which fixes a pipeline *after* it actually fails; `run` itself is
untouched by `drift`.

Per Ingress, `drift`:

1. Reads the **live source schema metadata-only** (`df.schema`, zero Spark
   actions; parquet/delta from the footer/`_delta_log`, JDBC via a `LIMIT 0`
   probe).
2. Diffs against a **self-owned baseline**: the last-seen schema in
   `drift_checks`. No baseline yet ⇒ it stores the current schema and exits
   cleanly (no Probe dependency).
3. **Classifies** each change: a *dropped* or *type-changed* column is
   **breaking** (a downstream Channel that names it will fail); an *added*
   column is **benign** (a `SELECT named_cols` pipeline tolerates a superset).
   Both are recorded in `drift_checks` and printed in the report; neither
   triggers a heal — a breaking change is left for the next real `run` to
   catch and self-heal reactively.

Scope is **schema drift only**: value-distribution / data-quality drift is out
of scope (a noisier, separate concern). Exit codes: `0` (no drift / baseline
set / only benign drift), `DATA_OR_RUNTIME` (a breaking change was found, or a
source could not be read/diffed).

## **8.11 Remediation domains**

Aqueduct's self-healing operates in explicit **remediation domains**, a
domain is the boundary of what a patch is allowed to touch. Two are built:

| Domain | The patch edits | Its permission model |
| :- | :- | :- |
| `pipeline` | The Blueprint's modules, their config, and the edges between them, via a `PatchSpec` op (§8.5) | The Blueprint's own `agent.guardrails` (`forbidden_ops`, `allowed_paths`) |
| `engine_config` | An engine's session config, via `set_engine_config` writing the Blueprint's `engine.<name>` block | The target engine's core allowlist plus the effective-config delta check (§8, "Efficacy check") |

Every domain, present or future, follows the same principle:

- **Declarative, typed operations**: a fixed, closed grammar of ops (never
  freeform code generation; §1.4's "patch grammar over codegen" principle).
- **Per-domain validation gates**: guardrails, lineage-impact, sandbox
  replay, and resolvability gates (§8.5) sit between "the LLM proposed a
  patch" and "the patch is applied," scoped to what that domain can break.
- **Never freeform code execution**: a domain's operations are data, not
  code; the agent cannot ship an arbitrary script to remediate a failure.

Framing self-healing this way keeps each domain's contract explicit and
gives any domain added later the same shape to slot into, rather than a
one-off extension of the patch grammar.

**A domain is a property of the FIX, not of the failure.** The same failure
is often reachable from more than one domain: an executor OOM on a large
shuffle is fixed either by raising the shuffle-partition count
(`engine_config`) or by inserting a repartition step (`pipeline`). Anything
that classifies work by domain therefore has to allow more than one, and
must not group failures by domain. A benchmark scenario
(`.aqscenario.yml`) declares the domains its expected fix may touch in a
`domains:` list, and `aqueduct benchmark --domain <name>` selects on it; a
scenario declaring both is the normal case for a failure with two valid
fixes, not an ambiguity to be resolved. See
[`gallery/aqscenarios/README.md`](../../gallery/aqscenarios/README.md) for the
scenario file format.

## **8.14 Heal-patch cross-engine provenance**

The healing system prompt is engine-flavored by design (§8's composed-prompt
rule: each engine registers its own `PromptRules` pack). A patch generated
while healing a DuckDB run can therefore carry DuckDB-dialect SQL, cast
syntax, or format options; if the same Blueprint is later compiled for
Spark, that content may be wrong there. Without provenance, healing could
silently manufacture a production defect on a different engine than the one
it was validated against.

**The `healed_by:` block.** `aqueduct patch apply` (and the `agent.
approval: auto` direct-write path) appends one record per applied patch to a
top-level `healed_by:` list on the Blueprint YAML, machine-written via the
same ruamel round-trip machinery as every other patch operation: Blueprint
authors never hand-write it:

```yaml
healed_by:
  - patch_id: fix-yellow-taxi-path
    engine: duckdb
    classification: engine_shaped   # dialect_neutral | engine_shaped
    applied_at: "2026-07-18T00:00:00Z"
    validated_on: []          # engines a GREEN run has validated this patch on since
    reverted_at: null         # set by `aqueduct patch revert`; absent on a live record
```

The record is bounded to these six fields: only what the compile-time
cross-engine gate and `aqueduct patch revert` need to read out of a
travelling Blueprint. `engine_config_delta`, `engine_version`,
`perf_baseline`, `perf_observations` and `run_id` moved to the `patch_index`
table in the observability store, keyed by the same `patch_id` (see
"Where the delta is recorded" above and "Perf attribution" below): those
fields grew with every green run, one before/after config dict per engine
and one perf note per engine per patch, and turned a Blueprint artifact into
a changelog. There is no migration and no back-compat read: a `healed_by`
record still carrying one of the moved fields fails schema validation,
naming the field and stating that its data now lives in the patch index.

The block is compiler-consumed metadata only: no engine executes it, and it
is excluded from `Manifest` assembly entirely, so it never perturbs a
compiled Manifest's content or checkpoint hash.

**Classification.** Every PatchSpec op (§8.5) is classified once, in
`aqueduct/patch/provenance.py`, as `dialect_neutral` (retry/timeout,
structural rewiring, resource/config numerics, schema hints: safe to carry
across engines unmodified) or `engine_shaped` (can introduce SQL text, cast
syntax, or format/session config). `set_module_config_key` is
field-sensitive: classified by whether the config key it touches is
dialect-bearing (`query`, `sql`, `format`, `mode`, `options.*`, …) or not
(`path`, retry counts, …). A patch's overall classification is the max over
its operations: one `engine_shaped` op makes the whole patch
`engine_shaped`.

**Compile-time gate.** For every `healed_by` record whose `classification`
is `engine_shaped`, whose `engine` differs from the compile's target engine,
and whose `validated_on` does not yet include the target engine, `compile()`
emits a suppressible warning (rule_id `cross_engine_heal`) naming the patch,
its origin engine, and the target engine. `dialect_neutral`-only records
never warn: they carry no dialect content to be wrong about. This is a
warning, not an unconditional error: healing's value is a shippable
Blueprint, and a human/CI reviewer decides whether to ship anyway.

**Strict escalation.** `warnings.strict` (`aqueduct.yml`, a list of
`rule_id`s, default empty) promotes listed rules from warning to a hard
`CompileError`: the same rule_id vocabulary as `warnings.suppress`, in the
opposite direction. Setting `warnings.strict: [cross_engine_heal]` makes an
unvalidated cross-engine heal fail the build.

**Self-clearing.** A GREEN run on engine X (the CLI `run` command's success
path) appends X to every `healed_by` record's `validated_on` list, when the
block exists and X is not already present: a Blueprint with no `healed_by:`
block is never touched. The stamp is best-effort: a write failure is logged
and never fails an otherwise-successful run (`aqueduct/patch/apply.py::
stamp_validated_engine`). A green run rewrites the Blueprint only when
`validated_on` actually changes: a run that adds no new engine to any
record leaves the YAML untouched.

**Perf attribution (warn-only).** `validated_on` is binary: the run after
the patch either succeeded or it did not. Config-op success is not binary.
The usual outcome of naive shuffle or partition tuning is a run that
completes and is much slower, which `validated_on` records as an
unqualified success while the patch persists into the Blueprint and every
later run inherits it. Two fields carry the non-binary half.

`perf_baseline` and `perf_observations` are recorded in the `patch_index`
table, not in the Blueprint (see "Where the delta is recorded" above):
they are apply-time and green-run facts, and a Blueprint artifact is not
the place for a history that grows with every run.

`perf_baseline` is snapshotted at apply time: the last green run of this
blueprint that finished before the patch was applied (wall-clock duration
from `run_records`, plus a volume proxy summed from `module_metrics`). It
is snapshotted rather than looked up later because the Blueprint travels
and the observability store does not.

`perf_observations` is written by the same green-run stamp that appends to
`validated_on`, once per engine, so the list is bounded by the engine count
rather than the run count. Each note is `observed` (the ratio, both
durations, and its own caveats) or `not_applicable` (which fact was
missing). There is no `pass` member, because nothing is judged, and no
`fail`, because nothing can fail: **Aqueduct sets no regression threshold.**
There is no measurement behind a number like "3x is a regression", so the
observed ratio is reported and a human decides. The note never blocks a
run, never changes acceptance, and never affects an exit code.

Two runs of one Blueprint are not automatically comparable, and the note
says so rather than implying a causation it cannot support. A baseline
whose engine set differs from the observing run's is refused outright
(`not_applicable`) rather than compared; `run_records` carries no `engine`
column, so the engine set is derived from the per-module `engine` its
`module_results` JSON already records. The input-volume proxy comes from
`module_metrics`, which the Spark executor writes per module and the DuckDB
executor writes only for Handoff modules, so on DuckDB it is reported as
unavailable with a stated reason, never as a zero. Every remaining
limitation (co-applied patches, changed input volume, the standing fact
that wall-clock time has many causes) is written into the note's `caveats`,
which travel with it into the patch index record. See
`aqueduct/patch/perf_attribution.py`.

**Undoing a heal (`aqueduct patch revert`).** A healed patch persists into
the Blueprint and every later run inherits it, including runs long past the
failure it was written for. `aqueduct patch revert <patch_id> --blueprint
<file>` undoes one applied patch's engine-config writes in place: each
`engine.<name>` key the patch wrote goes back to the value the
`engine_config_delta` recorded in the patch index captured, and the
`healed_by` record is stamped `reverted_at:` rather than deleted. Because
the prior value now lives in the patch index rather than in the record
itself, an unreachable observability store is a loud refusal, never an
empty mapping: a missing or unreadable index is indistinguishable from
"nothing was recorded" unless the command insists on telling the two
apart. Keeping the record is the point: deleting
it would erase the fact that a heal ever happened, and leaving it unmarked
would make it describe a Blueprint that no longer carries its change. Every
consumer reads the stamp, so a reverted record stops raising the cross-engine
warning above and stops accruing `validated_on` entries.

Only engine-config writes are revertible, because they are the only change
for which a prior value is recorded anywhere: `set_module_config_key` and its
siblings store the new value alone, so a module patch has no inverse to
compute. A revert is therefore not itself a patch, and the PatchSpec grammar
gains no op for it (the inverse of a write whose prior state was "absent" is
a key deletion, which no op expresses).

The command refuses, naming the reason and writing nothing, when: the patch
also carries a non-config operation (undoing half of it would leave a
Blueprint matching no state that ever ran); a later, not-itself-reverted
patch wrote one of the same keys (revert in reverse order, or use `patch
rollback`); the value has been edited since the patch was applied; the patch
id matches zero or more than one record; or the computed restore cannot be
shown to reproduce the recorded pre-patch effective config exactly. That last
check is mechanical rather than argued: the plan is applied to a copy and
re-resolved through the same function Gate 1 measured the delta with, and any
key that lands anywhere other than its recorded prior value: or any key the
patch never wrote that moves at all: aborts the revert.

`aqueduct patch rollback <blueprint> --to <patch_id>` remains the whole-file
counterpart: it restores the Blueprint from git history, undoing everything
in that commit, and is the documented fallback for every case `revert`
refuses.

**Surfacing healed config keys (`aqueduct doctor`).** For a Blueprint target,
doctor reads the engine-config delta and the perf notes back from the patch
index, keyed by `patch_id`, and emits one `healed-config:<patch_id>` row per
`healed_by` record whose patch index entry carries an `engine_config_delta`:
what was changed and when, whether a green run has validated it, the perf
notes' observed ratios verbatim, and the `patch revert` command that undoes
it. An unreadable patch index is a single `warn` row naming the reason,
never silence. It states **no staleness threshold**:
"healed more than N days ago" is a number nothing supports, since an
hour-old heal on a monthly pipeline is older in every sense that matters than
a year-old one on an hourly pipeline. The one condition that escalates to a
warning is an equality, not a threshold: the value the record says the patch
wrote is no longer what the effective config resolves to, so the record's
perf attribution no longer describes the live configuration and `patch
revert` will refuse it.

**Sandbox requirement.** Chained multi-patch healing (`agent.max_patches >
1`) refuses to run with `agent.sandbox_mode: off`: a `ConfigError` at
run-start with an actionable message. Each attempt's advancement test
depends on validating a candidate before it is folded into the chain;
without sandbox validation there is no safe way to tell "advanced" from
"about to compound a wrong fix." `sample` (default) or `preflight` are
required. A single-attempt heal (`max_patches: 1`, the default) is
unaffected — `sandbox_mode: off` stays legal there.

**Observability.** `heal_attempts` (see `docs/observability_guide.md`)
carries a `chain_link` column: the 1-based index of which attempt within
the heal an LLM-diagnosis row belongs to. `attempt_num` still carries the
reprompt sequence *within* one attempt; `chain_link` is an orthogonal
axis. Token totals aggregate across all attempts into the single
`healing_outcomes` row the combined patch produces.

**Scope note.** Chaining is wired into the single-model (`agent.approval:
auto`, non-cascade) heal path only. Multi-model cascade (§8's cascade
model) never chains: a cascade tier's escalation semantics and a chain's
advancement semantics are two independent axes that have not been
reconciled — each cascade tier still produces at most one patch per
attempt.

---

