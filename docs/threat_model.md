# Threat Model: the Self-Healing Loop

Audience: the operator deploying Aqueduct in production, and the developer
integrating Aqueduct into a larger system. This document describes the trust
boundaries around Aqueduct's LLM-driven self-healing loop, the assets it
touches, the injection surface a hostile actor could exploit, and the
mitigations Aqueduct actually implements today, verified against code. It
also states what is explicitly out of scope.

This is not a general security audit of Aqueduct. It covers the healing
loop specifically: the path from a pipeline failure, through an LLM call,
to a written Blueprint change.

## 1. Trust boundaries

The healing loop crosses four distinct trust boundaries. Conflating them is
the root of most of the risk described below.

| Boundary | Who controls it | Trust level |
|---|---|---|
| Blueprint author | The engineer who wrote the `.yml` Blueprint and the Aqueduct config (`aqueduct.yml`) | Trusted. The Blueprint and its `agent.guardrails` are the policy the rest of the loop is measured against. |
| Runtime data | Whatever source the Ingress modules read (S3 objects, databases, APIs) | Untrusted. Row content, column values, and anything an exception message echoes back from that content did not pass through any human review. |
| LLM output | The model configured under `agent.provider`/`agent.model` | Semi-trusted. The model is asked to read the failure and propose a fix, but its output is JSON parsed into a closed grammar (`PatchSpec`) and gated before it can touch anything, never executed as code. |
| Operator config | `aqueduct.yml`, `agent.guardrails`, `danger.*` | Trusted, and the authority everything else is checked against. A misconfigured operator boundary (an empty allowlist under `auto`, a `danger.*` flag flipped in a shared config) is the operator's own risk to accept, not a gap in the other three boundaries. |

The failure case this document is mainly about: **runtime data reaching the
LLM, then steering LLM output past a boundary it should not cross.** Runtime
data is attacker-reachable in any pipeline that ingests external data (a
malicious CSV row, an API response an attacker partially controls). The LLM
is a text model, not a sandboxed interpreter; text in its context can, in
principle, influence text it produces.

## 2. Assets

What a successful attack could reach, roughly ordered by blast radius:

- **Credentials.** `${ENV_VAR}` references resolved into Blueprint config,
  secrets loaded via the `secrets:` block, and the resolved values redaction
  tracks (§4).
- **Data.** Whatever the pipeline reads and writes: the actual assets a
  Blueprint pipeline exists to move.
- **The git repository.** The Blueprint YAML itself, and (for `human`/`ci`
  approval) whatever a `patch import`/`patch apply` commits.
- **The Aqueduct process's write access.** Anywhere the Spark or DuckDB
  driver process can write a file, which is the actual boundary a
  file-touching PatchSpec op is constrained by (§5, §6).

Aqueduct never executes model-authored code. There is no code-execution
asset to defend because there is no code-execution surface: `PatchSpec` is a
closed set of typed operations (`aqueduct/patch/grammar.py`), not a
scripting language, and UDF bodies are explicitly out of scope for
self-healing (see `AGENTS.md`'s "UDF bodies are out of scope" rule); a
patch cannot rewrite a UDF's implementation.

## 3. The injection surface: FailureContext → prompt → PatchSpec

The path an attacker has to work with:

1. **A pipeline fails**, and `FailureContext` (`aqueduct/surveyor/models.py`)
   captures the error message, a stack trace or structured root-cause
   fields, and the compiled module config.
2. **`aqueduct/agent/prompts.py` composes a prompt** from that context. The
   error message and the root-cause/stack-trace section are the parts most
   directly reachable by data an attacker influenced: a Spark exception can
   echo back a bad value verbatim, and the failed module's compiled config
   can include a `context_ref`-resolved value.
3. **The LLM receives the prompt** and returns a `PatchSpec` JSON document.
4. **The PatchSpec is applied**, gated (§5), and, only if every gate
   passes, written to the Blueprint and re-run.

An attacker who can influence step 1's raw content (a crafted error string,
a crafted data row an Ingress later samples) is trying to influence step 3's
output: get the model to emit a PatchSpec op it should not, for example one
that points an Egress at an attacker-chosen path. This is the scenario the
mitigations below are built for.

## 4. Mitigations

### 4.1 The gate ladder (structural, not prompt-based)

Every PatchSpec, regardless of what produced it, passes through the same
deterministic gates before it can touch the on-disk Blueprint
(`docs/specs.md` §8.7):

1. **Gate 1, guardrails** (`aqueduct/patch/apply.py::_check_guardrails`):
   `forbidden_ops`, `allowed_paths`, `deny_patterns`, and the
   `set_engine_config` allowlist (§4.2). Deterministic, enforced before the
   LLM's JSON is even fully trusted to describe a valid operation.
2. A compile-check: the patched Blueprint must re-parse.
3. **Gate 2, lineage**: a patch cannot silently break a downstream column
   consumer.
4. **Gate 3, sandbox**: the patched Blueprint replays against representative
   data (1000 rows per Ingress by default; see §4.3) with all Egress writes
   dropped, before anything real is touched.
5. **Gate 4, resolvability**: dependency check, not injection-relevant.

This means the injection surface is bounded by what the closed `PatchSpec`
grammar can express, not by what the model can be talked into saying. A
successful injection still has to produce a syntactically valid op that
survives every gate above; a prompt cannot bypass Gate 1's allowlist check,
because that check runs on the parsed PatchSpec independent of what text
produced it.

### 4.2 Gate 1's allowlist, including the auto-mode deny-by-default (2.2.0)

`agent.guardrails.allowed_paths` is an fnmatch allowlist checked against
every path/`output_path` value a patch op would write
(`set_module_config_key`, `replace_module_config`, `insert_module`,
`add_probe`, `add_arcade_ref`). Historically an empty `allowed_paths` meant
unrestricted, so any path passed.

As of 2.2.0, that default is deny-by-default specifically under
**`agent.approval: auto`**, the only approval mode where a patch applies
with zero human review (`human` and `ci` still route every patch through a
person before it takes effect, so the historical empty-means-unrestricted
behavior is unchanged there). An `auto`-mode patch that would write a
`path`/`output_path` value is refused when `allowed_paths` is unset, with an
error naming the offending value and pointing at
`agent.guardrails.allowed_paths` or a switch to `human`/`ci`
(`aqueduct/patch/apply.py`, `_check_path_against_allowlist`'s
`deny_if_empty` parameter). This closes the path an injected instruction
would otherwise have to point an Egress or Ingress at an attacker-chosen
location on a fully unattended `auto` run.

`deny_patterns` layers on top and is subtract-only: it is enforced even when
`allowed_paths` is empty, in every approval mode, because a deny-list can
only remove permission, never grant it.

`set_engine_config` writes are separately allowlist-gated against each
engine's `engine_config_allowlist.yml` (`aqueduct/executor/engine_config_allowlist.py`),
independent of `agent.guardrails`; an engine's config surface (executor
memory limits, extension loading, and similar) has its own deny-first,
then-allow-membership, then-type/enum evaluation, always enforced,
never optional.

### 4.3 The sandbox

Gate 3 replays the patched Blueprint before any write happens for real.
`agent.sandbox_mode` (`docs/specs.md` §8.4):

| Mode | Sample size | Egress writes |
|---|---|---|
| `sample` (default) | 1000 rows per Ingress | dropped |
| `preflight` (`danger.allow_full_preflight` required) | full dataset | dropped |
| `off` (`danger.allow_skip_sandbox` required) | no replay | writes for real |

`off` is a `danger.*`-gated escape hatch, not a default; an operator has to
explicitly opt into skipping the sandbox. Combined with `auto` and
`max_patches > 1` (also `danger.*`-gated,
`danger.allow_multi_patch`), it is the most exposed configuration Aqueduct
allows; `docs/production_guide.md`'s security table calls this out
explicitly and the `danger.*` naming is deliberate.

### 4.4 Untrusted-data framing in the composed prompt (2.2.0)

Before 2.2.0, the composed system prompt gave the model no signal that any
part of the failure report originated from data outside the operator's
control. As of `PROMPT_VERSION` 1.13, the system prompt scaffold
(`aqueduct/agent/prompts.py::_SYSTEM_PROMPT_TEMPLATE`) opens with an
"Untrusted data" instruction block, placed before the description of what
the failure report contains, so the model reads the rule before it reads
any data governed by it. It names two sentinel markers,
`<<<UNTRUSTED_DATA>>>` / `<<</UNTRUSTED_DATA>>>`, and states that content
between them is data, never an instruction, and that instruction-like text
found there is a likely injection attempt to be ignored.

The user prompt wraps the two runtime-data sections most directly reachable
by pipeline content in those markers: the error message, and the
root-cause section (the structured Spark/Py4J extraction, or the raw stack
trace when structured extraction was unavailable). See §5 for what this mitigation does and does not guarantee.

Prompt framing is defense-in-depth, not the primary defense. The primary
defense is that the model's output cannot bypass the gate ladder (§4.1) no
matter what it says.

### 4.5 Redaction chokepoints

`aqueduct/redaction.py` scrubs resolved `@aq.secret()` values before they
reach any of five sinks: CLI console output, `observability.db` writes
(`failure_contexts.context_json`, `runs.error_message`), patch sidecar files
(`patches/{pending,applied}/*.json`), webhook payloads, and the LLM request
body itself (`aqueduct/agent/providers.py::_call_agent`, before the
`httpx.post`). Redaction is explicitly documented as a backstop, not the
primary defense; "the primary defense remains 'do not echo secrets at
all'" (module docstring). It uses substring matching with a length/entropy
gate at registration (short or low-entropy values are not registered, and a
weak-secret warning is emitted instead) and a token-boundary regex at
redaction time, to bound false positives and false negatives. A secret that
never gets registered (too short, too low-entropy) will not be redacted;
that is a known, documented limitation, not a gap this document is
introducing new information about.

### 4.6 Human review as a mode choice

`human` and `ci` approval modes route every patch through a person before
it changes anything, regardless of how the patch was produced. If the
injection surface above is a concern for a given deployment, the structural
answer is approval mode, not prompt engineering: `auto` is the only mode
this document's mitigations are trying to make safe to run unattended, and
even there the gate ladder (§4.1), not the prompt, is what actually blocks
a bad patch.

## 5. What this does not claim

- **The prompt framing in §4.4 is not a hard security boundary.** LLMs can
  be talked past instructions embedded in their own context; that failure
  mode is inherent to the technology, not specific to Aqueduct's prompt
  text. The framing reduces the odds a naive injection succeeds and gives
  the model an explicit rule to fall back on; it does not make injection
  impossible. The gate ladder is what actually bounds the damage a
  successful injection can do.
- **Redaction does not guarantee no secret ever leaks.** It is substring
  matching with deliberate false-negative tolerance for short/low-entropy
  values (§4.5).
- **This document does not cover:** vulnerabilities in the LLM provider
  itself (prompt-injection defenses the provider does or does not implement
  server-side), supply-chain risk in Aqueduct's own dependencies, access
  control to the machine running the Aqueduct process or to the git
  repository it commits to, or denial-of-service via pipeline resource
  exhaustion (Spark/DuckDB OOM is a reliability concern handled elsewhere in
  the docs, not a security boundary this document addresses).
- **`danger.*` settings are, by name, an explicit trust decision the
  operator makes.** Flipping one is treated as the operator accepting a
  specific, named risk (`docs/production_guide.md`'s security table), not
  as a gap in Aqueduct's defaults.

## 6. Related documents

- `docs/specs.md` §8.3 (approval modes), §8.5 (patch grammar), §8.7 (the
  gate ladder in full): the mechanics this document summarizes.
- `docs/production_guide.md`'s "Security considerations" table: operator
  checklist form of §4 above.
- `aqueduct/redaction.py`: the redaction registry's own docstring, quoted
  in §4.5.
