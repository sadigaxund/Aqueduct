# Chained multi-patch healing

Demonstrates chained multi-patch self-healing. The
pre-existing `max_patches > 1` loop re-diagnoses the SAME first failure on
every attempt. It patches a candidate in memory, re-runs, and if the
pipeline still fails, discards the candidate and starts over against the
original, unpatched Blueprint. With two independent bugs, a correct fix for
bug #1 gets thrown away every time because the pipeline still fails
downstream at bug #2.

## Setup

```bash
pip install -r requirements.txt
```

## Two blueprints

`blueprint_bugged.yml` is the deliberately-broken pipeline this demo is
built around (see the two bugs below); it's the file the commands below
run to trigger the chain. `blueprint.yml` is the same
pipeline already healed; it's the file CI's snippet lane runs via
`aqueduct run blueprint.yml`, so the lane stays green with no LLM key.
Both files share the identical `agent: {approval: auto, max_patches: 3,
sandbox_mode: sample}` block, and `aqueduct.yml` sets
`danger.allow_multi_patch: true`, which any `max_patches > 1` run needs.

## How it works

This blueprint has two independent bugs in two different Channels:

1. `priced` references `unit_cost`. The real column is `unit_price`.
2. `discounted` (downstream of `priced`) filters on `quantity`. The real
   column carried through `priced` is `qty`.

A candidate patch that validates but still
leaves the pipeline failing checks *where* the new failure surfaced:

- **Different module** than the one just patched (`discounted`, after
  `priced` was patched) means the candidate is folded into an accumulating
  multi-op patch and the *next* link diagnoses the new failure. The chain
  **advances**.
- **Same module again** means the chain is **stuck** and ends.

So here: link 1 diagnoses and fixes `priced` (bug #1); the accumulated
patch is re-validated end-to-end and the pipeline now fails at
`discounted`, a different module, so the chain advances. Link 2 diagnoses
`discounted` (bug #2) against the manifest with link 1's fix already
applied. Nothing is written to the Blueprint until the full 2-op combined
patch passes the pipeline end-to-end. The loop lives in
`aqueduct/cli/run.py`; `merge_patch_specs`
(`aqueduct/agent/merge.py`) folds the links into the one patch that is
staged or applied.

`agent.max_patches` (set to 3 here) caps the number of
links, independent of each link's own `max_reprompts`/budget ceiling.

**Sandbox requirement.** Each link's validation IS its advancement test:
without per-link sandbox replay, a link has no way to check a candidate
before folding it into the accumulated patch. `require_sandbox_for_chained_healing`
(`aqueduct/cli/run_setup.py`) refuses to start chained healing when
`agent.sandbox_mode: off`; this blueprint sets `sandbox_mode: sample`
explicitly (the same as the default) so the requirement is visible.

## How to run

```bash
python populate_data.py
aqueduct run blueprint_bugged.yml
```

The run fails at `priced` (bug #1). Because `approval: auto` is set and
`danger.allow_multi_patch: true` opens the multi-patch loop, chained
healing goes straight through both bugs in one heal cycle rather than
stopping after the first fix stops "working".

```bash
python inspect_results.py
```

prints the healed output (if the chain solved both bugs) and the recorded
heal attempts. A `priced` attempt followed by a `discounted` attempt is
the chain advancing from link 1 to link 2.

**Requires an LLM provider** (`AQ_OPENAI_API_KEY` / `AQ_OPENAI_BASE_URL`,
or configure `agent:` in `aqueduct.yml`). Without one, the run reports the
first failure (`priced`) and no patch is staged, same as any self-heal
without a reachable model.
