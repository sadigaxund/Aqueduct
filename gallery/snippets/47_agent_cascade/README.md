# Multi-model healing cascade

Demonstrates `agent.cascade`: try a cheap model first, escalate to a
stronger one only when the cheap model gets stuck. Most heals are small
diagnoses (a typo'd column, a wrong path) that a 7B local model handles in
one shot, so a cascade avoids spending a hosted model's tokens on those.

## Setup

```bash
pip install -r requirements.txt
```

## Two blueprints

`blueprint_bugged.yml` is the deliberately-broken pipeline this demo is
built around (`enrich`'s query references a `total` column that doesn't
exist — the real column is `total_amt`); it's the file the commands below
run to trigger the cascade. `blueprint.yml` is the same pipeline already
healed; it's the file CI's snippet lane runs via `aqueduct run
blueprint.yml`, so the lane stays green with no LLM key. Both files share
the identical `agent: {approval: auto}` block, and both read the same
`aqueduct.yml` cascade config.

## How it works

`aqueduct.yml`'s `agent.cascade` is a list of tiers, cheapest first:

```yaml
agent:
  cascade:
    - model: qwen2.5-coder:7b
      provider: openai_compat
      base_url: "http://localhost:11434/v1"
      max_reprompts: 2
      max_seconds: 30
    - model: claude-sonnet-4-6
      provider: anthropic
      max_reprompts: 3
      deep_loop: true
      allow_defer: true
```

`agent.cascade` is engine-level **only**. `aqueduct/config.py`'s
`AgentConnectionConfig` carries it, and `AgentSchema` (the Blueprint-level
`agent:` block) deliberately has no `cascade` field. A Blueprint chooses
risk policy (`approval`, `guardrails`, `sandbox_mode`, and so on); it never
chooses which LLM endpoint gets called, that stays an operator decision in
`aqueduct.yml`. That's why `blueprint.yml` here only sets `agent.approval:
auto` and nothing about models.

`generate_cascade_patch` (`aqueduct/agent/cascade.py`) tries tier 0 first.
It escalates to the next tier when the result's stop reason is
`stuck_signature`, `exhausted_attempts`, or `deferred`, meaning the model
tried and gave up, not merely "this endpoint is down" (an unreachable tier
retries at the *next* tier for a different reason: `api_error`). Whichever
tier produces the patch, that tier's model and its 0-based position are
recorded on the `healing_outcomes` row for the heal.

## How to run

```bash
python populate_data.py
aqueduct run blueprint_bugged.yml
```

`enrich`'s query references a column that doesn't exist (`total`, the real
column is `total_amt`). Because `approval: auto`, the failed run
immediately triggers self-healing, and because `aqueduct.yml` declares
`agent.cascade`, the CLI reports which tiers are in play at run start:

```
cascade · 2 tier(s) · qwen2.5-coder:7b → claude-sonnet-4-6
```

```bash
python inspect_results.py
```

prints the healed output (if the pipeline ended up succeeding) and, from
`healing_outcomes`, which model and cascade tier actually produced the
winning patch.

**Requires reachable providers.** Tier 1 needs a local Ollama server at
`http://localhost:11434` serving `qwen2.5-coder:7b`; tier 2 needs
`ANTHROPIC_API_KEY` in the environment. With neither reachable, both tiers
report `api_error` and the run reports the original failure with no patch
staged. The cascade wiring is still visible in the "cascade · 2 tier(s)"
line and in each attempt's tier index, even without a live model.
