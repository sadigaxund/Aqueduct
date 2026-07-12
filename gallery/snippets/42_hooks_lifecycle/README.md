# Blueprint Lifecycle Hooks (`hooks:`)

Demonstrates `hooks.on_success` and `hooks.on_failure` — sequential actions
fired after the run's terminal state — plus two Hooks-v2 additions:
`when_error:` filtering and `in_process: true` blueprint chaining.

## How to Run

```bash
# The webhook endpoint won't exist (localhost:9999), but command: will write:
aqueduct run blueprint.yml
cat data/output/hook_signal.txt  # → 'ok'

# Hooks never change the exit code — a failing webhook just warns and skips
# the remaining hooks of that event.
```

## Hook entry types

Each hook entry sets exactly one of three action types:

| Type | Syntax | Requires |
|------|--------|----------|
| `blueprint:` | `blueprint: path/to/other.yml` | Cycle guard (depth ≤ 8, `AQUEDUCT_HOOK_CHAIN` env) |
| `webhook:` | `webhook: "https://..."` or map with url/method/headers/payload | None (same engine as engine-level `webhooks:` block) |
| `command:` | `command: "touch /tmp/signal"` (shlex argv, no shell) | `danger.allow_command_hooks: true` in aqueduct.yml |

```yaml
hooks:
  on_success:
    - command: mkdir -p data/output
    - command: touch data/output/hook_signal_ok.txt
    - blueprint: chain_target.yml
      timeout: 60
    - blueprint: chain_target.yml
      in_process: true   # reuse the live SparkSession, no subprocess spawn
      timeout: 60
    - webhook: "http://localhost:9999/hooks/success"
  on_failure:
    - command: mkdir -p data/output
    - command: touch data/output/hook_signal_failed.txt
    - command: touch data/output/hook_signal_empty_dataset.txt
      when_error: ["EmptyDataset"]   # only fires when error_type matches
    - webhook: "http://localhost:9999/hooks/failure"
```

## Hooks-v2: `when_error:` and `in_process:`

- **`when_error:`** — optional, on `on_failure` / `on_patch_pending` /
  `on_healed` entries only (they carry a failure context; `on_success` does
  not — setting it there is a schema error at parse time). A list of
  error-type labels matched against `FailureContext.error_type` or the
  exception class name, same exact-match semantics as
  `agent.guardrails.heal_on_errors`. Unset = fires unconditionally. A
  non-matching entry is silently skipped (not a `[hook_failed]`).
- **`in_process: true`** — `blueprint:` entries only. Parses, compiles, and
  executes the target Blueprint in this same process, reusing the caller's
  live SparkSession, instead of spawning a fresh `aqueduct run` subprocess.
  No self-healing loop for the chained target — a failure is still just a
  `[hook_failed]` warning. Falls back to the subprocess path (with an info
  message) when the target Blueprint sets its own `spark_config`.

## `on_patch_pending` / `on_healed` (mid-run heal milestones)

Two more events beyond `on_success`/`on_failure`, not shown in this
snippet's `blueprint.yml` (see `docs/specs.md` §Hooks for the full
example): `on_patch_pending` fires every time a heal stages a patch for
human/CI review; `on_healed` fires once a heal's re-run succeeds — patch
applied AND the pipeline green again — and always runs BEFORE the outer
run's terminal `on_success` hooks. Both accept the same three action types
plus `when_error:` filtering.

## Behaviour rules

- Hooks fire **after** the pipeline reaches its terminal state.
- A failing hook emits a `[hook_failed]` warning and **skips** the remaining
  hooks of that event (the other event's hooks are unaffected).
- Hooks **never change the run's exit code** — the pipeline's result stands.
- `blueprint:` hooks are cycle-guarded (`AQUEDUCT_HOOK_CHAIN` env var with
  depth cap of 8).
- `command:` hooks run a subprocess via `shlex.split()` — **no shell**.
  Environment interpolation (`${run.id}`, `${run.status}`, `${blueprint.id}`)
  is supported.
- The engine-level `webhooks:` block in aqueduct.yml (ops-owned alerting)
  fires independently of blueprint hooks.
- `aqueduct doctor` statically walks `blueprint:` chains for cycles and
  missing targets.
