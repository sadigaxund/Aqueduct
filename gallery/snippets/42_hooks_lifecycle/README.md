# Blueprint Lifecycle Hooks (`hooks:`)

Demonstrates `hooks.on_success` and `hooks.on_failure` — sequential actions
fired after the run's terminal state.

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
    - webhook: "http://localhost:9999/hooks/success"
  on_failure:
    - command: mkdir -p data/output
    - command: touch data/output/hook_signal_failed.txt
    - webhook: "http://localhost:9999/hooks/failure"
```

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
