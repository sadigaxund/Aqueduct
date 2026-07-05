# Conditional Module Execution (`enabled:`)

Demonstrates the `enabled:` module attribute. Every module accepts `enabled:`
(default `true`) — a bool or a `${ctx.*}`/`${ENV}` expression, so a context
profile can toggle whole pipeline sections.

## How to Run

```bash
# Default — premium_enrich is disabled (AQ_PREMIUM=false):
aqueduct run blueprint.yml

# Enable premium branch:
AQ_PREMIUM=true aqueduct run blueprint.yml
```

## What happens when a module is disabled

1. The disabled module compiles but is **skipped** (shown as `⏭` in the
   run summary with the reason).
2. The disable **cascades at compile time** to every consumer of its output
   — via edges, `depends_on`, or Probe `attach_to` — transitively and
   uniformly.
3. A join/union missing one input never runs partially — the entire
   consumer chain is skipped.
4. Disabled modules are excluded from compile-time warnings.
5. Disabling every module is a `CompileError`.

## Blueprint structure

Two parallel Channel modules read from the same Ingress:

| Module | `enabled` | What it does |
|--------|-----------|-------------|
| `basic_enrich` | `true` (default) | Adds tier=basic, tax column |
| `premium_enrich` | `${ctx.premium}` | Only when AQ_PREMIUM=true adds tier=premium, segment column |

Both feed into a Funnel `union_all`, then to an Egress. When `premium_enrich`
is disabled, the Funnel and Egress are also skipped (transitive cascade).

```yaml
modules:
  - id: premium_enrich
    type: Channel
    enabled: "${ctx.premium}"
    config:
      op: sql
      query: |
        SELECT *, 'premium' AS tier, amount * 0.05 AS tax
        FROM __input__
```
