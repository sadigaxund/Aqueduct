# Context Registry — Tier 0 Static Resolution

Demonstrates `${ctx.*}` — Aqueduct's static variable system, resolved at parse
time before Spark ever sees the Blueprint.

## Setup

```bash
pip install -r requirements.txt
```

## How it works

The `context:` block defines reusable values substituted into any string
position in the Blueprint:

```yaml
context:
  env: ${AQUEDUCT_ENV:-dev}
  paths:
    input: "data/input/products.csv"
    output: "data/output/${ctx.env}/products.parquet"
```

`${ctx.env}` becomes the literal `"dev"` or `"US"` before Spark compiles.
Resolution priority (highest first):

| Priority | Source | Example |
|----------|--------|---------|
| 1 | CLI flag | `--ctx env=prod` |
| 2 | Env var | `AQUEDUCT_CTX_ENV=prod` |
| 3 | Profile | `--profile prod` → `context_profiles.prod.*` |
| 4 | Default | `context.env: dev` |

## Try it

```bash
# Default "dev" — filter is "region = 'dev'" → zero rows match
python populate_data.py

aqueduct run blueprint.yml

# Override env — filter becomes "region = 'US'" → 3 rows
aqueduct run blueprint.yml --ctx env=US

# Via env var
AQUEDUCT_CTX_ENV=APAC aqueduct run blueprint.yml

# Profile — reads a smaller dataset, writes to different output
aqueduct run blueprint.yml --profile prod

# Profile + CLI override (CLI wins)
aqueduct run blueprint.yml --profile prod --ctx env=EU
```

## What the `prod` profile changes

| Field | Default (dev) | `--profile prod` |
|-------|---------------|-------------------|
| Input | `products.csv` (5 rows, mixed regions) | `products_prod.csv` (2 rows, US only) |
| Output | `data/output/dev/products.parquet` | `data/output/prod/products.parquet` |
| `env` | `dev` (filter matches nothing) | `US` (filter matches 2 rows) |

Both paths are local — no S3, no Docker, no setup required.

## Resolution order test

```bash
# Profile overrides context default, CLI overrides profile:
aqueduct run blueprint.yml --profile prod --ctx env=EU
```

Context values appear in logs and the observability store as resolved
literals — the original `${ctx.*}` expression is not leaked downstream.

> **Explicit `edges:` declaration:** This blueprint declares data-flow paths
> under `edges:` rather than relying on auto-wiring. Required for modules
> with multiple inputs/outputs (Funnels, Junctions, spillways). See
> `docs/specs.md` §3.4.
