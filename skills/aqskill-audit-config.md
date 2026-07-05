---
name: aqskill-audit-config
description: Audit Aqueduct schema/template sync, path anchoring, silent no-ops, and danger-config coverage. Use after adding/removing config fields, changing key names, or touching templates. Triggers include "audit config sync", "check templates", "verify danger settings", "find dead config".
---

# Aqueduct Config/Schema Audit

Extends `aqskill-audit-health.md` with Aqueduct-specific config patterns. All prevention rules live in AGENTS.md — this skill provides detection, not rules.

## ⚠️ Verify before you report (precision gate — read first)

`rg` produces **candidates, not findings**. Promote one ONLY after:
1. **Read the cited `file:line` + context.**
2. **Check the repo neutralizers below** — drop if one applies.
3. **Write a concrete failure scenario** ("user writes X in their YAML → Y happens / field silently ignored"). No scenario → SUSPECTED.
4. **Confirm it's still true at HEAD.**

Tag each row **PROVEN**/**SUSPECTED**.

## Repo neutralizers — false-positive guards (check before flagging)

- **Phase/Sprint artifacts are ALLOWED in some places.** The stale-language greps below hit `# Phase NN`/`Sprint`/`Task` comments. Per AGENTS.md these are **allowed** in source-code comments/docstrings (`aqueduct/**/*.py`), `CHANGELOG.md`, and `TODOs.md` — they are only forbidden on **user-facing surfaces** (`docs/**`, `aqueduct/templates/**`, `gallery/**`, `README.md`, `CONTRIBUTING.md`). Only flag hits on user-facing surfaces.
- **Zero-consumer needs a FULL-tree search.** Before calling a field a silent no-op, grep its attribute across **all** of `aqueduct/` (cli, compiler, agent, executor, surveyor, stores) — not one subdir. Consumers are often in a different layer. One subdir returning nothing is NOT proof.
- **Pydantic validator.** `raise ValueError` inside a validator is required; not a config bug.
- **Template intentional omissions.** Some config blocks are deliberately not templated (advanced/rare). A field absent from a template is a finding only if the field is commonly needed AND the AGENTS change-trigger matrix requires it. Check the matrix before flagging "missing from template."
- **Already current.** Re-read the template/schema at HEAD — sync fixes may have landed.

## Schema → template sync

Every pydantic field must have a commented example in the corresponding template.

```bash
# Find all pydantic fields in engine config
rg -n ': .* = Field\(' aqueduct/config.py

# Find all pydantic fields in Blueprint schema
rg -n ': .* = Field\(' aqueduct/parser/schema.py

# For each field, verify it appears in the template:
# aqueduct.yml.template for config.py fields
# blueprint.yml.template for schema.py fields
#
# Manual cross-reference — grep the field's config key (not Python attr) in templates/
```

```bash
# Also check: template shows wrong key name (singular/plural mismatch, old alias)
rg -n 'depot:' aqueduct/templates/ | rg -v 'depots'
rg -n 'approval_mode' aqueduct/templates/ docs/ SKILL.md
rg -n 'stores\.lineage' aqueduct/templates/ | rg -v 'test_'
```

## Danger config coverage

Every `DangerConfig` field must have a startup warning and doc coverage.

```bash
# List all DangerConfig fields
rg -n ': bool = Field\(' aqueduct/config.py -A 5 | grep -A 2 'allow_'

# Verify each has a warning in cli/run.py (danger startup section)
rg -n 'cfg\.danger\.' aqueduct/cli/run.py

# Verify production_guide.md danger section covers all fields
rg -n 'allow_' docs/production_guide.md
```

## Path anchoring

Every `Annotated[str, FsPath()]` field must resolve relative to the correct base directory.

```bash
# Find all FsPath fields
rg -n 'FsPath\(\)' aqueduct/

# For each: trace to ensure parse calls pass base_dir=
rg -n 'parse_dict\(|parse\(' aqueduct/ --glob '!test_*'

# Find tempfile detours (parse → write to temp → re-parse from different dir)
rg -n 'NamedTemporaryFile|mkstemp|mkdtemp' aqueduct/ | rg -v 'test_'
```

## Silent no-ops — dead config fields

Config fields accepted but never consumed silently lie to users.

```bash
# Manual: for each pydantic Field in DangerConfig, ProbesConfig, StoresConfig, AgentConfig:
# 1. Note the config key (Python attribute name)
# 2. Search for ALL consumers of that attribute
# 3. Flag any with zero consumers

# Example: does probes.max_sample_rows actually cap samples?
rg -n 'max_sample_rows' aqueduct/executor/

# Does probes.default_sample_fraction actually set defaults?
rg -n 'default_sample_fraction' aqueduct/executor/
```

## Store-config consistency

```bash
# Verify store backends match documented options
rg -n 'backend.*Literal' aqueduct/config.py

# Check template vs code: same backends in both?
rg -n 'duckdb|postgres|redis|s3|gcs|adls' aqueduct/templates/default/aqueduct.yml.template

# Verify observability_guide.md store table matches config
rg -n 'duckdb|postgres|redis' docs/observability_guide.md
```

## Stale-artifact detection

After removing config keys/values, find leftover references in non-test, non-archive files.

```bash
# General stale language
rg -n 'deprecated|legacy|removed in|formerly|used to be|was replaced' \
  --glob '!{docs/archive/**,tests/**,__pycache__/**}' aqueduct/ docs/ SKILL.md gallery/

# Specific: stale alias references
rg -n 'aggressive|approval_mode|allow_aggressive|aggressive_max_patches' \
  --glob '!{docs/archive/**,tests/**,__pycache__/**}' aqueduct/ docs/ SKILL.md

# Specific: stale store references
rg -n 'stores\.depot[^s]|stores\.lineage' \
  --glob '!{docs/archive/**,tests/**,__pycache__/**}' aqueduct/ docs/

# Specific: engine: flink references (should be roadmap-only)
rg -n 'engine.*flink|flink.*engine' \
  --glob '!{docs/archive/**,docs/roadmap.md,tests/**,__pycache__/**}' aqueduct/ docs/
```

## Output format

```markdown
## Config audit — [date]

Every row MUST carry a **verdict (PROVEN/SUSPECTED)** and a **failure scenario**.
Only rows that survived the verify gate.

### Schema → template mismatches
| field (file:line) | template | status (missing/wrong) | verdict | failure scenario |

### Danger config gaps
| field | warning? | doc? |

### Path anchoring risks
| field (file:line) | parse path | risk |

### Silent no-ops
| field (file:line) | consumers found | severity |

### Stale artifacts
| file:line | stale text | suggested replacement |
```
