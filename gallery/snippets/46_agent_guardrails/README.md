# Agent guardrails

Demonstrates `agent.guardrails`, the deterministic policy layer that
constrains what a patch (LLM-authored or hand-authored) is allowed to do to
a Blueprint. Enforcement happens in `_check_guardrails` at apply time
(`aqueduct/patch/apply.py`), so it applies identically whether the patch
came from a real self-heal or, as here, a plain PatchSpec JSON file. No
LLM call is required to see the effect.

## Setup

```bash
pip install -r requirements.txt
```

## Two blueprints

`blueprint_bugged.yml` is the deliberately-broken pipeline this demo is
built around (`enrich`'s query references a `total` column that doesn't
exist — the real column is `total_amt`) — that's the file the commands
below patch. `blueprint.yml` is the same pipeline already healed (`total`
corrected to `total_amt`); it's the file CI's snippet lane runs via
`aqueduct run blueprint.yml`, so the lane stays green with no LLM key. Both
files carry the identical `agent.guardrails` block.

## How it works

`blueprint_bugged.yml`'s (and `blueprint.yml`'s) `agent.guardrails` block declares:

- `forbidden_ops: [insert_module, remove_module]`: these PatchSpec
  operation types are blocked outright, whatever they would do.
- `allowed_paths: ["data/output/*"]`: an fnmatch pattern checked against
  the **resolved value** of any `path`/`output_path` config key a patch
  writes. This is a string match against what the Blueprint would end up
  saying, not a filesystem check, so a patch that tries to point Egress at
  `/etc/aqueduct_exfil.parquet` never gets that far.
- `heal_on_errors: [AnalysisException]` / `never_heal_errors:
  [PermissionError]`: pre-trigger guards checked before the LLM is even
  called (`aqueduct/cli/__init__.py::_check_heal_guardrails`). Healing only
  fires on a Spark analysis failure, never on a permission error, no matter
  what `approval` mode is set.

`sample_patches/` has three hand-authored PatchSpec files that exercise the
first two guardrails directly:

| File | What it tries | Result |
|------|----------------|--------|
| `01_forbidden_op.json` | `remove_module` on `output` | rejected, `remove_module` is in `forbidden_ops` |
| `02_disallowed_path.json` | sets `output.path` to `/etc/aqueduct_exfil.parquet` | rejected, doesn't match `data/output/*` |
| `03_valid_fix.json` | sets `enrich.query` to the corrected SQL | applied: fixes the deliberate bug (`total` becomes `total_amt`), touches neither a forbidden op nor a `path`/`output_path` key |

## How to run

```bash
python populate_data.py

aqueduct patch apply sample_patches/01_forbidden_op.json --blueprint blueprint_bugged.yml
# ✗ patch failed: Operation 'remove_module' is forbidden by agent.guardrails.forbidden_ops...

aqueduct patch apply sample_patches/02_disallowed_path.json --blueprint blueprint_bugged.yml
# ✗ patch failed: Path value '/etc/aqueduct_exfil.parquet' (key='path') in op
#   'set_module_config_key' (module 'output') does not match any
#   agent.guardrails.allowed_paths pattern: ['data/output/*']

aqueduct patch apply sample_patches/03_valid_fix.json --blueprint blueprint_bugged.yml
aqueduct patch commit --blueprint blueprint_bugged.yml

python inspect_results.py
```

`inspect_results.py` reads `blueprint.yml` (the already-healed file), not
`blueprint_bugged.yml` — it always reports the fix as applied. Point it at
`blueprint_bugged.yml` yourself (`cp blueprint_bugged.yml blueprint.yml`,
or edit the hardcoded filename) to see it flag the still-broken query
before you run the commands above.

The first two commands leave `blueprint_bugged.yml` untouched: the
guardrail check runs before any operation is applied to the working copy,
and `01_forbidden_op.json`/`02_disallowed_path.json` are left in place
since they were never applied. The third command applies and archives
normally; `aqueduct patch apply` treats a local patch file as a pending
patch, so `sample_patches/03_valid_fix.json` is moved into
`patches/applied/` and removed from `sample_patches/` once it succeeds.
Restore it from git (or re-run `git checkout -- sample_patches/03_valid_fix.json`)
to repeat the demo. `patch commit` writes the applied change to
`blueprint_bugged.yml`, leaving it identical to the already-healed
`blueprint.yml`.

`aqueduct run blueprint_bugged.yml` (with an LLM configured and `approval:
human` or `auto`) would produce a real LLM patch here instead of
`03_valid_fix.json`. It passes through the exact same `_check_guardrails`
gate.
