# LLM Self-Healing Demo

Demonstrates the full self-healing loop:
**blueprint fails → LLM diagnoses → patch written → you inspect → apply → success**

The planted bug is a SQL typo (`FORM` instead of `FROM`) in the Channel.
Spark's error message includes the bad token, the correct SQL is visible in the
failure report, so the LLM can derive the fix without any guesswork.

---

## Step 1 — Seed input data

```bash
cd examples/llm_healing_demo
python seed_data.py
```

Creates `/tmp/aq_demo/orders` (10 rows of parquet, 1 null amount).

---

## Step 2 — Run the broken blueprint

```bash
export AQ_OLLAMA_URL=http://10.0.0.39:11434
aqueduct run blueprint.yml --config aqueduct.yml
```

**What you see:**
- Pipeline fails on `src` module (wrong path)
- Surveyor detects failure, builds FailureContext
- LLM is called (you see `[aqueduct llm] gemma3:12b thinking.....` on stderr)
- Patch written to `patches/pending/<patch_id>.json`

---

## Step 3 — Inspect the original blueprint

```bash
cat blueprint.yml
# Look at the src module path: /tmp/aq_demo/WRONG_PATH_orders
```

---

## Step 4 — Inspect the LLM patch

```bash
ls patches/pending/
cat patches/pending/<patch_id>.json
```

You'll see something like:
```json
{
  "patch_id": "fix-sql-typo-clean",
  "rationale": "SQL uses 'FORM' instead of 'FROM'; correcting keyword typo",
  "operations": [
    {
      "op": "replace_module_config",
      "module_id": "clean",
      "config": {
        "sql": "SELECT order_id, region, amount, status\nFROM __input__\nWHERE amount IS NOT NULL\n"
      }
    }
  ]
}
```

---

## Step 5 — Apply the patch

```bash
aqueduct patch apply patches/pending/<patch_id>.json --blueprint blueprint.yml
```

What happens:
1. Operations applied to deep copy of blueprint
2. Patched copy re-parsed (schema validation)
3. Backup of original written to `patches/backups/`
4. Blueprint overwritten atomically
5. Patch archived to `patches/applied/`

---

## Step 6 — Verify the blueprint changed

```bash
cat blueprint.yml
# src.config.path should now be /tmp/aq_demo/orders (or whatever the LLM chose)
```

---

## Step 7 — Re-run and confirm success

```bash
aqueduct run blueprint.yml --config aqueduct.yml
```

Should complete with 9 rows written to `/tmp/aq_demo/clean_orders`
(1 row filtered by the Channel — the null amount row).

```bash
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
spark.read.parquet('/tmp/aq_demo/clean_orders').show()
spark.stop()
"
```

---

## Patch lifecycle directories

```
patches/
  pending/    ← LLM patches waiting for human approval (approval_mode: human)
  applied/    ← Patches applied to the blueprint (auto or manual)
  backups/    ← Original blueprint snapshots before each patch
  rejected/   ← Patches you rejected via: aqueduct patch reject <patch_id> "reason"
```

---

## Switching to auto-apply

Change `approval_mode: human` → `approval_mode: auto` in blueprint.yml.  
The LLM patch will be applied immediately without Step 5. Steps 3 and 4 still
visible via `patches/applied/` and `patches/backups/`.

## Disabling LLM healing entirely

Remove the `agent:` block from the blueprint, or the feature is not triggered.
