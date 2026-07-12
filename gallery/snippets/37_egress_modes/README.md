# Egress — 5 Write Modes

Compares all 5 supported write modes from the same input data.

## Setup

```bash
pip install -r requirements.txt
```

## Modes

| Mode | First run | Second run |
|------|-----------|------------|
| **append** | Creates target, 3 rows | Adds 3 more rows (6 total) |
| **error** | Creates target, 3 rows | **Fails** — target exists |
| **ignore** | Creates target, 3 rows | Skips silently — still 3 rows |
| **overwrite_partitions** + `partition_by` | Creates partitioned directory | Replaces files in matching `category=` dirs |
| **overwrite_partitions** + `replace_where` | Creates target with `replaceWhere` | Replaces rows matching `event = 'page_view'` |

## Run twice to see the difference

```bash
# First run — all 5 modes create their targets
python populate_data.py

aqueduct run blueprint.yml
python inspect_results.py

# Second run — append grows, error fails, ignore skips,
# overwrite_partitions replaces selective files
aqueduct run blueprint.yml
python inspect_results.py
```

`mode: error` will abort the whole pipeline on the second run.
Comment out the `save_error` block to run the other 4 modes
side by side on repeated runs:

```yaml
# - id: save_error
#   type: Egress
#   config:
#     mode: error
```
