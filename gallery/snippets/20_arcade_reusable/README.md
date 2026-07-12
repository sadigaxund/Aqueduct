# Reusable Arcade Snippet

Demonstrates the **Arcade**: a reusable sub-Blueprint, namespaced and inlined at
compile time. Write a pipeline fragment once, instantiate it many times with
different inputs.

## Setup

```bash
pip install -r requirements.txt
```

## Key Concept

`arcades/region_filter.yml` is a self-contained mini-pipeline that declares
`required_context: [region, src_path]`. The parent `blueprint.yml` references it
**twice** as `type: Arcade` modules, each passing different values via
`context_override`:

- `us_sales` → `region: US`
- `eu_sales` → `region: EU`

At compile time the Arcade is expanded: its modules are flattened and namespaced
(`us_sales__ingress`, `us_sales__filtered`, …), so the Manifest the engine runs
contains no Arcade indirection. A `Funnel` then unions both instances.

Missing a `required_context` key in `context_override` fails compilation fast —
try deleting `region:` from one instance and run `aqueduct validate blueprint.yml`.

## How to Run

1. **Generate test data**:
   ```bash
   python populate_data.py
   ```

2. **Execute the Pipeline**:
   ```bash
   aqueduct run blueprint.yml
   ```

3. **Inspect Results**:
   ```bash
   python inspect_results.py
   ```

## Expected Result

| region | product | amount |
| :--- | :--- | :--- |
| US | widget | 120 |
| US | gadget | 80 |
| EU | widget | 90 |
| EU | gadget | 60 |

`APAC` rows are dropped — neither Arcade instance was configured for that region.
