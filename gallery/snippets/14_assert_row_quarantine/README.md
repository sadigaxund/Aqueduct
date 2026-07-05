  # Row-Level Quarantine Assertion

Demonstrates how to use the `Assert` module's built-in quarantine feature to safely route "bad" records away from your main pipeline.

## Key Concept: Quarantine
Unlike a simple `filter` which silently drops data, or a standard `Assert` which aborts the entire pipeline, the **Quarantine** pattern allows you to:
1. Define a row-level rule (e.g., `amount > 0`).
2. Keep rows that pass in the `main` stream.
3. Automatically divert rows that fail to the `spillway` port.
4. Enforce quality without halting the business process.

## How to Run

1. **Execute the Pipeline**:
   ```bash
   aqueduct run blueprint.yml
   ```

2. **Inspect Results**:
   ```bash
   python inspect_results.py
   ```

## Configuration
In `blueprint.yml`:
```yaml
- id: valid_amount
  type: sql_row
  expr: "amount IS NOT NULL AND amount > 0"
  on_fail: quarantine
```

Note how the `edges` configuration routes the `spillway` port of the validator to a separate Egress module.
```yaml
- from: validate_orders
  to: rejected_orders
  port: spillway
```
