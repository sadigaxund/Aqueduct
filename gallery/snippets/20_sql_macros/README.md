# SQL Macros Snippet

Demonstrates **SQL Macros**: reusable SQL fragments resolved at **compile time**.
The Manifest the executor runs is plain SQL — no macro syntax ever reaches Spark.

## Key Concept

Two macros are declared at the top of `blueprint.yml`:

- `active` — a plain fragment: `status = 'active' AND deleted_at IS NULL`
- `trunc` — a **parameterized** fragment using `{{ period }}` / `{{ col }}`
  placeholders, called as `{{ macros.trunc(period='day', col=order_ts) }}`

The `recent_active` Channel composes both in one `WHERE` clause. At compile time
they expand inline, so the same predicate logic stays defined once and reused.

## How to Run

1. **Execute the Pipeline**:
   ```bash
   aqueduct run blueprint.yml
   ```

2. **Inspect Results**:
   ```bash
   python inspect_results.py
   ```

## Expected Result

| order_id | reason kept |
| :--- | :--- |
| 1 | active, not deleted, 2026-01-02 |
| 5 | active, not deleted, 2026-01-05 |

Excluded: `2` (before 2026), `3` (not active), `4` (`deleted_at` set).
