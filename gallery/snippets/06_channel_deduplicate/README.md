# Deduplication Snippet (Keep Latest)

Demonstrates the common ETL pattern of deduplicating records by a unique ID while keeping the most recent entry based on a timestamp.

## Setup

```bash
pip install -r requirements.txt
```

## Key Concept: Window Functions
This snippet uses the **SQL Window Function** `ROW_NUMBER()` to:
1. **Partition** the data by `user_id`.
2. **Order** each group by `updated_at DESC`.
3. **Filter** to keep only the row where the rank is `1`.

This is significantly more robust than a simple `DISTINCT` or `GROUP BY`, as it allows you to maintain the full record state of the most recent version.

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

## Comparison

| User ID | Input Versions | Output Result |
| :--- | :--- | :--- |
| 1 | 2 records | Only the 11:00 update |
| 2 | 1 record | Preserved |
| 3 | 3 records | Only the 14:00 update |
