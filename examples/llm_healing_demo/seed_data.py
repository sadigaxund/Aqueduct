"""Write reviews.json with the NEW schema (ReviewDate, not review_date).

This simulates an upstream schema drift — the data producer renamed the field
without telling us. The pipeline still references the old name.
"""
import json
from pathlib import Path

records = [
    {"product_id": "P-001", "rating": 5, "ReviewDate": "2024-01-15", "comment": "Excellent"},
    {"product_id": "P-002", "rating": 3, "ReviewDate": "2024-01-16", "comment": "Average"},
    {"product_id": "P-003", "rating": 1, "ReviewDate": "2024-01-17", "comment": "Terrible"},
    {"product_id": "P-004", "rating": 4, "ReviewDate": "2024-01-18", "comment": "Good"},
    {"product_id": "P-005", "rating": 2, "ReviewDate": "2024-01-19", "comment": "Below average"},
    {"product_id": "P-006", "rating": 5, "ReviewDate": "2024-01-20", "comment": "Amazing"},
]

out = Path("/tmp/aq_demo/reviews.json")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
print(f"Wrote {len(records)} records to {out}")
print("Fields:", list(records[0].keys()))
print("Note: field is 'ReviewDate' (capital R, capital D) — pipeline references 'review_date'")
