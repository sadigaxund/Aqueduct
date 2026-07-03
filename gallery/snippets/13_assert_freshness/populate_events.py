"""Generate events CSV with dynamic timestamps relative to now."""
import os
from datetime import datetime, timedelta

now = datetime.now()
fmt = "%Y-%m-%d %H:%M:%S"

os.makedirs("data/input", exist_ok=True)

rows = [
    ("id", "status", "processed_at"),
    (1, "FRESH", now.strftime(fmt)),
    (2, "FRESH", (now - timedelta(hours=1)).strftime(fmt)),
    (3, "STALE", (now - timedelta(hours=18)).strftime(fmt)),
    (4, "STALE", (now - timedelta(hours=48)).strftime(fmt)),
]

csv_path = "data/input/events.csv"
with open(csv_path, "w") as f:
    for row in rows:
        f.write(",".join(str(v) for v in row) + "\n")

print(f"Generated {csv_path}")
print(f"  Row 1-2: within 12h (fresh)")
print(f"  Row 3:   18h old (stale > 12h max_age_hours)")
print(f"  Row 4:   48h old (stale > 12h max_age_hours)")
