import os
import csv

os.makedirs("data/input", exist_ok=True)

rows = [
    ["region", "product", "revenue"],
    ["US", "Widget A", "1200"],
    ["US", "Widget B", "850"],
    ["EU", "Widget A", "970"],
    ["EU", "Widget C", "1100"],
]

with open("data/input/sales.csv", "w", newline="") as f:
    csv.writer(f).writerows(rows)

print("Created data/input/sales.csv")
