import os
import csv

os.makedirs("data/input", exist_ok=True)

rows = [
    ["region", "product", "amount"],
    ["US", "widget", "120"],
    ["US", "gadget", "80"],
    ["EU", "widget", "90"],
    ["EU", "gadget", "60"],
    ["APAC", "widget", "50"],
]

with open("data/input/sales.csv", "w", newline="") as f:
    csv.writer(f).writerows(rows)

print("Created data/input/sales.csv")
