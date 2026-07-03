import csv, os

DATA_DIR = "data/input"

def write_csv(filename, rows):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    print(f"Created {path} ({len(rows)-1} data rows)")

def main():
    write_csv("orders.csv", [
        ["order_id", "customer", "amount"],
        ["101", "Alice", "100.0"],
        ["102", "Bob", "200.0"],
        ["103", "Charlie", "150.0"],
    ])
    write_csv("updates.csv", [
        ["order_id", "customer", "amount"],
        ["101", "Alice", "110.0"],
        ["104", "Diana", "250.0"],
    ])

if __name__ == "__main__":
    main()
