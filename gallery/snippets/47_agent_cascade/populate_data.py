import csv
import os

DATA_DIR = "data/input"


def write_csv(filename, rows):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    print(f"Created {path} ({len(rows) - 1} data rows)")


def main():
    write_csv(
        "orders.csv",
        [
            ["order_id", "customer", "amount", "total_amt"],
            ["1001", "Alice", "100.00", "100.00"],
            ["1002", "Bob", "200.00", "200.00"],
            ["1003", "Charlie", "300.00", "300.00"],
        ],
    )


if __name__ == "__main__":
    main()
