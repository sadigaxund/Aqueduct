import csv
import os
import sys

DATA_DIR = "data/input"

BASELINE_HEADER = ["order_id", "customer", "amount", "status"]
BASELINE_ROWS = [
    ["1001", "Alice", "100.00", "paid"],
    ["1002", "Bob", "200.00", "paid"],
    ["1003", "Charlie", "300.00", "pending"],
]


def write_csv(filename, header, rows):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"Wrote {path}  columns={header}")


def main():
    scenario = sys.argv[1] if len(sys.argv) > 1 else "baseline"

    if scenario == "baseline":
        write_csv("orders_a.csv", BASELINE_HEADER, BASELINE_ROWS)
        write_csv("orders_b.csv", BASELINE_HEADER, BASELINE_ROWS)
        print("\nBaseline schema written for both sources.")
        print("Next: aqueduct drift blueprint.yml   (establishes the baseline)")
    elif scenario == "drift":
        # orders_a: BREAKING, 'status' column is dropped upstream.
        breaking_header = ["order_id", "customer", "amount"]
        breaking_rows = [row[:3] for row in BASELINE_ROWS]
        write_csv("orders_a.csv", breaking_header, breaking_rows)

        # orders_b: BENIGN, a new 'region' column is added upstream.
        benign_header = [*BASELINE_HEADER, "region"]
        regions = ["us-east", "us-west", "eu-west"]
        benign_rows = [row + [regions[i]] for i, row in enumerate(BASELINE_ROWS)]
        write_csv("orders_b.csv", benign_header, benign_rows)

        print("\nDrift applied: orders_a dropped 'status' (breaking),")
        print("orders_b added 'region' (benign).")
        print("Next: aqueduct drift blueprint.yml   (reports both)")
    else:
        raise SystemExit(f"unknown scenario {scenario!r} (use 'baseline' or 'drift')")


if __name__ == "__main__":
    main()
