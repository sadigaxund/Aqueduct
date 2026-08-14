import csv, os

DATA_DIR = "data"


def write_csv(filename, rows):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    print(f"Created {path} ({len(rows)-1} data rows)")


def main():
    write_csv(
        "nodes.csv",
        [
            ["id", "value"],
            ["1", "alpha"],
            ["2", "beta"],
            ["3", "gamma"],
        ],
    )


if __name__ == "__main__":
    main()
