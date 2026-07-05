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
    write_csv('sample.csv', [
        ['id', 'name', 'amount', 'email'],
        ['1', 'Alice', '100.00', 'alice@example.com'],
        ['2', 'Bob', '', 'bob@example.com'],
        ['3', 'Charlie', '25000.00', 'charlie@example.com'],
        ['4', 'Diana', '50.00', 'diana@example.com'],
        ['5', 'Eve', '-5.00', 'eve@example.com'],
        ['6', 'Frank', '200.00', 'frank@example.com'],
    ])

if __name__ == '__main__':
    main()
