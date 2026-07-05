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
    write_csv('orders.csv', [
        ['order_id', 'status', 'deleted_at', 'order_ts', 'amount'],
        ['1', 'active', '', '2026-01-02 09:00:00', '100'],
        ['2', 'active', '', '2025-12-30 10:00:00', '80'],
        ['3', 'inactive', '', '2026-01-03 11:00:00', '50'],
        ['4', 'active', '2026-01-04 12:00:00', '2026-01-04 12:00:00', '70'],
        ['5', 'active', '', '2026-01-05 08:30:00', '120'],
    ])

if __name__ == '__main__':
    main()
