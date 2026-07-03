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
    write_csv('events.csv', [
        ['event_id', 'event_type', 'user_id', 'amount', 'created_at'],
        ['1', 'purchase', '1001', '50.00', '2025-06-01'],
        ['2', 'purchase', '1002', '75.00', '2025-06-02'],
        ['3', 'refund', '1001', '20.00', '2025-06-05'],
        ['4', 'purchase', '1003', '100.00', '2025-06-10'],
        ['5', 'purchase', '1001', '30.00', '2025-06-15'],
        ['6', 'refund', '1004', '10.00', '2025-06-20'],
    ])

if __name__ == '__main__':
    main()
