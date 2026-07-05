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
        ['id', 'event_ts', 'value'],
        ['1', '2026-01-01 09:00:00', '10'],
        ['2', '2026-01-01 10:30:00', '20'],
        ['3', '2026-01-02 08:15:00', '30'],
        ['4', '2026-01-02 12:45:00', '40'],
        ['5', '2026-01-03 07:05:00', '50'],
    ])

if __name__ == '__main__':
    main()
