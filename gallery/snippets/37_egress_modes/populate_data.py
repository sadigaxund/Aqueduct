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
        ['ts', 'event', 'user'],
        ['2024-01-01T10:00:00', 'page_view', 'alice'],
        ['2024-01-01T10:05:00', 'click', 'bob'],
        ['2024-01-01T10:10:00', 'page_view', 'carol'],
    ])

if __name__ == '__main__':
    main()
