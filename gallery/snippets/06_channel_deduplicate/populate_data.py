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
    write_csv('users.csv', [
        ['user_id', 'updated_at', 'created_at', 'data'],
        ['1', '2024-01-01 10:00:00', '2024-01-01 10:00:00', 'initial'],
        ['1', '2024-01-01 11:00:00', '2024-01-01 10:00:00', 'updated'],
        ['2', '2024-01-02 09:00:00', '2024-01-02 09:00:00', 'single'],
        ['3', '2024-01-03 12:00:00', '2024-01-03 12:00:00', 'first'],
        ['3', '2024-01-03 13:00:00', '2024-01-03 12:00:00', 'second'],
        ['3', '2024-01-03 14:00:00', '2024-01-03 12:00:00', 'third'],
    ])

if __name__ == '__main__':
    main()
