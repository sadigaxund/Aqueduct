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
    write_csv('fallback.csv', [
        ['user_id', 'email'],
        ['1', 'user1@fallback.com'],
        ['2', 'user2@fallback.com'],
        ['3', 'user3@fallback.com'],
    ])

    write_csv('primary.csv', [
        ['user_id', 'email'],
        ['1', 'user1@primary.com'],
        ['2', ''],
        ['3', 'user3@primary.com'],
    ])

if __name__ == '__main__':
    main()
