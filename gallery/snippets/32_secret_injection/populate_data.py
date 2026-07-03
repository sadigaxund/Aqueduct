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
        ['user_id', 'name', 'email'],
        ['1', 'Alice Johnson', 'alice@example.com'],
        ['2', 'Bob Smith', 'bob@example.com'],
        ['3', 'Charlie Brown', 'charlie@example.org'],
    ])

if __name__ == '__main__':
    main()
