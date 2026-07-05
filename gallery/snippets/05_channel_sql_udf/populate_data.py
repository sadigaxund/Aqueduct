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
        ['name', 'email', 'phone'],
        ['John Doe', 'john.doe@example.com', '555-123-4567'],
        ['Jane Smith', 'jane.smith@gmail.com', '555-234-5678'],
        ['Alice Wong', 'alice.w@outlook.com', '555-345-6789'],
        ['Bob Builder', 'bob@builder.com', '555-456-7890'],
        ['Charlie Brown', 'cb@peanuts.org', '555-567-8901'],
    ])

if __name__ == '__main__':
    main()
