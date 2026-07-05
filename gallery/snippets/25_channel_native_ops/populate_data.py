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
    write_csv('people.csv', [
        ['id', 'name', 'email', 'city', 'age', 'salary'],
        ['1', 'Alice Johnson', 'ALICE@EXAMPLE.COM', 'New York', '30', '75000'],
        ['2', 'Bob Smith', 'bob@example.com', 'Los Angeles', '25', '55000'],
        ['3', 'Charlie Brown', 'charlie@EXAMPLE.ORG', 'Chicago', '35', '82000'],
        ['4', 'Diana Prince', 'diana@example.net', 'Houston', '28', '62000'],
        ['5', 'Eve Williams', 'eve@example.com', 'Phoenix', '42', '95000'],
    ])

if __name__ == '__main__':
    main()
