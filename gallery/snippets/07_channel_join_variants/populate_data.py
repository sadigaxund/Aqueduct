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
        ['order_id', 'user_id', 'amount'],
        ['101', '1', '50.0'],
        ['102', '2', '75.0'],
        ['103', '1', '20.0'],
        ['104', '4', '100.0'],
    ])

    write_csv('users.csv', [
        ['user_id', 'name'],
        ['1', 'John Doe'],
        ['2', 'Jane Smith'],
        ['3', 'Bob Builder'],
    ])

if __name__ == '__main__':
    main()
