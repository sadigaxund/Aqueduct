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
        ['order_id', 'amount'],
        ['101', '50.0'],
        ['102', '75.0'],
        ['103', '-10.0'],
        ['104', ''],
        ['105', '9999.0'],
    ])

if __name__ == '__main__':
    main()
