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
    write_csv('orders_2024.csv', [
        ['order_id', 'product', 'amount'],
        ['1001', 'Widget', '50.00'],
        ['1002', 'Gadget', '75.00'],
        ['1003', 'Widget', '25.00'],
    ])

    write_csv('orders_2025.csv', [
        ['order_id', 'product', 'amount'],
        ['1003', 'Widget', '25.00'],
        ['2001', 'Doohickey', '30.00'],
        ['2002', 'Gadget', '60.00'],
        ['2003', 'Thingamajig', '120.00'],
    ])

if __name__ == '__main__':
    main()
