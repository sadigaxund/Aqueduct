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
    write_csv('products.csv', [
        ['id', 'name', 'price', 'quantity', 'listed_at'],
        ['SKU-001', 'Widget', '19.99', '150', '2025-01-15'],
        ['SKU-002', 'Gadget', '29.99', '200', '2025-01-16'],
        ['SKU-003', 'Doohickey', '14.99', '500', '2025-01-17'],
        ['SKU-004', 'Thingamajig', '99.99', '30', '2025-01-18'],
    ])

if __name__ == '__main__':
    main()
