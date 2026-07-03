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
        ['id', 'name', 'category', 'price', 'stock', 'in_stock', 'created_at'],
        ['1', 'Widget A', 'Gadgets', '19.99', '150', 'true', '2025-01-01'],
        ['2', 'Widget B', 'Gadgets', '29.99', '200', 'true', '2025-01-02'],
        ['3', 'Gadget X', 'Gadgets', '49.99', '75', 'true', '2025-01-03'],
        ['4', 'Gadget Y', 'Gadgets', '99.99', '30', 'true', '2025-01-04'],
        ['5', 'Doohickey', 'Tools', '14.99', '500', 'true', '2025-01-05'],
        ['6', 'Thingamajig', 'Tools', '24.99', '0', 'false', '2025-01-06'],
        ['7', 'Whatchamacallit', 'Widgets', '39.99', '100', 'true', '2025-01-07'],
        ['8', 'Super Widget', 'Gadgets', '199.99', '10', 'true', '2025-01-08'],
        ['9', 'Mini Gadget', 'Gadgets', '9.99', '1000', 'true', '2025-01-09'],
        ['10', 'Pro Tool', 'Tools', '149.99', '25', 'true', '2025-01-10'],
    ])

if __name__ == '__main__':
    main()
