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
        ['id', 'product', 'price', 'in_stock'],
        ['1', 'Widget A', '19.99', 'true'],
        ['2', 'Widget B', '29.99', 'true'],
        ['3', 'Gadget', '49.99', 'true'],
        ['4', 'Doohickey', '9.99', 'false'],
    ])

if __name__ == '__main__':
    main()
