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
        ['product_id', 'name', 'price', 'region'],
        ['1', 'Widget A', '19.99', 'US'],
        ['2', 'Widget B', '29.99', 'US'],
        ['3', 'Gadget', '49.99', 'EU'],
        ['4', 'Doohickey', '14.99', 'APAC'],
        ['5', 'Thingamajig', '99.99', 'US'],
    ])

    write_csv('products_prod.csv', [
        ['product_id', 'name', 'price', 'region'],
        ['6', 'Premium Widget', '199.99', 'US'],
        ['7', 'Enterprise Suite', '999.99', 'US'],
    ])

if __name__ == '__main__':
    main()
