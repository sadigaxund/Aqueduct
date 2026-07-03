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
    write_csv('customers.csv', [
        ['customer_id', 'name', 'region'],
        ['1', 'Alice', 'US'],
        ['2', 'Bob', 'EU'],
        ['3', 'Charlie', 'APAC'],
    ])

    write_csv('products.csv', [
        ['id', 'name', 'category'],
        ['1', 'Widget', 'Gadgets'],
        ['2', 'Gadget', 'Gadgets'],
        ['3', 'Tool', 'Hardware'],
    ])

if __name__ == '__main__':
    main()
