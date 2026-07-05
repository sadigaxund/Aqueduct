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
        ['order_id', 'customer', 'amount', 'region', 'status', 'score'],
        ['1', 'Alice', '150.0', 'US', 'active', '85'],
        ['2', 'Bob', '75.0', 'UK', 'active', '92'],
        ['3', 'Charlie', '200.0', 'DE', 'pending', '45'],
        ['4', 'Dana', '300.0', 'FR', 'active', '77'],
        ['5', 'Evan', '50.0', 'US', 'cancelled', '60'],
        ['6', 'Farah', '25.0', 'UK', 'active', '99'],
        ['7', 'Grace', '120.0', 'DE', 'pending', '55'],
        ['8', 'Hank', '275.0', 'US', 'active', '81'],
        ['9', 'Ivy', '90.0', 'FR', 'cancelled', '38'],
        ['10', 'Jack', '210.0', 'UK', 'active', '73'],
    ])

if __name__ == '__main__':
    main()
