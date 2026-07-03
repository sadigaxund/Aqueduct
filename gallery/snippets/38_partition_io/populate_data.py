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
    write_csv('transactions.csv', [
        ['tx_id', 'date', 'amount', 'region'],
        ['1', '2024-01-15', '100.00', 'US'],
        ['2', '2024-01-15', '150.00', 'US'],
        ['3', '2024-02-10', '75.50', 'EU'],
        ['4', '2024-02-20', '200.00', 'APAC'],
        ['5', '2024-03-05', '50.00', 'US'],
        ['6', '2024-03-12', '180.00', 'EU'],
        ['7', '2024-03-15', '95.00', 'APAC'],
        ['8', '2024-04-01', '300.00', 'US'],
    ])

if __name__ == '__main__':
    main()
