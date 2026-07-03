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
        ['order_id', 'customer', 'amount', 'status', 'region', 'order_ts'],
        ['1001', 'Alice', '250.00', 'completed', 'US', '2025-01-15'],
        ['1002', 'Bob', '25.00', 'pending', 'EU', '2025-01-16'],
        ['1003', 'Charlie', '500.00', 'completed', 'US', '2025-01-17'],
        ['1004', 'David', '1500.00', 'cancelled', 'APAC', '2025-01-18'],
        ['1005', 'Eve', '300.00', 'completed', 'EU', '2025-01-19'],
        ['1006', 'Frank', '75.00', 'pending', 'US', '2025-01-20'],
        ['1007', 'Grace', '50.00', 'completed', 'EU', '2025-01-21'],
        ['1008', 'Heidi', '1000.00', 'completed', 'US', '2025-01-22'],
        ['1009', 'Ivan', '10.00', 'pending', 'APAC', '2025-01-23'],
        ['1010', 'Judy', '800.00', 'completed', 'US', '2025-01-24'],
    ])

if __name__ == '__main__':
    main()
