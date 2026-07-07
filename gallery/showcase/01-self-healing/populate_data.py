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
    write_csv("orders.csv", [
        ["order_ref", "customer_id", "product_id", "quantity", "unit_price", "order_date", "status"],
        ["ORD-1001", "C-01", "P-01", "12", "45.00", "2026-06-01", "shipped"],
        ["ORD-1002", "C-02", "P-02", "3", "18.50", "2026-06-01", "shipped"],
        ["ORD-1003", "C-03", "P-01", "1", "899.00", "2026-06-02", "pending"],
        ["ORD-1004", "C-01", "P-03", "5", "22.00", "2026-06-02", "shipped"],
        ["ORD-1005", "C-04", "P-02", "2", "18.50", "2026-06-03", "shipped"],
        ["ORD-1006", "C-02", "P-04", "40", "12.00", "2026-06-03", "shipped"],
        ["ORD-1007", "C-05", "P-01", "1", "899.00", "2026-06-04", "pending"],
        ["ORD-1008", "C-03", "P-03", "0", "22.00", "2026-06-04", "cancelled"],   # quantity=0 -> quarantined
        ["ORD-1009", "C-01", "P-04", "6", "12.00", "2026-06-05", "shipped"],
        ["ORD-1010", "C-06", "P-02", "-2", "18.50", "2026-06-05", "returned"],  # negative quantity -> quarantined
        ["ORD-1011", "C-04", "P-01", "2", "899.00", "2026-06-06", "shipped"],
        ["ORD-1012", "C-02", "P-03", "8", "22.00", "2026-06-06", "shipped"],
        ["ORD-1013", "C-05", "P-04", "15", "12.00", "2026-06-07", "shipped"],
        ["ORD-1014", "C-01", "P-02", "1", "18.50", "2026-06-07", "shipped"],
        ["ORD-1015", "C-03", "P-01", "3", "899.00", "2026-06-08", "shipped"],
    ])


if __name__ == "__main__":
    main()
