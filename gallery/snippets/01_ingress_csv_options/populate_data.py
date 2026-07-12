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
    write_csv('sample_data.csv', [
        ['id', 'name', 'amount', 'category'],
        ['1', 'widget', '10.0', 'a'],
        ['2', 'gadget', '25.0', 'b'],
        ['3', 'doohickey', '5.0', 'a'],
    ])

if __name__ == '__main__':
    main()
