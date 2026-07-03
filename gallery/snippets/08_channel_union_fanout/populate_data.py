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
    write_csv('data_a.csv', [
        ['id', 'val', 'source'],
        ['1', '10', 'A'],
        ['2', '25', 'A'],
    ])

    write_csv('data_b.csv', [
        ['id', 'val', 'source'],
        ['3', '5', 'B'],
        ['4', '30', 'B'],
    ])

if __name__ == '__main__':
    main()
