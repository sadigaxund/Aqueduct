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
    write_csv('scores.csv', [
        ['id', 'score'],
        ['1', '85'],
        ['2', '-10'],
        ['3', '95'],
        ['4', 'invalid'],
    ])

if __name__ == '__main__':
    main()
