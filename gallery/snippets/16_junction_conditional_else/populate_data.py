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
    write_csv('tickets.csv', [
        ['ticket_id', 'status'],
        ['TK-1', 'NEW'],
        ['TK-2', 'PENDING'],
        ['TK-3', 'NEW'],
        ['TK-4', 'ERROR'],
        ['TK-5', 'PENDING'],
        ['TK-6', 'DEPRECATED'],
    ])

if __name__ == '__main__':
    main()
