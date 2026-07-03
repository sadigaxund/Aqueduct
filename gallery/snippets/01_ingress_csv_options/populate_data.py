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
        ['vendor_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'ratecode_id', 'store_and_fwd_flag', 'pulocation_id', 'dolocation_id', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee'],
        ['1', '2024-01-01 00:00:00', '2024-01-01 00:10:00', '1', '2.5', '1', 'N', '141', '236', '1', '10.0', '0.5', '0.5', '2.0', '0.0', '0.3', '13.3', '0.0', '0.0'],
        ['2', '2024-01-01 00:05:00', '2024-01-01 00:15:00', '2', '3.0', '1', 'N', '161', '162', '2', '12.0', '0.0', '0.5', '0.0', '0.0', '0.3', '12.8', '0.0', '0.0'],
        ['1', '2024-01-01 00:20:00', '2024-01-01 00:30:00', '1', '1.8', '1', 'N', '237', '238', '1', '8.5', '0.5', '0.5', '1.5', '0.0', '0.3', '11.3', '0.0', '0.0'],
    ])

if __name__ == '__main__':
    main()
