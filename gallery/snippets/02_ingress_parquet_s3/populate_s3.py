import pandas as pd
import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def main():
    print("--- Aqueduct S3/Minio Population Tool ---")
    
    # Static path as requested
    s3_path = "s3a://aqueduct-gallery/weather-data/"
    bucket_name = "aqueduct-gallery"
    
    # Try to get from environment first
    endpoint = os.environ.get("S3_ENDPOINT", "").strip()
    access_key = os.environ.get("S3_ACCESS_KEY", "").strip()
    secret_key = os.environ.get("S3_SECRET_KEY", "").strip()

    print(f"Target Path: {s3_path}")

    # Create sample data
    df = pd.DataFrame({
        'station_id': ['S1', 'S2', 'S3'],
        'observation_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'temperature': [25.5, 26.0, 24.8],
        'humidity': [60.0, 65.0, 58.5],
        'is_valid': [True, True, True]
    })
    
    df['observation_date'] = pd.to_datetime(df['observation_date'])

    print(f"\nPreparing to upload {len(df)} records to {s3_path}...")
    
    storage_options = {}
    if endpoint:
        storage_options['client_kwargs'] = {'endpoint_url': endpoint}
    if access_key:
        storage_options['key'] = access_key
    if secret_key:
        storage_options['secret'] = secret_key

    try:
        import s3fs
        
        # Initialize s3fs
        fs = s3fs.S3FileSystem(**storage_options)
        
        # Check if bucket exists, create if not
        if not fs.exists(bucket_name):
            print(f"Bucket '{bucket_name}' not found. Creating it...")
            fs.mkdir(bucket_name)
            print(f"✓ Bucket '{bucket_name}' created.")
        
        output_file = f"{s3_path.replace('s3a://', 's3://')}weather_data.parquet"
        
        df.to_parquet(output_file, index=False, storage_options=storage_options)
        print(f"\nSuccessfully uploaded to {output_file}")
        
        
        print("\nNext run: aqueduct run blueprint.yml")

    except ImportError:
        print("\nError: 's3fs' library not found.")
        print("Run: pip install s3fs pyarrow")
        sys.exit(1)
    except Exception as e:
        print(f"\nUpload failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
