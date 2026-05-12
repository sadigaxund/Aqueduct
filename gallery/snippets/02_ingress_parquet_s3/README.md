# S3/Minio Parquet Ingress Snippet

Demonstrates how to read Parquet files from S3 or Minio with custom endpoints and credentials.

## Setup (Optional: Minio/S3)

If you want to test against a real S3/Minio store:

1. **Populate the store**:
   Run the interactive population tool:
   ```bash
   python populate_s3.py
   ```
   *Note: This will also generate the necessary export commands for your credentials.*

2. **Set Environment Variables**:
   Create a `.env` file in this directory:
   ```env
   S3_ENDPOINT=http://localhost:9000
   S3_ACCESS_KEY=your_key
   S3_SECRET_KEY=your_secret
   ```
   Aqueduct will automatically load this file when you run the pipeline.

## How to Run

1. **Execute the Pipeline**:
   ```bash
   aqueduct run blueprint.yml
   ```
   *Note: If no S3 variables are set, it will fail unless you have local data in `data/input/`.*

2. **Inspect Results**:
   ```bash
   python inspect_results.py
   ```
