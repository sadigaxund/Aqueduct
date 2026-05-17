"""
Seed MinIO with synthetic transaction data.

Run this BEFORE `aqueduct run` — the pipeline reads
s3a://showcase/input/transactions.parquet, which does not exist until
this script uploads it.

Requirements:
    pip install boto3 pyarrow pandas

Config (environment, no .env file):
    HOST_IP   required — LAN IP of the machine running MinIO (the
              docker-compose host). MinIO is reached at http://$HOST_IP:9000.
              MinIO credentials are the local-testing default minioadmin
              / minioadmin (matches docker-compose.yml).

Usage:
    export HOST_IP=192.168.1.10
    python seed.py
"""

import io
import os
import sys

HOST_IP = os.environ.get("HOST_IP")
if not HOST_IP:
    print(
        "ERROR: HOST_IP is not set.\n"
        "  This is the LAN IP of the machine running the Spark cluster + MinIO\n"
        "  (the docker-compose host). Same machine as this script? Use that\n"
        "  machine's LAN IP — NOT 127.0.0.1 / localhost (Spark workers inside\n"
        "  Docker cannot reach loopback).\n\n"
        "  Find it:\n"
        "    Linux:  hostname -I | awk '{print $1}'\n"
        "    macOS:  ipconfig getifaddr en0\n\n"
        "  Then:  export HOST_IP=<that-ip>  &&  python seed.py",
        file=sys.stderr,
    )
    sys.exit(1)

ENDPOINT = f"http://{HOST_IP}:9000"
BUCKET = "showcase"

# ── Connect ───────────────────────────────────────────────────────────────────

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

print(f"Connecting to MinIO at {ENDPOINT} ...")

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
)

try:
    s3.list_buckets()
except EndpointConnectionError:
    print(f"ERROR: cannot reach MinIO at {ENDPOINT}", file=sys.stderr)
    print("  - Is the stack up?   docker compose ps", file=sys.stderr)
    print("  - Is HOST_IP a LAN IP reachable from here (not loopback)?", file=sys.stderr)
    sys.exit(1)

# ── Bucket ────────────────────────────────────────────────────────────────────

try:
    s3.create_bucket(Bucket=BUCKET)
    print(f"  create  s3://{BUCKET}/")
except ClientError as e:
    if e.response["Error"]["Code"] not in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
        raise
    print(f"  exists  s3://{BUCKET}/")

# ── Generate data ─────────────────────────────────────────────────────────────

import random

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

random.seed(42)
n = 1000

data = {
    "transaction_id": [f"T{i:05d}" for i in range(n)],
    "region":         [random.choice(["US", "EU", "APAC"]) for _ in range(n)],
    "amount":         [round(random.uniform(10, 5000), 2) if random.random() > 0.05 else None for _ in range(n)],
    "status":         [random.choice(["completed", "completed", "completed", "pending", "failed"]) for _ in range(n)],
    "event_time":     pd.date_range("2026-01-01", periods=n, freq="1min").tolist(),
}

df = pd.DataFrame(data)
table = pa.Table.from_pandas(df, preserve_index=False)

buf = io.BytesIO()
pq.write_table(table, buf)
buf.seek(0)

s3.put_object(Bucket=BUCKET, Key="input/transactions.parquet", Body=buf.read())

completed = (df["status"] == "completed").sum()
nulls = df["amount"].isna().sum()

print(f"  seed    s3://{BUCKET}/input/transactions.parquet")
print(f"          {n} rows  |  {completed} completed  |  {nulls} null amounts")
print()
print(f"MinIO console: http://{HOST_IP}:9001  (minioadmin / minioadmin)")
print(f"Spark UI:      http://{HOST_IP}:8080")
