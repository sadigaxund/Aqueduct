#!/usr/bin/env python3
"""Generate mock order + customer data and upload to MinIO.

Creates ~1000 orders and ~200 customers with intentional data quality issues:
  - ~5 % null amounts
  - ~3 % future order_dates
  - ~10 % malformed emails (no '@' sign)

Uploads Parquet files to:
  s3://raw-data/orders/orders.parquet
  s3://raw-data/customers/customers.parquet

Also writes local copies to examples/comprehensive_test/data/ for inspection.
"""

from __future__ import annotations

import random
import string
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

# ── Config ─────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = "http://10.0.0.39:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin12345"
RAW_BUCKET       = "raw-data"

LOCAL_OUT = Path(__file__).parent / "data"

REGIONS  = ["US", "US", "US", "EU", "EU", "OTHER"]   # weighted ~50/33/17
STATUSES = ["completed", "completed", "pending", "cancelled", "refunded"]

N_ORDERS    = 1000
N_CUSTOMERS = 200

# ── Helpers ────────────────────────────────────────────────────────────────────

def _rand_id(prefix: str, length: int = 8) -> str:
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return f"{prefix}-{suffix}"


def _rand_past_dt(max_days: int = 365) -> datetime:
    delta = timedelta(
        days=random.randint(0, max_days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return datetime.now(tz=timezone.utc) - delta


def _rand_future_dt() -> datetime:
    return datetime.now(tz=timezone.utc) + timedelta(days=random.randint(1, 60))


def _rand_email(name: str, *, malformed: bool = False) -> str:
    domain = random.choice(["gmail.com", "yahoo.com", "outlook.com", "company.io"])
    slug   = name.lower().replace(" ", ".")
    return f"{slug}[at]{domain}" if malformed else f"{slug}@{domain}"


# ── Data Generation ────────────────────────────────────────────────────────────

def _make_customers(n: int = N_CUSTOMERS) -> pd.DataFrame:
    rows = []
    for i in range(n):
        name      = f"Customer {i:04d}"
        malformed = (i % 10 == 7)           # every 10th → ~10 % malformed
        rows.append(
            {
                "customer_id": _rand_id("C"),
                "name":        name,
                "email":       _rand_email(name, malformed=malformed),
                "signup_date": (
                    datetime.now(tz=timezone.utc) - timedelta(days=random.randint(30, 1000))
                )
                .date()
                .isoformat(),
            }
        )
    return pd.DataFrame(rows)


def _make_orders(customer_ids: list[str], n: int = N_ORDERS) -> pd.DataFrame:
    rows = []
    for i in range(n):
        null_amount  = (i % 20 == 0)        # every 20th → ~5 % null amounts
        future_date  = (i % 33 == 32)       # every 33rd → ~3 % future dates
        amount       = None if null_amount else round(random.uniform(5.0, 5000.0), 2)
        order_date   = _rand_future_dt() if future_date else _rand_past_dt()
        rows.append(
            {
                "order_id":   _rand_id("O"),
                "customer_id": random.choice(customer_ids),
                "amount":     amount,
                "order_date": order_date.isoformat(),
                "region":     random.choice(REGIONS),
                "status":     random.choice(STATUSES),
            }
        )
    return pd.DataFrame(rows)


# ── I/O helpers ────────────────────────────────────────────────────────────────

def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf   = BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf)
    return buf.getvalue()


def _ensure_bucket(s3: boto3.client, bucket: str) -> None:
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"  created bucket '{bucket}'")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code not in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
            raise


def _upload(s3: boto3.client, bucket: str, key: str, data: bytes) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    print(f"  ✓  s3://{bucket}/{key}  ({len(data):,} bytes)")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    random.seed(42)

    # ── generate ───────────────────────────────────────────────────────────────
    print("Generating mock data …")
    customers = _make_customers()
    orders    = _make_orders(customers["customer_id"].tolist())

    null_amounts  = int(orders["amount"].isna().sum())
    bad_emails    = int((~customers["email"].str.contains("@")).sum())
    future_orders = int(
        (pd.to_datetime(orders["order_date"], utc=True) > pd.Timestamp.now(tz="UTC")).sum()
    )
    print(
        f"  customers : {len(customers):>5}  (malformed emails: {bad_emails})\n"
        f"  orders    : {len(orders):>5}  "
        f"(null amounts: {null_amounts}, future dates: {future_orders})"
    )

    # ── local copies ───────────────────────────────────────────────────────────
    LOCAL_OUT.mkdir(parents=True, exist_ok=True)
    customers.to_parquet(LOCAL_OUT / "customers.parquet", index=False)
    orders.to_parquet(LOCAL_OUT / "orders.parquet",    index=False)
    print(f"\nLocal copies → {LOCAL_OUT}/")

    # ── upload to MinIO ────────────────────────────────────────────────────────
    print(f"\nConnecting to MinIO at {MINIO_ENDPOINT} …")
    s3 = boto3.client(
        "s3",
        endpoint_url         = MINIO_ENDPOINT,
        aws_access_key_id    = MINIO_ACCESS_KEY,
        aws_secret_access_key = MINIO_SECRET_KEY,
    )

    _ensure_bucket(s3, RAW_BUCKET)
    _ensure_bucket(s3, "processed-data")   # egress bucket must exist before blueprint writes
    print("Uploading …")
    _upload(s3, RAW_BUCKET, "orders/orders.parquet",       _to_parquet_bytes(orders))
    _upload(s3, RAW_BUCKET, "customers/customers.parquet", _to_parquet_bytes(customers))

    print("\n✓ Upload complete.")
    print("\nNext step:")
    print("  cd examples/comprehensive_test")
    print("  aqueduct run blueprint.yml --config aqueduct.yml")


if __name__ == "__main__":
    main()
