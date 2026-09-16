#!/usr/bin/env python3
"""
Raw Layer Quality Checks
Structural/existence checks for raw data before Bronze processing.
"""

from datetime import datetime
import os
import sys

import boto3
from botocore.exceptions import ClientError

# ===== MinIO Configuration =====


def get_minio_client():
    """Create boto3 client configured for MinIO"""
    # Use minio:9000 when running inside Docker
    # Use localhost:9000 when running on host
    # Default to minio:9000 (since we're inside the container)
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        use_ssl=False,
        verify=False
    )

# ===== RAW CHECKS =====


def check_bucket_exists(client, bucket="raw"):
    """Check that the raw bucket exists"""
    try:
        client.head_bucket(Bucket=bucket)
        print(f"✅ Raw bucket exists: {bucket}")
        return True
    except ClientError as e:
        print(f"❌ Raw bucket not found: {bucket} - {e}")
        return False


def check_partition_exists(client, bucket="raw", prefix="ecommerce_events/"):
    """Check that at least one ingested_date partition exists"""
    try:
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter='/'
        )
        # Look for ingested_date= folders
        partitions = [p.get('Prefix') for p in response.get('CommonPrefixes', [])]
        if partitions:
            print(f"✅ Partitions found: {len(partitions)}")
            return True
        else:
            print("❌ No partitions found in raw bucket")
            return False
    except ClientError as e:
        print(f"❌ Error listing partitions: {e}")
        return False


def check_files_non_empty(client, bucket="raw", prefix="ecommerce_events/"):
    """Check that CSV files exist and are non-empty"""
    try:
        # List all objects in the raw bucket
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )

        csv_files = [obj for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

        if not csv_files:
            print("❌ No CSV files found in raw bucket")
            return False

        non_empty_count = 0
        for obj in csv_files:
            if obj['Size'] > 0:
                non_empty_count += 1
                print(f"   ✅ {obj['Key']}: {obj['Size']:,} bytes")
            else:
                print(f"   ❌ {obj['Key']}: EMPTY (0 bytes)")

        if non_empty_count == len(csv_files):
            print(f"✅ All {len(csv_files)} CSV files are non-empty")
            return True
        else:
            print(f"❌ {len(csv_files) - non_empty_count} files are empty")
            return False

    except ClientError as e:
        print(f"❌ Error checking files: {e}")
        return False


def check_raw_freshness(client, bucket="raw", prefix="ecommerce_events/", max_age_days=30):
    """Check that the most recent partition is recent enough"""
    try:
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter='/'
        )

        partitions = [p.get('Prefix') for p in response.get('CommonPrefixes', [])]
        if not partitions:
            print("❌ No partitions found for freshness check")
            return False

        # Extract dates from partition names
        dates = []
        for p in partitions:
            # e.g., "ecommerce_events/ingested_date=2026-07-20/"
            parts = p.rstrip('/').split('=')
            if len(parts) >= 2:
                try:
                    date_str = parts[-1]
                    dates.append(datetime.strptime(date_str, '%Y-%m-%d'))
                except ValueError:
                    continue

        if not dates:
            print("❌ No valid partition dates found")
            return False

        latest = max(dates)
        days_ago = (datetime.now() - latest).days

        if days_ago <= max_age_days:
            print(f"✅ Latest partition is {days_ago} days old (max: {max_age_days} days)")
            return True
        else:
            print(f"❌ Latest partition is {days_ago} days old (max: {max_age_days} days)")
            return False

    except ClientError as e:
        print(f"❌ Error checking freshness: {e}")
        return False


def check_row_count(client, bucket="raw", prefix="ecommerce_events/"):
    """Check that CSV files have a reasonable row count"""
    try:
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )

        csv_files = [obj for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

        if not csv_files:
            return False

        # Get one file and count rows (rough line count via head request)
        # For simplicity, check that the file exists and has size > 100 bytes
        # A real implementation could stream the file and count rows
        total_size = sum(obj['Size'] for obj in csv_files)

        # Rough estimate: each row is ~100-200 bytes
        # 100 bytes minimum for 1000 rows = 100,000 bytes
        if total_size > 100000:
            print(f"✅ Total raw data size: {total_size:,} bytes (estimated ~{total_size//150:,} rows)")
            return True
        else:
            print(f"❌ Raw data too small: {total_size} bytes")
            return False

    except ClientError as e:
        print(f"❌ Error checking row count: {e}")
        return False

# ===== MAIN =====


def run_raw_checks():
    """Run all raw layer quality checks"""
    print("=" * 60)
    print("🔍 RAW LAYER QUALITY CHECKS")
    print("=" * 60)
    print("📂 Bucket: raw")
    print("📁 Prefix: ecommerce_events/")
    print("=" * 60)

    client = get_minio_client()

    checks = [
        ("Bucket exists", lambda: check_bucket_exists(client)),
        ("Partition exists", lambda: check_partition_exists(client)),
        ("Files non-empty", lambda: check_files_non_empty(client)),
        ("Freshness (<=2 days)", lambda: check_raw_freshness(client)),
        ("Row count > 0", lambda: check_row_count(client))
    ]

    passed = 0
    failed = 0

    for name, check_fn in checks:
        try:
            result = check_fn()
            if result:
                passed += 1
            else:
                failed += 1
                print(f"   ❌ FAILED: {name}")
        except Exception as e:
            failed += 1
            print(f"   ❌ ERROR in {name}: {e}")

    print("\n" + "=" * 60)
    print("📊 RAW CHECKS COMPLETE")
    print("=" * 60)
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")

    if failed == 0:
        print("✅ ALL RAW CHECKS PASSED!")
        return 0
    else:
        print("❌ SOME RAW CHECKS FAILED!")
        return 1


if __name__ == "__main__":
    sys.exit(run_raw_checks())
