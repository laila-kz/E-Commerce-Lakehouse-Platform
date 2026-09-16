"""
Create required MinIO buckets for the data lakehouse
"""
import os
import boto3
from botocore.exceptions import ClientError


def create_buckets():
    """Create all required buckets"""
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

    client = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        use_ssl=False,
        verify=False
    )

    buckets = ["raw", "bronze", "silver", "gold", "reference", "logs"]

    for bucket in buckets:
        try:
            client.create_bucket(Bucket=bucket)
            print(f"[CREATED] Bucket: {bucket}")
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code', '')
            if code in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']:
                print(f"[EXISTS] Bucket already exists: {bucket}")
            else:
                print(f"[ERROR] Error creating {bucket}: {e}")


if __name__ == "__main__":
    create_buckets()
