"""
Kaggle Dataset Ingestion Script
Uploads existing CSV files from local data directory to MinIO raw bucket.
"""

import argparse
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import sys

# Try to import python-dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed. Install with: pip install python-dotenv")
    pass

# Third-party imports
import boto3
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""

    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "event": getattr(record, "event", record.msg),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }

        # Add extra fields if they exist
        if hasattr(record, "extra"):
            log_entry.update(record.extra)

        return json.dumps(log_entry)


def setup_logging():
    """Configure structured JSON logging"""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    # Prevent duplicate logs
    logger.propagate = False

    return logger


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Upload local CSV files to MinIO raw bucket"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-upload even if already ingested"
    )

    parser.add_argument(
        "--data-dir",
        default="./data",
        help="Directory containing CSV files (default: ./data)"
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level (default: INFO)"
    )

    return parser.parse_args()


# ============================================================================
# MINIO CLIENT FUNCTIONS
# ============================================================================

def get_minio_client():
    """
    Create and return a boto3 client configured for MinIO
    """
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

    return client


def test_minio_connection(client, logger):
    """
    Test connection to MinIO by listing buckets
    """
    try:
        response = client.list_buckets()
        buckets = [b['Name'] for b in response['Buckets']]
        logger.info(
            "MinIO connection successful",
            extra={
                "event": "minio_connection_success",
                "bucket_count": len(buckets),
                "buckets": buckets
            }
        )
        return True
    except Exception as e:
        logger.error(
            "MinIO connection failed",
            extra={
                "event": "minio_connection_failure",
                "error": str(e),
                "error_type": type(e).__name__
            }
        )
        return False


# ============================================================================
# IDEMPOTENCY FUNCTIONS (MinIO-based)
# ============================================================================

def check_already_ingested_minio(s3_client, bucket="raw", prefix="ecommerce_events", logger=None):
    """
    Check if data has already been ingested by checking for _SUCCESS marker in MinIO
    Returns: (bool, str) - (is_ingested, marker_path)
    """
    marker_key = f"{prefix}/_SUCCESS"

    try:
        s3_client.head_object(Bucket=bucket, Key=marker_key)

        if logger:
            logger.info(
                "Found existing ingestion marker in MinIO",
                extra={
                    "event": "marker_found_minio",
                    "bucket": bucket,
                    "key": marker_key
                }
            )
        return True, f"s3://{bucket}/{marker_key}"

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            if logger:
                logger.info(
                    "No ingestion marker found in MinIO",
                    extra={
                        "event": "marker_not_found_minio",
                        "bucket": bucket,
                        "key": marker_key
                    }
                )
            return False, f"s3://{bucket}/{marker_key}"
        else:
            if logger:
                logger.error(
                    "Error checking marker in MinIO",
                    extra={
                        "event": "marker_check_error",
                        "bucket": bucket,
                        "key": marker_key,
                        "error": str(e)
                    }
                )
            raise


def write_marker_minio(s3_client, bucket="raw", prefix="ecommerce_events", metadata=None, logger=None):
    """
    Write success marker to MinIO after successful ingestion
    """
    marker_key = f"{prefix}/_SUCCESS"

    if metadata is None:
        metadata = {}

    metadata['timestamp'] = datetime.utcnow().isoformat() + "Z"
    metadata['status'] = "success"

    body = json.dumps(metadata)

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=marker_key,
            Body=body.encode('utf-8'),
            ContentType='application/json'
        )

        if logger:
            logger.info(
                "Success marker written to MinIO",
                extra={
                    "event": "marker_written_minio",
                    "bucket": bucket,
                    "key": marker_key,
                    "metadata": metadata
                }
            )

        return True

    except Exception as e:
        if logger:
            logger.error(
                "Failed to write marker to MinIO",
                extra={
                    "event": "marker_write_failure",
                    "bucket": bucket,
                    "key": marker_key,
                    "error": str(e)
                }
            )
        raise


# ============================================================================
# UPLOAD FUNCTIONS
# ============================================================================

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((ClientError, ConnectionError, TimeoutError))
)
def upload_files_to_minio(s3_client, data_dir, bucket="raw", prefix="ecommerce_events", logger=None):
    """
    Upload CSV files from data_dir to MinIO
    """
    data_path = Path(data_dir)

    # Look for CSV files directly in data_dir
    csv_files = list(data_path.glob("*.csv"))

    # If no CSV files in root, check in ecommerce_events subfolder
    if not csv_files:
        ecommerce_path = data_path / "ecommerce_events"
        if ecommerce_path.exists():
            csv_files = list(ecommerce_path.glob("*.csv"))
            data_path = ecommerce_path
            logger.info(
                "Found CSV files in ecommerce_events subfolder",
                extra={"event": "using_subfolder", "path": str(data_path)}
            )

    if not csv_files:
        logger.error(
            "No CSV files found to upload",
            extra={"event": "upload_no_files", "path": str(data_path)}
        )
        return {"uploaded": False}

    # Get current date for partitioning
    today = datetime.utcnow().strftime("%Y-%m-%d")

    uploaded_count = 0
    total_size = 0
    file_names = []

    for csv_file in csv_files:
        s3_key = f"{prefix}/ingested_date={today}/{csv_file.name}"
        file_size = csv_file.stat().st_size

        try:
            logger.info(
                "Uploading file to MinIO",
                extra={
                    "event": "upload_start",
                    "local_file": csv_file.name,
                    "s3_key": s3_key,
                    "bytes": file_size
                }
            )

            s3_client.upload_file(
                Filename=str(csv_file),
                Bucket=bucket,
                Key=s3_key
            )

            uploaded_count += 1
            total_size += file_size
            file_names.append(csv_file.name)

            logger.info(
                "Upload complete",
                extra={
                    "event": "upload_success",
                    "s3_key": s3_key,
                    "bytes": file_size
                }
            )

        except Exception as e:
            logger.error(
                "Upload failed",
                extra={
                    "event": "upload_failure",
                    "s3_key": s3_key,
                    "error": str(e)
                }
            )
            raise

    logger.info(
        "All files uploaded successfully",
        extra={
            "event": "upload_complete",
            "file_count": uploaded_count,
            "total_bytes": total_size,
            "date_partition": today,
            "files": file_names
        }
    )

    return {
        "uploaded": True,
        "file_count": uploaded_count,
        "total_bytes": total_size,
        "date_partition": today,
        "files": file_names,
        "bucket": bucket,
        "prefix": prefix
    }


# ============================================================================
# FAILURE-INJECTION HOOK (testing only, default off)
# ============================================================================

def maybe_inject_upload_failure(logger):
    """
    Testing-only hook. When FAIL_AFTER_INGEST_UPLOAD is "true", raise after files
    are uploaded but before the _SUCCESS marker is written, to exercise crash
    recovery at the ingest-marker boundary. Default off; no effect in normal runs.
    """
    if os.environ.get("FAIL_AFTER_INGEST_UPLOAD", "false").lower() == "true":
        logger.warning(
            "SIMULATED CRASH triggered: upload complete, halting before _SUCCESS marker",
            extra={
                "event": "simulated_ingest_crash",
                "location": "after_upload_before_marker",
            }
        )
        raise RuntimeError(
            "Simulated crash: upload complete, _SUCCESS marker NOT written"
        )


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main entry point"""
    args = parse_args()

    logger = setup_logging()
    logger.setLevel(getattr(logging, args.log_level))

    logger.info(
        "Script started",
        extra={
            "event": "script_start",
            "data_dir": args.data_dir,
            "force": args.force
        }
    )

    # ===== Test MinIO Connection =====
    s3_client = get_minio_client()
    if not test_minio_connection(s3_client, logger):
        logger.error("MinIO connection failed, exiting")
        sys.exit(1)

    # ===== MinIO-based Idempotency Check =====
    already_ingested, marker_path = check_already_ingested_minio(
        s3_client,
        bucket="raw",
        prefix="ecommerce_events",
        logger=logger
    )

    if already_ingested and not args.force:
        logger.info(
            "Already ingested, skipping",
            extra={
                "event": "skip_already_ingested_minio",
                "marker_path": marker_path
            }
        )
        logger.info(
            "Script completed",
            extra={"event": "script_complete", "status": "skipped"}
        )
        return

    elif already_ingested and args.force:
        logger.info(
            "Force mode enabled, re-ingesting",
            extra={"event": "force_mode_minio"}
        )

    logger.info(
        "Proceeding with upload",
        extra={"event": "upload_start"}
    )

    # ===== Upload to MinIO =====
    upload_result = upload_files_to_minio(
        s3_client,
        args.data_dir,
        bucket="raw",
        prefix="ecommerce_events",
        logger=logger
    )

    if not upload_result["uploaded"]:
        logger.error("Upload failed, exiting")
        sys.exit(1)

    # ===== Failure-injection hook (default off) =====
    maybe_inject_upload_failure(logger)

    # ===== Write Success Marker to MinIO =====
    write_marker_minio(
        s3_client,
        bucket="raw",
        prefix="ecommerce_events",
        metadata={
            "file_count": upload_result["file_count"],
            "total_bytes": upload_result["total_bytes"],
            "date_partition": upload_result["date_partition"],
            "files": upload_result["files"]
        },
        logger=logger
    )

    logger.info(
        "Script completed",
        extra={
            "event": "script_complete",
            "status": "success",
            "files_uploaded": upload_result["file_count"],
            "bytes_uploaded": upload_result["total_bytes"]
        }
    )


if __name__ == "__main__":
    main()
