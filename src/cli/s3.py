"""S3 operations for the Praxis CLI."""

from pathlib import Path

import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError, NoCredentialsError

BUCKET = "praxis-copilot"


def get_s3_client():
    """Get an S3 client, raising a clear error if credentials are missing."""
    try:
        client = boto3.client("s3")
        # Quick check that credentials are configured
        client.list_buckets()
        return client
    except NoCredentialsError:
        raise SystemExit(
            "AWS credentials not configured. Run 'aws configure' or set environment variables."
        )
    except ClientError as e:
        raise SystemExit(f"AWS error: {e}")


def upload_file(s3_client, local_path: Path, s3_key: str) -> None:
    """Upload a single file to S3."""
    try:
        s3_client.upload_file(str(local_path), BUCKET, s3_key)
    except (ClientError, S3UploadFailedError) as e:
        err_str = str(e)
        if "NoSuchBucket" in err_str:
            raise SystemExit(
                f"Bucket '{BUCKET}' does not exist. Create it first:\n"
                f"  aws s3 mb s3://{BUCKET}"
            )
        raise SystemExit(f"S3 upload error: {e}")


def upload_directory(s3_client, local_dir: Path, s3_prefix: str) -> list[str]:
    """Upload all files in a local directory to S3, preserving structure.

    Returns list of uploaded S3 keys.
    """
    uploaded = []
    for path in sorted(local_dir.rglob("*")):
        if path.is_file():
            relative = path.relative_to(local_dir)
            s3_key = f"{s3_prefix}/{relative}"
            upload_file(s3_client, path, s3_key)
            uploaded.append(s3_key)
    return uploaded


def key_exists(s3_client, s3_key: str) -> bool:
    """Check if an S3 key exists."""
    try:
        s3_client.head_object(Bucket=BUCKET, Key=s3_key)
        return True
    except ClientError:
        return False


def list_prefix(s3_client, prefix: str) -> list[str]:
    """List all keys under an S3 prefix."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def download_file(s3_client, s3_key: str) -> bytes:
    """Download an S3 object and return its contents."""
    response = s3_client.get_object(Bucket=BUCKET, Key=s3_key)
    return response["Body"].read()
