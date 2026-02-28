"""S3 utility functions for reading/writing EHR data files."""
import boto3
import logging
from typing import List

logger = logging.getLogger(__name__)

s3 = boto3.client("s3")


def list_new_files(bucket: str, prefix: str, marker: str = "") -> List[str]:
    """List all files in an S3 prefix, optionally paginating past a marker."""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", [])]


def read_file(bucket: str, key: str) -> str:
    """Read a file from S3 and return as string."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def move_to_processed(bucket: str, key: str, processed_prefix: str = "processed/"):
    """Move a file from landing to processed prefix after successful ingestion."""
    new_key = key.replace("incoming/", processed_prefix)
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=new_key)
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info(f"Moved {key} -> {new_key}")
