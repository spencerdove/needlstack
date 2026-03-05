"""
Thin boto3 wrapper for Cloudflare R2 uploads.

Required env vars:
    R2_ACCOUNT_ID         — Cloudflare account ID
    R2_ACCESS_KEY_ID      — R2 API token key ID
    R2_SECRET_ACCESS_KEY  — R2 API token secret
    R2_BUCKET_NAME        — bucket name (default: needlstack)
"""
import os

import boto3


def get_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )


def upload_json(key: str, data: str, bucket: str = None) -> None:
    if bucket is None:
        bucket = os.environ.get("R2_BUCKET_NAME", "needlstack")
    get_r2_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=data.encode(),
        ContentType="application/json",
        CacheControl="public, max-age=3600",
    )
