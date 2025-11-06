# utils.py
import logging, io
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def get_s3_hook(s3_conn_id='aws_default'):
    return S3Hook(aws_conn_id=s3_conn_id)

def now_iso():
    return datetime.utcnow().isoformat() + 'Z'

def upload_bytes_to_s3(hook, data_bytes, key, bucket_name, replace=True):
    hook.load_bytes(data_bytes, key=key, bucket_name=bucket_name, replace=replace)
    logging.info(f"Uploaded to s3://{bucket_name}/{key}")