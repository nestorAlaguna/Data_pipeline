import logging, io
import pandas as pd
from airflow.exceptions import AirflowException
from datetime import datetime
from settings import CONFIG
from utils import get_s3_hook, upload_bytes_to_s3

def validate_source_file(execution_date, ti=None):
    # Ensure the expected file exists in source bucket and basic checks.
    s3_hook = get_s3_hook(CONFIG['s3_conn_id'])
    # execution_date may be a string if passed from templated DAG; try parse if needed
    if isinstance(execution_date, str):
        try:
            execution_date = datetime.strptime(execution_date, '%Y-%m-%d')
        except Exception:
            # try date string YYYYMMDD
            execution_date = datetime.strptime(execution_date, '%Y%m%d')
    expected_date = execution_date.strftime('%Y%m%d')
    expected_filename = f"network_metrics_{expected_date}.csv"
    logging.info(f"Checking for {expected_filename} in bucket {CONFIG['source_bucket']}")
    if not s3_hook.check_for_key(expected_filename, bucket_name=CONFIG['source_bucket']):
        raise AirflowException(f"File not found: {expected_filename}")
    # check size > 0
    key = s3_hook.get_key(expected_filename, bucket_name=CONFIG['source_bucket'])
    if getattr(key, 'content_length', 0) == 0:
        raise AirflowException(f"File is empty: {expected_filename}")
    # push to xcom if provided
    if ti:
        ti.xcom_push(key='source_file', value=expected_filename)
        ti.xcom_push(key='processing_date', value=execution_date.strftime('%Y-%m-%d'))
    logging.info('Source file validated.')
    return expected_filename

def ingest_to_bronze(source_file, processing_date, ti=None):
    # Read CSV from source bucket and write an immutable parquet to bronze path.
    s3_hook = get_s3_hook(CONFIG['s3_conn_id'])
    try:
        csv_content = s3_hook.read_key(source_file, bucket_name=CONFIG['source_bucket'])
        df = pd.read_csv(io.StringIO(csv_content))
    except Exception as e:
        logging.error(f"Failed reading source CSV: {e}")
        raise AirflowException(str(e))
    # Add metadata
    df['ingestion_timestamp'] = datetime.utcnow()
    df['source_file'] = source_file
    df['processing_date'] = processing_date
    # Write parquet bytes
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False)
    out_buffer.seek(0)
    bronze_key = f"bronze/network_metrics/processing_date={processing_date}/network_data.parquet"
    upload_bytes_to_s3(s3_hook, out_buffer.getvalue(), bronze_key, CONFIG['target_bucket'], replace=True)
    if ti:
        ti.xcom_push(key='bronze_path', value=bronze_key)
        ti.xcom_push(key='bronze_record_count', value=len(df))
    logging.info(f"Bronze write complete: {bronze_key} (records={len(df)})")
    return bronze_key
