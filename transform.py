import logging, io
import pandas as pd
from airflow.exceptions import AirflowException
from utils import get_s3_hook, upload_bytes_to_s3
from settings import CONFIG
from data_quality import data_quality_summary

REQUIRED_COLS = ['tower_id', 'region', 'timestamp', 'signal_strength', 'data_volume_mb']

def clean_and_write_silver(processing_date, ti=None):
    s3_hook = get_s3_hook(CONFIG['s3_conn_id'])
    bronze_key = f\"bronze/network_metrics/processing_date={processing_date}/network_data.parquet\"
    try:
        raw_bytes = s3_hook.read_key(bronze_key, bucket_name=CONFIG['target_bucket'])
        raw_buf = io.BytesIO(raw_bytes.encode() if isinstance(raw_bytes, str) else raw_bytes)
        df = pd.read_parquet(raw_buf)
    except Exception as e:
        logging.error(f\"Failed reading bronze parquet: {e}\")
        raise AirflowException(str(e))
    initial_count = len(df)
    # Type coercion
    df['tower_id'] = pd.to_numeric(df['tower_id'], errors='coerce').astype('Int64')
    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce').astype('Int64')
    df['signal_strength'] = pd.to_numeric(df['signal_strength'], errors='coerce')
    df['data_volume_mb'] = pd.to_numeric(df['data_volume_mb'], errors='coerce')
    df['region'] = df['region'].astype(str).str.upper().str.strip()
    # Timestamp -> datetime
    df['event_time'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce', utc=True)
    df['date'] = df['event_time'].dt.date
    df['hour'] = df['event_time'].dt.hour
    # Data quality summary
    dq_report = data_quality_summary(df, REQUIRED_COLS, CONFIG['signal_bounds'])
    # Filter invalid rows
    df_clean = df[
        (df['tower_id'].notna()) &
        (df['timestamp'].notna()) &
        (df['signal_strength'].notna()) &
        (df['data_volume_mb'].notna()) &
        (df['data_volume_mb'] >= 0) &
        (df['signal_strength'].between(CONFIG['signal_bounds']['min'], CONFIG['signal_bounds']['max']))
    ].copy()
    # Remove duplicates
    dup_count = df_clean.duplicated(subset=['tower_id', 'timestamp']).sum()
    if dup_count > 0:
        logging.warning(f\"Found {dup_count} duplicates. Dropping.\")
        df_clean = df_clean.drop_duplicates(subset=['tower_id', 'timestamp'])
    final_count = len(df_clean)
    dq_report['initial_count'] = int(initial_count)
    dq_report['final_count'] = int(final_count)
    dq_report['removed_count'] = int(initial_count - final_count)
    # Write silver parquet
    out_buf = io.BytesIO()
    df_clean.to_parquet(out_buf, index=False)
    out_buf.seek(0)
    silver_key = f\"silver/network_metrics/date={processing_date}/cleaned_data.parquet\"
    upload_bytes_to_s3(s3_hook, out_buf.getvalue(), silver_key, CONFIG['target_bucket'], replace=True)
    if ti:
        ti.xcom_push(key='silver_path', value=silver_key)
        ti.xcom_push(key='dq_report', value=dq_report)
        ti.xcom_push(key='silver_record_count', value=final_count)
    logging.info(f\"Silver write complete: {silver_key} (records={final_count})\")
    return silver_key
