load.py
    import logging, io
    import pandas as pd
    from airflow.exceptions import AirflowException
    from scripts.utils import get_s3_hook, upload_bytes_to_s3
    from config.settings import CONFIG
    from datetime import datetime

    def create_gold_aggregations(processing_date, ti=None):
        s3_hook = get_s3_hook(CONFIG['s3_conn_id'])
        silver_key = f\"silver/network_metrics/date={processing_date}/cleaned_data.parquet\"
        try:
            silver_bytes = s3_hook.read_key(silver_key, bucket_name=CONFIG['target_bucket'])
            silver_buf = io.BytesIO(silver_bytes.encode() if isinstance(silver_bytes, str) else silver_bytes)
            df = pd.read_parquet(silver_buf)
        except Exception as e:
            logging.error(f\"Failed reading silver parquet: {e}\")
            raise AirflowException(str(e))
        # hourly aggregation
        hourly = df.groupby(['date','hour','region']).agg(
            total_data_volume_mb = pd.NamedAgg(column='data_volume_mb', aggfunc='sum'),
            avg_signal_strength = pd.NamedAgg(column='signal_strength', aggfunc='mean'),
            record_count = pd.NamedAgg(column='tower_id', aggfunc='count')
        ).reset_index()
        hourly['load_timestamp'] = datetime.utcnow()
        # daily aggregation
        daily = df.groupby(['date','region']).agg(
            daily_data_volume_mb = pd.NamedAgg(column='data_volume_mb', aggfunc='sum'),
            daily_avg_signal_strength = pd.NamedAgg(column='signal_strength', aggfunc='mean'),
            daily_records = pd.NamedAgg(column='tower_id', aggfunc='count'),
            min_signal = pd.NamedAgg(column='signal_strength', aggfunc='min'),
            max_signal = pd.NamedAgg(column='signal_strength', aggfunc='max')
        ).reset_index()
        daily['load_timestamp'] = datetime.utcnow()
        daily['data_volume_gb'] = daily['daily_data_volume_mb'] / 1024
        # write to gold
        hourly_buf = io.BytesIO()
        hourly.to_parquet(hourly_buf, index=False)
        hourly_buf.seek(0)
        hourly_key = f\"gold/hourly_metrics/date={processing_date}/hourly_aggregations.parquet\"
        upload_bytes_to_s3(s3_hook, hourly_buf.getvalue(), hourly_key, CONFIG['target_bucket'], replace=True)
        daily_buf = io.BytesIO()
        daily.to_parquet(daily_buf, index=False)
        daily_buf.seek(0)
        daily_key = f\"gold/daily_summary/date={processing_date}/daily_aggregations.parquet\"
        upload_bytes_to_s3(s3_hook, daily_buf.getvalue(), daily_key, CONFIG['target_bucket'], replace=True)
        if ti:
            ti.xcom_push(key='gold_hourly_path', value=hourly_key)
            ti.xcom_push(key='gold_daily_path', value=daily_key)
        logging.info(f\"Gold write complete: {hourly_key}, {daily_key}\")
        return hourly_key, daily_key