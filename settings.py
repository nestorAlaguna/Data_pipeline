# settings.py
# Centralized configuration management for the pipeline.
# All configurable parameters in one place for easy maintenance.


CONFIG = {
    'source_bucket': 'raw-telecom-network-data',
    'target_bucket': 'processed-telecom-data',
    'file_pattern': r'^network_metrics_\\d{8}\\.csv$',
    's3_conn_id': 'aws_default',
    # DQ thresholds
    'dq': {
        'null_threshold_pct': 5.0,    # warn if > 5% nulls in required cols
        'anomaly_iqr_multiplier': 3.0
    },
    # Reasonable signal strength bounds (dBm)
    'signal_bounds': { 'min': -150, 'max': 50 }
}