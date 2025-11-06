import logging
import pandas as pd
import numpy as np
from .utils import now_iso

def check_required_columns(df, required_cols):
    missing = [c for c in required_cols if c not in df.columns]
    return missing

def null_rate_report(df, cols):
    total = len(df)
    report = {}
    for c in cols:
        nulls = df[c].isnull().sum()
        pct = (nulls / total * 100) if total>0 else 0
        report[c] = {'nulls': int(nulls), 'pct': float(pct)}
    return report

def detect_volume_anomalies(df, col='data_volume_mb', multiplier=3.0):
    ser = df[col].dropna()
    if ser.empty:
        return df.iloc[0:0]
    q1 = ser.quantile(0.25)
    q3 = ser.quantile(0.75)
    iqr = q3 - q1
    upper = q3 + multiplier * iqr
    anomalies = df[(df[col].isna()) | (df[col] < 0) | (df[col] > upper)]
    return anomalies

def data_quality_summary(df, required_cols, signal_bounds):
    report = {}
    missing = check_required_columns(df, required_cols)
    report['missing_columns'] = missing
    report['null_rates'] = null_rate_report(df, required_cols)
    report['anomalies_sample'] = detect_volume_anomalies(df).head(5).to_dict(orient='records') if 'data_volume_mb' in df.columns else []
    # signal range check
    if 'signal_strength' in df.columns:
        out_of_range = df[~df['signal_strength'].between(signal_bounds['min'], signal_bounds['max'])]
        report['signal_out_of_range_count'] = int(len(out_of_range))
    report['timestamp'] = now_iso()
    return report