# Telecom Network Metrics Pipeline

## Overview
This project simulates an **end-to-end data pipeline** for a telecom company collecting daily tower performance metrics. It ingests raw CSVs, validates them, performs transformations, applies data quality checks, and generates aggregated business summaries.

The data is processed in three layers (Bronze → Silver → Gold) following the **Medallion architecture**, orchestrated using **Apache Airflow**, processed with **Python/Pandas**, and stored as **Parquet** for efficient analytics.



## Architecture & Design Choices

### Assumptions

- One CSV per day ("network_metrics_YYYYMMDD.csv)
- Each tower generates hourly metrics
- S3 bucket → "raw-telecom-network-data"
- Regions are static ("North", "South", "East", "West")
- Airflow runs daily at midnight UTC

### Bronze Layer – Raw Zone
- **Purpose:** Immutable landing zone storing raw data exactly as received.  
- **Data Stored As:** Parquet for efficiency (smaller, faster reads than CSV).  
- **Content:**  
  - Original columns + metadata ("ingestion_timestamp", "source_file", "processing_date").  
  - Duplicates and invalid rows are *not removed* here.  
- **Rationale:**  
  - Enables reprocessing and auditing.  
  - Guarantees that no raw data is lost.  
  - Parquet chosen for columnar compression and schema consistency.

### Silver Layer – Cleaned Zone
- **Purpose:** Clean, validate, and standardize data for trusted analysis.  
- **Transformations Applied:**  
  - Schema and datatype enforcement.  
  - Convert Unix timestamps → human-readable datetime.  
  - Normalize "region" names.  
  - Remove duplicates and invalid entries (e.g., negative "data_volume_mb", out-of-range "signal_strength").  
- **Rationale:**  
  - Creates a single “source of truth.”  
  - Ensures clean data for analytics.  
  - Simplifies downstream aggregations and anomaly checks.

### Gold Layer – Aggregated Zone
- **Purpose:** Provide **business-ready metrics** for reporting and dashboards.  
- **Aggregations:**  
  - **Hourly:** total "data_volume_mb", average "signal_strength" by region/hour.  
  - **Daily:** total and average metrics by region.  
- **Rationale:**  
  - Matches operations team requirements.  
  - Reduces query cost and improves dashboard performance.

### Why Parquet?

- **Columnar compression** Reduces S3 storage costs. 
- **Faster reads:** Ideal for aggregations and selective queries.  
- **Schema support:** Ensures consistency across processing stages. 


Using Parquet makes downstream queries (e.g., daily KPIs) faster and cheaper, especially when integrating with Superset dashboards.



## Data Quality (DQ) Design

###  DQ Checks Implemented

| Type | Purpose | Implementation |
|------|----------|----------------|
| **File-level check** | Ensure daily CSV exists and is readable | Check file presence in S3 bucket |
| **Schema validation** | Confirm expected columns & dtypes | Compare Pandas schema |
| **Null check** | Detect missing values | "df.dropna()" + warning log |
| **Duplicate check** | Remove redundant entries | "df.drop_duplicates()" |
| **Freshness check** | Ensure data not stale | Compare latest timestamp vs current date |


Each check logs structured output (with timestamps), and **raises an exception** if a critical failure occurs (e.g., missing file or wrong schema).


### How Failures Are Handled
- **Critical issues:** DAG fails, Airflow sends alert.  
- **Minor issues:** Logged in validation reports and quarantined for review.  
- **Quality metrics:** Stored in XCom and logs for trend monitoring.


 
  


## Task Breakdown

| Step | Script | Description |
|------|---------|-------------|
| **Extract (Bronze)** | "extract.py" | Load daily raw CSV from S3 |
| **Transform (Silver)** | "transform.py" | Clean, deduplicate, and convert timestamps |
| **Load (Gold)** | "load.py" | Aggregate by region/hour/day |
| **Data Quality** | "data_quality.py" | Run DQ checks and log results |
| **Airflow DAG** | "pipeline.py" | Orchestrates entire ETL pipeline |









