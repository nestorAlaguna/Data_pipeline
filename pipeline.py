from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract import validate_source_file, ingest_to_bronze
from scripts.transform import clean_and_write_silver
from scripts.load import create_gold_aggregations

default_args = {
    'owner': 'telecom-data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='telecom_network_pipeline_modular',
    default_args=default_args,
    start_date=datetime(2025,7,1),
    schedule_interval='0 2 * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    validate = PythonOperator(
        task_id='validate_source_file',
        python_callable=validate_source_file,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    bronze = PythonOperator(
        task_id='ingest_to_bronze',
        python_callable=ingest_to_bronze,
        op_kwargs={'source_file': '{{ ti.xcom_pull(task_ids=\"validate_source_file\", key=\"source_file\") }}',
                    'processing_date': '{{ ti.xcom_pull(task_ids=\"validate_source_file\", key=\"processing_date\") }}'
                    },
    )

    silver = PythonOperator(
        task_id='clean_and_write_silver',
        python_callable=clean_and_write_silver,
        op_kwargs={'processing_date': '{{ ti.xcom_pull(task_ids=\"validate_source_file\", key=\"processing_date\") }}'}
    )

    gold = PythonOperator(
        task_id='create_gold_aggregations',
        python_callable=create_gold_aggregations,
        op_kwargs={'processing_date': '{{ ti.xcom_pull(task_ids=\"validate_source_file\", key=\"processing_date\") }}'}
    )
