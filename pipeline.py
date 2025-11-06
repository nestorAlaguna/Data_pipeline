from datetime import datetime, timedelta
from airflow import DAG
from from airflow.providers.standard.operators.python.PythonOperator import PythonOperator
from extract import validate_source_file, ingest_to_bronze
from transform import clean_and_write_silver
from load import create_gold_aggregations

default_args = {
    'owner': 'telecom-data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def dag_failure_alert(context):
    # simple failure callback placeholder - integrate Slack/PagerDuty here
    dag_id = context.get('dag').dag_id if context.get('dag') else 'unknown'
    task_id = context.get('task_instance').task_id if context.get('task_instance') else 'unknown'
    print(f"DAG failure: dag={dag_id}, task={task_id}")

with DAG(
    dag_id='telecom_network_pipeline_modular',
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule='0 2 * * *',        # <- use `schedule` instead of schedule_interval
    catchup=False,
    max_active_runs=1,
    on_failure_callback=dag_failure_alert
) as dag:

    validate = PythonOperator(
        task_id='validate_source_file',
        python_callable=validate_source_file,
        op_kwargs={'execution_date': '{{ ds }}'},  # pass ds template (string); the function handles parsing
    )

    bronze = PythonOperator(
        task_id='ingest_to_bronze',
        python_callable=ingest_to_bronze,
        op_kwargs={  # prefer to pass minimal args; the callable can pull XCom if needed
            'source_file': '{{ ti.xcom_pull(task_ids="validate_source_file", key="source_file") }}',
            'processing_date': '{{ ti.xcom_pull(task_ids="validate_source_file", key="processing_date") }}'
        }
    )

    silver = PythonOperator(
        task_id='clean_and_write_silver',
        python_callable=clean_and_write_silver,
        op_kwargs={'processing_date': '{{ ti.xcom_pull(task_ids="validate_source_file", key="processing_date") }}'}
    )

    gold = PythonOperator(
        task_id='create_gold_aggregations',
        python_callable=create_gold_aggregations,
        op_kwargs={'processing_date': '{{ ti.xcom_pull(task_ids="validate_source_file", key="processing_date") }}'}
    )

    validate >> bronze >> silver >> gold
