"""
Airflow Orchestration DAG for E-Commerce Data Lakehouse Pipeline
Runs: Ingestion -> Bronze Transform -> Bronze Quality Gate -> Silver Transform
      -> Silver Quality Gate -> dbt (Staging -> Core Dims/Facts -> Marts) -> dbt Tests
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Import custom failure callback plugin
sys.path.append('/opt/airflow/plugins')
try:
    from slack_alert import slack_alert_on_failure
except ImportError:
    # Fallback placeholder if plugin path differs locally
    def slack_alert_on_failure(context):
        pass

PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt"

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'on_failure_callback': slack_alert_on_failure,
}

dag = DAG(
    'ecommerce_lakehouse',
    default_args=default_args,
    description='End-to-End E-Commerce Data Lakehouse ETL & Quality Pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
)

SPARK_SUBMIT_PREFIX = (
    "docker exec spark /opt/spark/bin/spark-submit "
    "--driver-memory 2g --executor-memory 2g "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)

# Quality gate runner is plain Python (not a Spark job) — use python3 directly
PYTHON_EXEC_PREFIX = "docker exec spark python3"

# 1. Ingestion Layer
ingest_raw = BashOperator(
    task_id='ingest_raw',
    bash_command=f'python {PROJECT_DIR}/ingestion/kaggle_ingest.py --data-dir {PROJECT_DIR}/data/demo_sample --force',
    cwd=PROJECT_DIR,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

# 2. Bronze Transform
bronze_transform = BashOperator(
    task_id='bronze_transform',
    bash_command=f'{SPARK_SUBMIT_PREFIX} /workspace/spark_jobs/bronze_transform.py',
    cwd=PROJECT_DIR,
    execution_timeout=timedelta(minutes=25),
    dag=dag,
)

# 3. Bronze Quality Gate (MUST PASS)
bronze_quality_gate = BashOperator(
    task_id='bronze_quality_gate',
    bash_command=f'{PYTHON_EXEC_PREFIX} /workspace/checks/run_quality_gate.py --layer bronze',
    cwd=PROJECT_DIR,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

# 4. Silver Transform (Incremental with watermark & SHA-256 deduplication)
silver_transform = BashOperator(
    task_id='silver_transform',
    bash_command=f'{SPARK_SUBMIT_PREFIX} /workspace/spark_jobs/silver_transform.py --batch-id {{{{ run_id }}}}',
    cwd=PROJECT_DIR,
    execution_timeout=timedelta(minutes=25),
    dag=dag,
)

# 5. Silver Quality Gate (MUST PASS)
silver_quality_gate = BashOperator(
    task_id='silver_quality_gate',
    bash_command=f'{PYTHON_EXEC_PREFIX} /workspace/checks/run_quality_gate.py --layer silver',
    cwd=PROJECT_DIR,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

# 6. dbt Layer Tasks
dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command='dbt run --select staging --profiles-dir .',
    cwd=DBT_DIR,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

dbt_run_intermediate = BashOperator(
    task_id='dbt_run_intermediate',
    bash_command='dbt run --select intermediate --profiles-dir .',
    cwd=DBT_DIR,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

dbt_run_dims_facts = BashOperator(
    task_id='dbt_run_dims_facts',
    bash_command='dbt run --select core --profiles-dir .',
    cwd=DBT_DIR,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

dbt_run_marts = BashOperator(
    task_id='dbt_run_marts',
    bash_command='dbt run --select marts --profiles-dir .',
    cwd=DBT_DIR,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

dbt_test_full = BashOperator(
    task_id='dbt_test_full',
    bash_command='dbt test --profiles-dir .',
    cwd=DBT_DIR,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

# Pipeline Task Dependencies
(
    ingest_raw
    >> bronze_transform
    >> bronze_quality_gate
    >> silver_transform
    >> silver_quality_gate
    >> dbt_run_staging
    >> dbt_run_intermediate
    >> dbt_run_dims_facts
    >> dbt_run_marts
    >> dbt_test_full
)
