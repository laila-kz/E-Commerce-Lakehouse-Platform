"""
Airflow Orchestration DAG for E-Commerce Data Lakehouse Pipeline
Runs: Ingestion -> Bronze Stage (Transform + Gate) -> Silver Stage (Transform + Gate)
      -> dbt Warehouse Stage (Staging -> Core -> Marts) -> dbt Test Gate
"""

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)

# Import custom failure callback plugin
sys.path.append('/opt/airflow/plugins')
try:
    from slack_alert import slack_alert_on_failure
except ImportError:
    logger.warning(
        "slack_alert plugin not importable from /opt/airflow/plugins; "
        "on_failure_callback is a no-op and Slack/email failure alerts are disabled. "
        "Ensure airflow/plugins/slack_alert.py is mounted to enable alerts."
    )

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
    'max_retry_delay': timedelta(minutes=15),
    'on_failure_callback': slack_alert_on_failure,
}

# Per-group retry / backoff matrix (overrides default_args per task).
# External/network stages retry more; data-quality and test gates retry little
# because their failures are not transient.
RETRY_POLICY = {
    "ingest": {
        "retries": 4,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=8),
    },
    "spark_transform": {
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
    },
    "quality_gate": {
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": False,
    },
    "dbt_build": {
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
    "test_gate": {
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": False,
    },
}

SPARK_SUBMIT_PREFIX = (
    "docker exec spark /opt/spark/bin/spark-submit "
    "--driver-memory 2g --executor-memory 2g "
    "--jars /opt/spark/jars-extra/*.jar "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.lakehouse.optimizer.EcommerceSparkExtensions "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)

PYTHON_EXEC_PREFIX = "docker exec spark python3"

with DAG(
    'ecommerce_lakehouse',
    default_args=default_args,
    description='End-to-End E-Commerce Data Lakehouse ETL & Quality Pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Ingestion Stage
    ingest_raw = BashOperator(
        task_id='ingest_raw',
        bash_command=f'python {PROJECT_DIR}/ingestion/kaggle_ingest.py --data-dir {PROJECT_DIR}/data/demo_sample --force',
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(minutes=10),
        **RETRY_POLICY["ingest"],
    )

    # 2. Bronze Stage & Quality Gate Group
    with TaskGroup(group_id='bronze_stage', tooltip='Bronze transformation and Soda quality gate') as bronze_stage:
        bronze_transform = BashOperator(
            task_id='bronze_transform',
            bash_command=f'{SPARK_SUBMIT_PREFIX} /workspace/spark_jobs/bronze_transform.py',
            cwd=PROJECT_DIR,
            execution_timeout=timedelta(minutes=25),
            **RETRY_POLICY["spark_transform"],
        )
        bronze_quality_gate = BashOperator(
            task_id='bronze_quality_gate',
            bash_command=f'{PYTHON_EXEC_PREFIX} /workspace/checks/run_quality_gate.py --layer bronze',
            cwd=PROJECT_DIR,
            execution_timeout=timedelta(minutes=15),
            **RETRY_POLICY["quality_gate"],
        )
        bronze_transform >> bronze_quality_gate

    # 3. Silver Stage & Quality Gate Group
    with TaskGroup(group_id='silver_stage', tooltip='Silver MERGE deduplication and Soda quality gate') as silver_stage:
        silver_transform = BashOperator(
            task_id='silver_transform',
            bash_command=f'{SPARK_SUBMIT_PREFIX} /workspace/spark_jobs/silver_transform.py --batch-id {{{{ run_id }}}}',
            cwd=PROJECT_DIR,
            execution_timeout=timedelta(minutes=25),
            **RETRY_POLICY["spark_transform"],
        )
        silver_quality_gate = BashOperator(
            task_id='silver_quality_gate',
            bash_command=f'{PYTHON_EXEC_PREFIX} /workspace/checks/run_quality_gate.py --layer silver',
            cwd=PROJECT_DIR,
            execution_timeout=timedelta(minutes=15),
            **RETRY_POLICY["quality_gate"],
        )
        silver_transform >> silver_quality_gate

    # 4. dbt Gold Warehouse & Marts Modeling Stage
    with TaskGroup(group_id='dbt_warehouse_stage', tooltip='Dimensional modeling and analytical marts') as dbt_warehouse_stage:
        dbt_run_staging = BashOperator(
            task_id='dbt_run_staging',
            bash_command='dbt run --select staging --profiles-dir .',
            cwd=DBT_DIR,
            execution_timeout=timedelta(minutes=15),
            **RETRY_POLICY["dbt_build"],
        )
        dbt_run_intermediate = BashOperator(
            task_id='dbt_run_intermediate',
            bash_command='dbt run --select intermediate --profiles-dir .',
            cwd=DBT_DIR,
            execution_timeout=timedelta(minutes=15),
            **RETRY_POLICY["dbt_build"],
        )
        dbt_run_dims_facts = BashOperator(
            task_id='dbt_run_dims_facts',
            bash_command='dbt run --select core --profiles-dir .',
            cwd=DBT_DIR,
            execution_timeout=timedelta(minutes=15),
            **RETRY_POLICY["dbt_build"],
        )
        dbt_run_marts = BashOperator(
            task_id='dbt_run_marts',
            bash_command='dbt run --select marts --profiles-dir .',
            cwd=DBT_DIR,
            execution_timeout=timedelta(minutes=15),
            **RETRY_POLICY["dbt_build"],
        )
        dbt_run_staging >> dbt_run_intermediate >> dbt_run_dims_facts >> dbt_run_marts

    # 5. Unified dbt Test Invariant Validation Gate
    dbt_test_gate = BashOperator(
        task_id='dbt_test_gate',
        bash_command='dbt test --profiles-dir .',
        cwd=DBT_DIR,
        execution_timeout=timedelta(minutes=10),
        **RETRY_POLICY["test_gate"],
    )

    # Clean Pipeline Orchestration Flow
    ingest_raw >> bronze_stage >> silver_stage >> dbt_warehouse_stage >> dbt_test_gate
