"""Master orchestration: generator -> bronze -> silver -> gold -> dbt -> GE."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="hmis_medallion_daily",
    description="HMIS daily medallion + reconciliation + dbt + GE checkpoint",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 2,
                  "retry_delay": timedelta(minutes=5)},
    tags=["hmis", "medallion"],
) as dag:

    seed = BashOperator(task_id="seed",
        bash_command="python -m generator.run --clients 2000 --projects 40")
    bronze = BashOperator(task_id="bronze",
        bash_command="python notebooks/bronze/batch_load_hmis_csvs.py")
    silver = BashOperator(task_id="silver",
        bash_command="python notebooks/silver/conform_and_mask.py")
    gold = BashOperator(task_id="gold",
        bash_command="python notebooks/gold/build_star_schema.py")
    reconcile = BashOperator(task_id="reconcile",
        bash_command="python great_expectations/reconciliation.py")
    dbt_run = BashOperator(task_id="dbt_run",
        bash_command="cd pipelines/dbt && dbt run --target prod && dbt test --target prod")

    seed >> bronze >> silver >> gold >> reconcile >> dbt_run
