"""Supervisor DAG: ensures the Kafka -> Delta stream is running. If not, restart."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="hmis_streaming_supervisor",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["hmis", "streaming"],
) as dag:
    BashOperator(
        task_id="ensure_consumer",
        bash_command="pgrep -f stream_to_delta.py || nohup python streaming/consumer/stream_to_delta.py &",
    )
