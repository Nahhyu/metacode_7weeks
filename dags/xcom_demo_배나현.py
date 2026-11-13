import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

logger = logging.getLogger(__name__)


def count_lines(ti):
    if ti.try_number == 1:
        raise RuntimeError("Q6: intentional failure on the first attempt")

    source = Path(__file__).with_name("sample_dag.py")
    with source.open(encoding="utf-8") as file:
        return sum(1 for line in file if line.strip())


def use_value(ti):
    line_count = ti.xcom_pull(task_ids="count_lines", key="return_value")
    logger.info("Received non-empty line count from XCom: %s", line_count)


with DAG(
    dag_id="xcom_demo_배나현",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
):
    count_task = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines,
        retries=2,
        retry_delay=timedelta(seconds=15),
        do_xcom_push=True,
    )

    use_task = PythonOperator(
        task_id="use_value",
        python_callable=use_value,
    )

    count_task >> use_task