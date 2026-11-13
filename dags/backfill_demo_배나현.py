from pathlib import Path

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, get_current_context
from airflow.timetables.trigger import CronTriggerTimetable


def write_daily_file():
    logical_date = get_current_context()["logical_date"]
    day = pendulum.instance(logical_date).in_timezone("Asia/Seoul").to_date_string()

    output_dir = Path("/opt/airflow/logs/q7_backfill")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"daily_{day}.txt"
    output_file.write_text(f"logical_date={day}\n", encoding="utf-8")


with DAG(
    dag_id="backfill_demo_배나현",
    start_date=pendulum.datetime(2026, 11, 6, tz="Asia/Seoul"),
    schedule=CronTriggerTimetable("0 0 * * *", timezone="Asia/Seoul"),
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=True,
):
    PythonOperator(
        task_id="write_daily_file",
        python_callable=write_daily_file,
    )