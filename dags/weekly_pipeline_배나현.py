import logging
import os
import tempfile
from pathlib import Path

import boto3
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

logger = logging.getLogger(__name__)
BRONZE_KEY = "bronze/netflix_titles.csv"


def download_csv():
    bucket = os.environ["Q9_S3_BUCKET"]
    run_dir = Path(tempfile.mkdtemp(prefix="week7-q9-"))
    input_csv = run_dir / "netflix_titles.csv"

    boto3.client("s3").download_file(bucket, BRONZE_KEY, str(input_csv))
    logger.info(
        "Downloaded s3://%s/%s to %s (%d bytes)",
        bucket, BRONZE_KEY, input_csv, input_csv.stat().st_size,
    )

    return {
        "run_dir": str(run_dir),
        "day": pendulum.now("Asia/Seoul").to_date_string(),
    }


def upload_silver(ti):
    bucket = os.environ["Q9_S3_BUCKET"]
    info = ti.xcom_pull(task_ids="download_csv", key="return_value")
    output_dir = Path(info["run_dir"]) / "parquet"
    prefix = f"silver/{info['day']}/"
    parquet_files = sorted(output_dir.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files in {output_dir}")

    s3 = boto3.client("s3")
    uploaded_keys = set()
    for file in parquet_files:
        key = f"{prefix}{file.name}"
        s3.upload_file(str(file), bucket, key)
        uploaded_keys.add(key)

    confirmed = set()
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    ):
        for obj in page.get("Contents", []):
            if obj["Key"] in uploaded_keys:
                confirmed.add(obj["Key"])
                logger.info("s3://%s/%s (%d bytes)", bucket, obj["Key"], obj["Size"])

    if confirmed != uploaded_keys:
        raise RuntimeError("S3 listing did not contain every uploaded file")

    logger.info("Uploaded Parquet objects: %d", len(confirmed))


with DAG(
    dag_id="weekly_pipeline_배나현",
    start_date=pendulum.datetime(2026, 11, 14, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
):
    download = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
    )

    transform = BashOperator(
        task_id="transform",
        append_env=True,
        env={
            "RUN_DIR": "{{ ti.xcom_pull(task_ids='download_csv')['run_dir'] }}",
            "MIN_YEAR": "2015",
        },
        bash_command=(
            'set -euo pipefail; '
            'spark-submit --master "local[2]" '
            '/opt/airflow/dags/jobs/transform.py '
            '"$RUN_DIR/netflix_titles.csv" "$RUN_DIR/parquet" '
            '--min-year "$MIN_YEAR"'
        ),
    )

    upload = PythonOperator(
        task_id="upload_silver",
        python_callable=upload_silver,
    )

    download >> transform >> upload