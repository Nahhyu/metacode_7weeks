import pendulum

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def say_hello():
    return "Hello Airflow"


def say_goodbye():
    return "Goodbye Airflow"


with DAG(
    dag_id="sample_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=False,
):
    start = EmptyOperator(task_id="start")

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=say_hello,
        show_return_value_in_logs=True,
    )
    goodbye_task = PythonOperator(
        task_id="goodbye_task",
        python_callable=say_goodbye,
        show_return_value_in_logs=True,
    )

    end = EmptyOperator(task_id="end")

    start >> hello_task >> goodbye_task >> end
