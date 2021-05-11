import os
import json

import airflow.utils.dates
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _get_data(
    year: str,
    month: str,
    day: str,
    hour: str,
    output_dir: str,
    task_instance: TaskInstance,
    execution_date,
):
    print(execution_date)
    print("year", year)
    print("month", month)
    print("day", day)
    print("hour", hour)

    result = {"wow": f"{year}/{month}/{day}/{hour}", "task_id": task_instance.task_id}
    output = os.path.join(output_dir, f"{year}_{month}_{day}_{hour}.json")
    with open(output, "w") as f:
        json.dump(result, f)


with DAG(
    dag_id="07_templating",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    max_active_runs=1,
) as dag:
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
        op_kwargs={
            "year": "{{ execution_date.year }}",
            "month": "{{ execution_date.month }}",
            "day": "{{ execution_date.day }}",
            "hour": "{{ execution_date.hour }}",
            "output_dir": "/opt/airflow/data",
        }
    )

    bash_example = BashOperator(
        task_id="bash_command", bash_command="echo {{ ds }}",
    )

    bash_example >> get_data
