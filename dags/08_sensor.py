from datetime import timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor

from airflow.utils.dates import days_ago


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _wait_for_file():
    return os.path.exists("/opt/airflow/data/wait.txt")


with DAG(
    "08_sensor",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),
) as dag:
    t1 = BashOperator(
        task_id="touch_file_1", bash_command="touch /opt/airflow/data/1.txt",
    )

    wait = PythonSensor(
        task_id="wait_for_file",
        python_callable=_wait_for_file,
        timeout=6000,
        poke_interval=10,
        retries=100,
        mode="poke",
    )

    t3 = BashOperator(
        task_id="touch_file_3",
        depends_on_past=True,
        bash_command="touch /opt/airflow/data/2.txt",
    )

    t1 >> wait >> t3
