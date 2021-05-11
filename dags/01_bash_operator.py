from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="01_bash_operator",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="touch_file_1",
        bash_command=f"touch /opt/airflow/data/1.txt",
    )

    t2 = BashOperator(
        task_id="sleep", bash_command="sleep 5",
    )

    t3 = BashOperator(
        task_id="touch_file_2",
        depends_on_past=False,
        bash_command=f"touch /opt/airflow/data/2.txt",
    )

    t1 >> [t2, t3]
