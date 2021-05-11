from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "email": ["airflow@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "10_docker",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=days_ago(0, 2),
) as dag:

    t1 = BashOperator(task_id="print_date", bash_command="date")
    t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3)
    t3 = DockerOperator(
        command="/bin/sleep 30",
        image="centos:latest",
        network_mode="bridge",
        task_id="docker_op_tester",
        dag=dag,
        do_xcom_push=False,
    )

    t4 = BashOperator(task_id="print_hello", bash_command='echo "hello world!!!"')

    t1 >> t2
    t1 >> t3
    t3 >> t4
