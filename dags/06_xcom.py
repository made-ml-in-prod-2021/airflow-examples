import uuid

import airflow

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


def _train_model(**context):
    model_id = str(uuid.uuid4())
    context["task_instance"].xcom_push(key="model_id", value=model_id)


def _deploy_model(**context):
    model_id = context["task_instance"].xcom_pull(
        task_ids="train_model", key="model_id"
    )
    print(f"Deploying model {model_id}")


with DAG(
    dag_id="06_xcom",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")

    fetch_dataset = DummyOperator(task_id="fetch_sales")
    fetch_another_dataset = DummyOperator(task_id="fetch_weather")
    process_dataset = DummyOperator(task_id="process_dataset")
    process_another_dataset = DummyOperator(task_id="process_another_dataset")
    join_datasets = DummyOperator(task_id="join_datasets")

    train_model = PythonOperator(task_id="train_model", python_callable=_train_model)

    deploy_model = PythonOperator(task_id="deploy_model", python_callable=_deploy_model)

    start >> [fetch_dataset, fetch_another_dataset]
    fetch_dataset >> process_dataset
    fetch_another_dataset >> process_another_dataset

    [process_another_dataset, process_dataset] >> join_datasets
    join_datasets >> train_model >> deploy_model
