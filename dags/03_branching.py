import airflow

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

CHANGE_DATE = airflow.utils.dates.days_ago(1)


def _pick_branch(execution_date):
    if execution_date < CHANGE_DATE:
        return "fetch_dataset_old"
    else:
        return "fetch_dataset_new"


def _fetch_dataset_old():
    print("Fetching data (OLD)...")


def _fetch_dataset_new():
    print("Fetching data (NEW)...")


with DAG(
    dag_id="03_branching",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")

    pick_branch = BranchPythonOperator(
        task_id="pick_branch", python_callable=_pick_branch
    )

    fetch_dataset_old = PythonOperator(
        task_id="fetch_dataset_old", python_callable=_fetch_dataset_old
    )

    fetch_dataset_new = PythonOperator(
        task_id="fetch_dataset_new", python_callable=_fetch_dataset_new
    )

    fetch_another_dataset = DummyOperator(
        task_id="fetch_another_dataset"
    )

    join_datasets = DummyOperator(task_id="join_datasets", trigger_rule="none_failed")

    train_model = DummyOperator(task_id="train_model")
    deploy_model = DummyOperator(task_id="deploy_model")

    start >> pick_branch
    pick_branch >> [fetch_dataset_old, fetch_dataset_new]
    [fetch_dataset_old, fetch_dataset_new, fetch_another_dataset] >> join_datasets
    join_datasets >> train_model >> deploy_model
