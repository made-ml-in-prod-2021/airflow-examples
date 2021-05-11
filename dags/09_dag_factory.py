import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator


def generate_dag(dataset_name):
    with DAG(
        dag_id=f"dag_factory_{dataset_name}",
        start_date=airflow.utils.dates.days_ago(5),
        schedule_interval="@daily",
    ) as dag:
        generate_set_of_tasks(dataset_name)
    return dag


def generate_set_of_tasks(dataset_name):
    download_task = DummyOperator(task_id=f"download_{dataset_name}")
    preprocess_task = DummyOperator(task_id=f"preprocess_{dataset_name}")
    download_task >> preprocess_task


for dataset in ["dataset_1", "dataset_2"]:
    globals()[f"09_dag_factory_{dataset}"] = generate_dag(dataset_name=dataset)
