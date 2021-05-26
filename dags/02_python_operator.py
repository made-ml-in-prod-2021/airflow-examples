import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _get_pictures():

    pathlib.Path("/opt/airflow/data/images").mkdir(parents=True, exist_ok=True)

    # Download all pictures in launches.json
    with open("/opt/airflow/data/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/opt/airflow/data/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")


with DAG(
    dag_id="02_python_operator_try_change_name",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
) as dag:

    download_launches = BashOperator(
        task_id="download_launches",
        bash_command="curl -o /opt/airflow/data/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
        dag=dag,
    )

    get_pictures = PythonOperator(
        task_id="get_pictures", python_callable=_get_pictures,
    )

    notify = BashOperator(
        task_id="notify",
        bash_command='echo "There are now $(ls /opt/airflow/data/images/ | wc -l) images."',
    )

    download_launches >> get_pictures >> notify
