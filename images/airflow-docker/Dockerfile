ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.0.0-python3.6"
FROM ${AIRFLOW_BASE_IMAGE}

RUN pip install --user --no-cache-dir \
    apache-airflow-providers-docker==1.2.0

USER root
ENV PYTHONPATH=/home/airflow/.local/lib/python3.6/site-packages
