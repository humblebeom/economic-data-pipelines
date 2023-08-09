ARG AIRFLOW_BASE_IMAGE="apache/airflow:latest-python3.11"
FROM ${AIRFLOW_BASE_IMAGE}

RUN pip install --user --no-cache-dir \
    apache-airflow-providers-postgres==1.0.0 \ 
    pandas==2.0.3 \
    openpyxl==3.1.2