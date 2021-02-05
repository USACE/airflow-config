FROM apache/airflow:master-python3.8

COPY dags/* /opt/airflow/dags/
COPY plugins/* /opt/airflow/plugins/