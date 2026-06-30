import requests
import logging
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.decorators import dag, task


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(hours=4)).replace(
        minute=0, second=0
    ),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=25),
}


@dag(
    default_args=default_args,
    tags=["wmes", "CWMS", "USGS", "Timeseries"],
    schedule="0 6 * * 6",
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
    doc_md=__doc__,
)
def check_ip():
    @task()
    def get_public_ip():
        ip = requests.get(
            "https://checkip.amazonaws.com",
            timeout=10
        ).text.strip()

        logging.info(f"Current public IP address: {ip}")
    
    get_public_ip()
    
DAG_ = check_ip()