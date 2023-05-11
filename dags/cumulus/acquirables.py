"""
Acquire and Process ABRFC 01h
"""

from collections import OrderedDict
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.utcnow(),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    default_args=default_args,
    schedule="0 5,23 * * *",
    tags=["cumulus", "acquirables"],
    max_active_runs=1,
    max_active_tasks=1,
)
def cumulus_acquirables():
    @task()
    def get_acquirables():
        context = get_current_context()
        ti = context["ti"]

        conn = cumulus.get_connection()

        h = HttpHook(http_conn_id=conn.conn_id, method="GET")
        endpoint = f"/acquirables?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        resp = h.run(endpoint=endpoint, headers=headers)

        resp_json = resp.json()

        return resp_json

    @task()
    def set_acquirables(xcom):
        acq_file = Path("/opt/airflow/data/acquirables.csv")
        with acq_file.open(mode="w", encoding="utf-8") as fp:
            for x in xcom:
                xorder = OrderedDict(sorted(x.items(), key=lambda v: v[0], reverse=True))
                linex = ", ".join(xorder.values())
                fp.write(linex + "\n")

    set_acquirables(get_acquirables())


DAG_ = cumulus_acquirables()
