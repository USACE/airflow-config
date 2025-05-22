from datetime import datetime, timedelta, timezone
from dataacquisition.getUSGS_ratings_CDA import getusgs_rating_cda
from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.models import Variable

APIKEY = Variable.get("API_KEY")
APIROOT = Variable.get("CDA_URL")
DAYSBACK = 3


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(hours=8)).replace(
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
    tags=["wmes", "CWMS", "USGS", "Ratings"],
    schedule="30 0,6,12,18 * * *",
    max_active_runs=1,
    max_active_tasks=4,
    catchup=False,
    doc_md=__doc__,
)
def cwms_usgs_ratings():
    with TaskGroup(group_id="USGS_ratings"):
        office_ids = ["LRL", "MVP"]
        for office_id in office_ids:

            @task(task_id=f"{office_id}_cwms_usgs_rating_byoffice")
            def cwms_usgs_ratings_byoffice(office_id):
                getusgs_rating_cda(
                    api_root=APIROOT,
                    office_id=office_id,
                    days_back=DAYSBACK,
                    api_key=APIKEY,
                )

            cwms_usgs_ratings_byoffice(office_id)


DAG_ = cwms_usgs_ratings()
