from datetime import datetime, timedelta, timezone
#from cwmscli.usgs.getusgs_cda import getusgs_cda
from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.models import Variable
from helpers.batch import get_office_groups
from cwmscli.usgs.getusgs_cda import getusgs_cda
from airflow import DAG

APIKEY = Variable.get("API_KEY")
APIROOT = Variable.get("CDA_URL")
OFFICES = Variable.get("USGS_TS_OFFICES").split(",")
DAYSBACK = float(Variable.get("USGS_TS_DAYS_BACK", default_var=0.5))

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
    schedule="@hourly",
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
    doc_md=__doc__,
)
def cwms_usgs_timeseries():
    groups = get_office_groups(OFFICES)
    for group_name, configs in groups.items():
        print(f"Processing group: {group_name} with configs: {configs}")
        with TaskGroup(group_id=group_name) as tg:
            for jc in configs:
                @task(task_id=f"{jc['office']}_cwms_usgs_ts")
                def cwms_usgs_ts(job_config):
                    getusgs_cda(
                        api_root=APIROOT,
                        office_id=job_config["office"].upper(),
                        days_back=DAYSBACK,
                        api_key=APIKEY,
                    )

                cwms_usgs_ts(jc)


DAG_ = cwms_usgs_timeseries()
