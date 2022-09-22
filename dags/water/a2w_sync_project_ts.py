import json
from datetime import datetime, timedelta
import string
from xml.dom import NotFoundErr

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException

from helpers.radar import get_timeseries as get_radar_timeseries
from helpers.water import get_cwms_timeseries as get_a2w_cwms_timeseries
from helpers.water import post_cwms_timeseries as post_a2w_cwms_timeseries

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=6)).replace(minute=0, second=0),
    # "start_date": datetime(2021, 4, 4),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    tags=["a2w"],
    schedule_interval="8 * * * *",
    max_active_runs=2,
    max_active_tasks=4,
    catchup=False,
    description="A simple DAG",
)
def a2w_sync_project_ts():
    """Comments here"""

    @task
    def get_a2w_config(office):

        r = get_a2w_cwms_timeseries(provider=office, datasource_type="cwms-timeseries")
        if len(r) == 0:
            raise AirflowSkipException

        return json.dumps(r)

    def create_task_group(**kwargs):
        office = kwargs["office"]

        with TaskGroup(group_id=f"{office}") as task_group:

            # Simplify the config payload to tsids only for
            # querying against RADAR
            @task(task_id=f"prep_{office}_tsids")
            def prep_tsids(office, config):
                tsids = []
                data = json.loads(config)
                for obj in data:
                    if obj["provider"].lower() == office.lower():
                        tsids.append(obj["key"])
                return tsids

            @task(task_id=f"extract_{office}_from_radar")
            def extract(ts_list):

                if isinstance(ts_list, str):
                    ts_list = [ts_list]

                # ts_list = json.loads(ts_list)
                print(ts_list)
                print(len(ts_list))
                print(type(ts_list))

                # extract the latest value

                logical_date = get_current_context()["logical_date"]
                begin = logical_date.strftime("%Y-%m-%dT%H:%M")
                end = (logical_date + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")

                if len(ts_list) == 0:
                    raise AirflowSkipException

                # begin = "2022-09-06T14:00"
                # end = "2022-09-06T16:00"
                r = get_radar_timeseries(ts_list, begin, end, office)
                r = json.loads(r)
                print(r)

                # Let's dig out the minimal data we need.
                # Minimize clutter in xcoms

                # Grab the time-series list object which can be iterated over
                # when multiple tsids are requested
                ts_obj_list = r["time-series"]["time-series"]

                return_obj_list = []

                for ts_obj in ts_obj_list:
                    tsid = ts_obj["name"]
                    ts_office = ts_obj["office"]
                    x = ts_obj["regular-interval-values"]["segments"][0]
                    ts_latest_data = [
                        x["last-time"],
                        x["values"][x["value-count"] - 1][0],
                    ]
                    return_obj = {}
                    return_obj["office"] = ts_office
                    return_obj["tsid"] = tsid
                    return_obj["latest"] = ts_latest_data
                    return_obj_list.append(return_obj)

                return return_obj_list

            @task(task_id=f"load_{office}_into_a2w")
            def load(office_data):

                # data = []
                # for x in office_data:
                #     # print(x)
                #     if len(x) > 0:
                #         # print(f"adding {x[0]}")
                #         data.append(x[0])

                a2w_payload = []

                for item in office_data:

                    # it may be possible for the extract task to return an empty
                    # list if the tsid was not valid (not found in RADAR).
                    # Ensure erray is not empty before trying to extract items
                    if len(item) > 0:
                        i = item[0]

                        obj = {}
                        # print(item["office"])
                        # print(item["tsid"])
                        obj["provider"] = i["office"].lower()
                        obj["datasource_type"] = "cwms-timeseries"
                        obj["key"] = i["tsid"]
                        obj["measurements"] = {
                            "times": [i["latest"][0]],
                            "values": [i["latest"][1]],
                        }
                        a2w_payload.append(obj)

                        # Post to the A2W API
                        post_a2w_cwms_timeseries(a2w_payload)

                return

            # extract(prep_tsids(office=office, config=get_a2w_config((office))))

            office_data = extract.expand(
                ts_list=prep_tsids(office=office, config=get_a2w_config((office)))
            )

            load(office_data)

            return task_group

    # @task
    # def get_project_ts_from_radar(arg):
    #     print(list(arg))

    # get_project_ts_from_radar.expand(arg=get_a2w_config())
    # consumer(arg=make_list())

    # get_a2w_config()

    _ = [create_task_group(office=office) for office in ["LRH", "LRN", "MVP"]]


project_ts_ydag = a2w_sync_project_ts()
