"""
National Blend of Models

Returns
-------
Airflow DAG
    Directed Acyclic Graph
"""
import re
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from pathlib import Path

import helpers.cumulus as cumulus
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from helpers.downloads import trigger_download

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=2)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

URL_NOMADS = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/v4.1"

S3_ACQUIRABLE = "nbm-conus"

MAX_HOUR = 265

IndexAttributes = namedtuple(
    "IndexAttributes",
    "band code dtime variable description forecast_interval probablity probability_forecast unk3 unk4 unk5",
    defaults=[0, 0, "", "", "", "", "", "", "", "", ""],
)


def acceptable_forecast_hours(start, finish, step):
    for i in range(start, finish, step):
        if (36 < i <= 192) and (i % 3 == 0):
            yield i
        elif i > 192 and i % 6 == 0:
            yield i
        elif i <= 36:
            yield i


def acquirable_slug(variable, interval):
    # acquire_qpf_01h = "nbm-co-qpf-01h"
    # acquire_qpf_03h = "nbm-co-qpf-03h"
    # acquire_qpf_06h = "nbm-co-qpf-06h"
    # acquire_qpf_12h = "nbm-co-qpf-12h"
    # acquire_qtf_01h = "nbm-co-qtf-01h"
    # acquire_qtf_03h = "nbm-co-qtf-03h"
    # acquire_qtf_06h = "nbm-co-qtf-06h"
    variables = {"TMP": "qtf", "APCP": "qpf"}
    try:
        return "nbm-co-{}-{:02d}h".format(variables[variable], interval)
    except Exception as ex:
        print(ex)


def get_forecast_interval(interval: str, forecast_hour: int):
    interval_split = interval.split()
    hours = interval_split[0]
    hr_int = 0
    if "-" in hours:
        hr1, hr2 = hours.split("-")
        hr_int = int(hr2) - int(hr1)
        print("hr1", type(hr1), hr1)
        print("hr2", type(hr2), hr2)
    else:
        if (36 < forecast_hour <= 192) and (forecast_hour % 3 == 0):
            return 3
        elif forecast_hour > 192 and forecast_hour % 6 == 0:
            return 6
        elif forecast_hour <= 36:
            return 1
        print("hr_int", type(hr_int), hr_int)
    return hr_int


@dag(
    default_args=default_args,
    tags=["cumulus", "precip", "airtemp", "NBM"],
    schedule="3 * * * *",
    max_active_runs=1,
    max_active_tasks=6,
)
def cumulus_national_blend_models():
    """
    # National Blend of Models

    This pipeline handles download and API notification for NBM forecast products

    - [NBM Information](https://vlab.ncep.noaa.gov/web/mdl/nbm-download)

    - [NBM Product Source](https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/v4.1)

    Filename Pattern:

    `blend.tCCz.core.fHHH.RR.grib2`

    `where CC = forecast cycle, HHH = forecasted hour and RR = region (e.g., co, ak, pr)`
    """

    s3_bucket = cumulus.S3_BUCKET
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX

    @task()
    def list_available_idx():
        logical_date = get_current_context()["logical_date"]
        url = logical_date.strftime(URL_NOMADS + "/" + "blend.%Y%m%d/%H/core")

        url_list = url + "/ls-l"

        file_pattern = re.compile("blend.t\d+z.core.f\d+.co.grib2.idx")

        request_response = requests.request(
            "GET",
            url_list,
        )
        request_response_text = request_response.text
        file_pattern_match = file_pattern.findall(request_response_text, re.MULTILINE)

        return url, file_pattern_match

    with TaskGroup(group_id="nbm_download_notify") as tg:
        for fhour in acceptable_forecast_hours(1, MAX_HOUR, 1):
            with TaskGroup(
                group_id="nbm_forecast_cycle_{:03d}".format(fhour)
            ) as task_group:

                @task(
                    task_id="variables_forecast_cycle_f{:03d}".format(fhour),
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                )
                def list_forecast_variables():
                    context = get_current_context()
                    ti = context["ti"]
                    fhour = int(ti.task_id[-3:])

                    url, files = ti.xcom_pull(task_ids="list_available_idx")

                    grib_url = None
                    for idx in files:
                        (
                            _,
                            cycle,
                            _,
                            forecast_hour,
                            region,
                            file_type,
                            file_suffix,
                        ) = idx.split(".")
                        forecast_hour = int(forecast_hour[1:])

                        if forecast_hour == fhour:
                            grib = Path(idx).with_suffix("").as_posix()
                            grib_url = url + "/" + grib
                            variables = []
                            idx_url = url + "/" + idx
                            request_response = requests.request(
                                "GET",
                                idx_url,
                            )

                            for line in request_response.text.split("\n")[:-1]:
                                index_attribute = IndexAttributes(*(line.split(":")))
                                if (
                                    index_attribute.variable == "APCP"
                                    and index_attribute.description == "surface"
                                    and index_attribute.probablity == ""
                                ):
                                    variables.append(index_attribute._asdict())
                                if (
                                    index_attribute.variable == "TMP"
                                    and index_attribute.description == "surface"
                                    and index_attribute.probablity == ""
                                ):
                                    variables.append(index_attribute._asdict())

                            break

                    return {"url": grib_url, "variables": variables}

                @task(
                    task_id="download_notify_cycle_f{:03d}".format(fhour),
                )
                def download_notify(url_variables):
                    context = get_current_context()
                    ti = context["ti"]
                    fhour = int(ti.task_id[-3:])

                    notifications = []
                    url = url_variables["url"]
                    variables = url_variables["variables"]
                    filename = Path(url).name

                    # the download part
                    for var in variables:
                        date_time = datetime.strptime(
                            var["dtime"], "d=%Y%m%d%H"
                        ).replace(tzinfo=timezone.utc)
                        variable_code = var["variable"]
                        forecast_interval = get_forecast_interval(
                            var["forecast_interval"], fhour
                        )

                        slug = acquirable_slug(variable_code, forecast_interval)
                        if slug:
                            s3_key = "{}/{}/{}".format(
                                key_prefix, S3_ACQUIRABLE, filename
                            )
                            trigger_download(
                                url=url,
                                s3_bucket=s3_bucket,
                                s3_key=s3_key,
                            )
                            notifications.append(
                                {
                                    "product_slug": slug,
                                    "datetime": date_time.isoformat(),
                                    "s3_key": s3_key,
                                }
                            )

                    # the notify part
                    for notification in notifications:
                        try:
                            uuid = cumulus.acquirables[notification["product_slug"]]
                            result = cumulus.notify_acquirablefile(
                                acquirable_id=uuid,
                                datetime=notification["datetime"],
                                s3_key=notification["s3_key"],
                            )
                            print(result)
                        except KeyError as ex:
                            print(ex)

                download_notify(list_forecast_variables())

    list_available_idx() >> tg


DAG_ = cumulus_national_blend_models()
