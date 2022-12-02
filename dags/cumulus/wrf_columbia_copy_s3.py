"""
# Weather Research and Forecasting (WRF)

## Columbia River Basin
"""

import json
from pathlib import Path
from string import Template
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import copy_s3_file

import helpers.cumulus as cumulus


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime(1928, 1, 1),
    "start_date": datetime(1928, 1, 1),
    "end_date": datetime(1938, 1, 1),
    # "end_date": datetime(2018, 1, 1),
    "catchup_by_default": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule="@yearly",
    tags=["cumulus", "precip", "airtemp", "Weather Research and Forecasting"],
    max_active_runs=2,
    max_active_tasks=3,
)
def cumulus_copy_columbia_wrf():
    """
    # Weather Research and Forecasting - Columbia River Basin

    ## S3 Processing

    netCDF files stored in S3 (columbia-river) will be copied over to the Cumulus S3 bucket and then the Cumulus database notified.

    ## S3 Bucket and Key

    Source Bucket = `columbia-river`

    key = `wrfout/d03fmt/reconstruction/*.nc`

    ## File Names

    ### Cumulus Acquirable

    WRF Columbia River acquirable slug: `wrf-columbia-river`

    ### Product Slug Naming

    Product slug names are constructed as follows:

    - Four part naming delimited by dash (`-`)
    - First part defines the model, which is WRF
    - Second part is the project name
    - Third part is the year (YYYY); some years are defined as `summer` (s) or `winter` (w)
    - The last part is the parameter name (file name); trailing underscores (_) removed
    - Source files are netCDF with the suffix `.nc`
    - Destination files are netCDF with the suffix `.nc`

    |  File Name    |  -->  |  Parameter                  |
    |  :----------  |  ---  |  :------------------------  |
    |  DEWPNT_T.nc  |  -->  |  Dewpoint Temperature       |
    |  GROUND_T.nc  |  -->  |  Surface Skin Temperature   |
    |  LWDOWN__.nc  |  -->  |  Longwave Radiation         |
    |  PRECIPAH.nc  |  -->  |  Precipitation              |
    |  PSTARCRS.nc  |  -->  |  Surface Pressure           |
    |  RH______.nc  |  -->  |  Surface Relative Humidity  |
    |  SWDOWN__.nc  |  -->  |  Shortwave Radiation        |
    |  T2______.nc  |  -->  |  Temperature at 2 m         |
    |  U10_____.nc  |  -->  |  U Wind at 10 m             |
    |  V10_____.nc  |  -->  |  V Wind at 10 m             |
    |  VAPOR_PS.nc  |  -->  |  Vapor Pressure             |

    """
    CUMULUS_ACQUIRABLE = "wrf-columbia"

    S3_SRC_BUCKET = "columbia-river"
    S3_SRC_KEY_PREFIX = "wrfout/d03fmt/reconstruction"
    S3_DST_BUCKET = "castle-data-develop"

    filename_template = Template("wrf-columbia-${yr}${season}-${para}")
    filenames = [
        "DEWPNT_T.nc",
        "GROUND_T.nc",
        "LWDOWN__.nc",
        "PRECIPAH.nc",
        "PSTARCRS.nc",
        "RH______.nc",
        "SWDOWN__.nc",
        "T2______.nc",
        "U10_____.nc",
        "V10_____.nc",
        "VAPOR_PS.nc",
    ]

    # Create a dynamic task
    def create_task_group(filename: Path):
        """
        # Create a task group

        Parameters
        ----------
        filename : Path
            netCDF filename, with extension, that is also the parameter
        """
        with TaskGroup(group_id=f"{filename.stem}") as task_group:

            def copy_season(filename: Path, season: str):
                execution_date = get_current_context()["data_interval_start"]
                product_filename = filename_template.substitute(
                    yr=execution_date.year,
                    season=season,
                    para=filename.stem.lower().replace("_", ""),
                )
                src_key = f"{S3_SRC_KEY_PREFIX}/{execution_date.year}{season}/ncf/{str(filename)}"
                dst_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{CUMULUS_ACQUIRABLE}/{product_filename}{filename.suffix}"

                copy_s3_file(S3_SRC_BUCKET, src_key, S3_DST_BUCKET, dst_key)

                return json.dumps(
                    {
                        "datetime": execution_date.isoformat(),
                        "s3_key": dst_key,
                        "product_slug": CUMULUS_ACQUIRABLE,
                    }
                )

            def notify_cumulus(notification):
                # Airflow will convert the parameter to a string, convert it back
                payload = json.loads(notification)
                cumulus.notify_acquirablefile(
                    acquirable_id=cumulus.acquirables[payload["product_slug"]],
                    datetime=payload["datetime"],
                    s3_key=payload["s3_key"],
                )

            @task(task_id=f"copy_seasonal_winter_{filename.stem}")
            def copy_winter_season(filename: Path):
                return copy_season(filename, "w")

            @task(task_id=f"copy_seasonal_summer_{filename.stem}")
            def copy_summer_season(filename: Path):
                return copy_season(filename, "s")

            @task(task_id=f"notify_seasonal_winter_{filename.stem}")
            def notify_cumulus_winter_season(notification):
                notify_cumulus(notification)

            @task(task_id=f"notify_seasonal_summer_{filename.stem}")
            def notify_cumulus_summer_season(notification):
                notify_cumulus(notification)

            notify_cumulus_winter_season(copy_winter_season(filename))
            notify_cumulus_summer_season(copy_summer_season(filename))

        return task_group

    _ = [create_task_group(Path(name)) for name in filenames]


wrf_dag = cumulus_copy_columbia_wrf()
