import json
from calendar import monthrange
from datetime import datetime, timedelta, timezone

import helpers.cumulus as cumulus
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

URL_ROOT = "https://data.prism.oregonstate.edu/time_series/us/an/4km"
SHORT_NAMES = ["ppt", "tmax", "tmin"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(1981, 1, 1, tzinfo=timezone.utc),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}


@dag(
    default_args=default_args,
    schedule=None,  # manually triggered only
    catchup=False,
    params={
        "start_year": Param(1981, type="integer", description="First year to backfill"),
        "start_month": Param(1, type="integer", description="First month (1-12)"),
        "end_year": Param(2025, type="integer", description="Last year (inclusive)"),
        "end_month": Param(
            12, type="integer", description="Last month (1-12, inclusive)"
        ),
    },
    tags=["cumulus", "backfill", "prism"],
    max_active_runs=1,
    max_active_tasks=1,  
)
def cumulus_prism_backfill_por():
    """Backfill historical PRISM data month-by-month."""

    @task()
    def generate_months() -> list[str]:
        """Build ordered list of YYYYMM strings from DAG params."""
        p = get_current_context()["params"]
        start = datetime(p["start_year"], p["start_month"], 1)
        end = datetime(p["end_year"], p["end_month"], 1)
        if start > end:
            raise ValueError(f"start ({start:%Y-%m}) is after end ({end:%Y-%m})")
        months, cur = [], start
        while cur <= end:
            months.append(cur.strftime("%Y%m"))
            cur = (
                cur.replace(year=cur.year + 1, month=1)
                if cur.month == 12
                else cur.replace(month=cur.month + 1)
            )
        return months

    @task(map_index_template="{{ yyyymm }}")
    def process_month(yyyymm: str) -> int:
        get_current_context()["yyyymm"] = yyyymm  # powers map_index_template label

        year = int(yyyymm[:4])
        month = int(yyyymm[4:])
        num_days = monthrange(year, month)[1]
        total = 0

        for short_name in SHORT_NAMES:
            product_slug = f"prism-{short_name}-early"
            for day in range(1, num_days + 1):
                dt = datetime(year, month, day, tzinfo=timezone.utc)
                file_dir = f"{URL_ROOT}/{short_name}/daily/{dt.strftime('%Y')}"
                filename = f"prism_{short_name}_us_25m_{dt.strftime('%Y%m%d')}.zip"
                s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{filename}"
                trigger_download(
                    url=f"{file_dir}/{filename}",
                    s3_bucket=cumulus.S3_BUCKET,
                    s3_key=s3_key,
                )
                cumulus.notify_acquirablefile(
                    acquirable_id=cumulus.acquirables[product_slug],
                    datetime=dt.isoformat(),
                    s3_key=s3_key,
                )
                total += 1

        return total  # files uploaded this month

    months = generate_months()
    process_month.expand(yyyymm=months)


backfill_dag = cumulus_prism_backfill_por()
