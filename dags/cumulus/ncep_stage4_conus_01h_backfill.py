"""
Backfill NCEP Stage 4 MOSAIC QPE - Hourly from NCAR GDEX Stage IV archive
Source: https://osdf-director.osg-htc.org/ncar/gdex/d507005/stage4/stage4.YYYYMM.tar
Trigger with: {"start_year": 2002, "start_month": 1, "end_year": 2005, "end_month": 12}
"""

import gzip
import io
import json
import logging
import os
import tarfile
import tempfile
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlunparse

import requests
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

import helpers.cumulus as cumulus
from helpers.downloads import trigger_download

CUTOFF = datetime(2020, 7, 20, tzinfo=timezone.utc)
URL_ROOT = "https://osdf-director.osg-htc.org/ncar/gdex/d507005/stage4"
PRODUCT_SLUG = "ncep-stage4-mosaic-01h"


def download_with_resume(url, dest_path, chunk_size=65536, max_attempts=10):
    for attempt in range(1, max_attempts + 1):
        existing_size = os.path.getsize(dest_path) if os.path.exists(dest_path) else 0
        headers = {"Range": f"bytes={existing_size}-"} if existing_size > 0 else {}

        logging.info(f"Download attempt {attempt}/{max_attempts}, offset={existing_size} bytes: {url}")
        resp = requests.get(url, headers=headers, stream=True, timeout=120)

        if resp.status_code == 416:
            logging.info("416 Range Not Satisfiable – file already complete")
            return

        # Retry on 5xx server errors with backoff
        if resp.status_code >= 500:
            wait = 30 * attempt
            logging.warning(f"Server error {resp.status_code} on attempt {attempt}, waiting {wait}s before retry...")
            time.sleep(wait)
            continue

        resp.raise_for_status()

        mode = "ab" if existing_size > 0 else "wb"
        with open(dest_path, mode) as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                f.write(chunk)

        clen = resp.headers.get("Content-Length")
        if not clen:
            logging.info("Download complete (no Content-Length to verify)")
            return
        final_size = os.path.getsize(dest_path)
        expected = existing_size + int(clen)
        if final_size >= expected:
            logging.info(f"Download complete: {final_size} bytes")
            return
        logging.warning(f"Incomplete on attempt {attempt}: {final_size} < {expected}, retrying...")

    raise RuntimeError(f"Failed to download {url} after {max_attempts} attempts")


def upload_bytes_via_cumulus(filename: str, content: bytes) -> str:
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    file_url = urlunparse(("file", "", tmp_path, "", "", ""))
    s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
    trigger_download(url=file_url, s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key)
    return s3_key


def process_one_month(yyyymm: str) -> list:
    """Download, extract, and upload all hourly files for one YYYYMM. Returns list of s3_key dicts."""
    monthly_name = f"stage4.{yyyymm}.tar"
    monthly_url = f"{URL_ROOT}/{monthly_name}"
    logging.info(f"--- Starting month {yyyymm}: {monthly_url}")

    s3_keys = []

    with tempfile.TemporaryDirectory() as tmpdir:
        monthly_path = os.path.join(tmpdir, monthly_name)
        download_with_resume(monthly_url, monthly_path)

        with tarfile.open(monthly_path) as monthly_tar:
            daily_members = monthly_tar.getmembers()
            logging.info(f"  Found {len(daily_members)} daily tars")

            for daily_member in daily_members:
                daily_name = os.path.basename(daily_member.name)
                daily_fileobj = monthly_tar.extractfile(daily_member)
                if daily_fileobj is None:
                    logging.info(f"  Skipping non-file: {daily_name}")
                    continue

                with tarfile.open(fileobj=io.BytesIO(daily_fileobj.read())) as daily_tar:
                    for hourly_member in daily_tar.getmembers():
                        inner_name = hourly_member.name
                        filename = os.path.basename(inner_name)

                        # GRIB2 post-cutoff
                        if filename.startswith("st4_conus.") and filename.endswith(".01h.grb2"):
                            dt_str = filename.split(".")[1]
                            file_dt = datetime.strptime(dt_str, "%Y%m%d%H").replace(tzinfo=timezone.utc)
                            if file_dt < CUTOFF:
                                continue
                            hourly_fileobj = daily_tar.extractfile(hourly_member)
                            if hourly_fileobj is None:
                                raise ValueError(f"Could not extract {inner_name}")
                            s3_key = upload_bytes_via_cumulus(filename, hourly_fileobj.read())
                            logging.info(f"    Uploaded GRIB2: {filename}")
                            s3_keys.append({"datetime": file_dt.isoformat(), "s3_key": s3_key})
                            continue

                        # GRIB1 pre-cutoff (gzipped)
                        if filename.startswith("ST4.") and filename.endswith(".01h.gz"):
                            dt_str = filename.split(".")[1]
                            file_dt = datetime.strptime(dt_str, "%Y%m%d%H").replace(tzinfo=timezone.utc)
                            if file_dt >= CUTOFF:
                                continue
                            gz_fileobj = daily_tar.extractfile(hourly_member)
                            if gz_fileobj is None:
                                raise ValueError(f"Could not extract gz {inner_name}")
                            grib_bytes = gzip.decompress(gz_fileobj.read())
                            out_name = f"st4_conus.{dt_str}.01h"
                            s3_key = upload_bytes_via_cumulus(out_name, grib_bytes)
                            logging.info(f"    Uploaded GRIB1: {out_name}")
                            s3_keys.append({"datetime": file_dt.isoformat(), "s3_key": s3_key})
                            continue

    if not s3_keys:
        raise ValueError(f"No matching hourly files found in {monthly_url}")

    logging.info(f"--- Finished month {yyyymm}: {len(s3_keys)} files uploaded")
    return s3_keys


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2002, 1, 1, tzinfo=timezone.utc),
    "catchup": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule=None,
    params={
        "start_year":  Param(2002, type="integer", description="First year to backfill"),
        "start_month": Param(1,    type="integer", description="First month (1-12)"),
        "end_year":    Param(2002, type="integer", description="Last year to backfill (inclusive)"),
        "end_month":   Param(12,   type="integer", description="Last month (1-12, inclusive)"),
    },
    tags=["cumulus", "precip", "QPE", "CONUS", "stage4", "NCEP", "backfill"],
    max_active_runs=1,
    max_active_tasks=2,
)
def cumulus_ncep_stage4_conus_01h_backfill():

    @task()
    def backfill_all_months():
        context = get_current_context()
        p = context["params"]
        ti = context["ti"]

        start = datetime(p["start_year"], p["start_month"], 1)
        end   = datetime(p["end_year"],   p["end_month"],   1)

        if start > end:
            raise ValueError(f"start ({start:%Y-%m}) is after end ({end:%Y-%m})")

        # Pull already-completed months from a previous attempt (empty set on first run)
        prev_try_number = ti.try_number - 1
        completed_months: set[str] = set()
        if prev_try_number > 0:
            for attempt in range(1, prev_try_number + 1):
                prior = ti.xcom_pull(
                    key="completed_months",
                    task_ids=ti.task_id,
                    map_indexes=ti.map_index,
                )
                if prior:
                    completed_months.update(prior)
                break  # xcom_pull returns the latest pushed value

        failed_months = []
        cur = start
        while cur <= end:
            yyyymm = cur.strftime("%Y%m")

            if yyyymm in completed_months:
                logging.info(f"Skipping already-completed month: {yyyymm}")
                if cur.month == 12:
                    cur = cur.replace(year=cur.year + 1, month=1)
                else:
                    cur = cur.replace(month=cur.month + 1)
                continue

            try:
                s3_keys = process_one_month(yyyymm)
                for item in s3_keys:
                    cumulus.notify_acquirablefile(
                        acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
                        datetime=item["datetime"],
                        s3_key=item["s3_key"],
                    )
                completed_months.add(yyyymm)
                # Persist progress after every successful month
                ti.xcom_push(key="completed_months", value=list(completed_months))
            except Exception as e:
                logging.error(f"FAILED month {yyyymm}: {e}")
                failed_months.append(yyyymm)

            if cur.month == 12:
                cur = cur.replace(year=cur.year + 1, month=1)
            else:
                cur = cur.replace(month=cur.month + 1)

        if failed_months:
            raise RuntimeError(
                f"{len(failed_months)} month(s) failed and need re-running: {failed_months}"
            )


    backfill_all_months()


backfill_dag = cumulus_ncep_stage4_conus_01h_backfill()
