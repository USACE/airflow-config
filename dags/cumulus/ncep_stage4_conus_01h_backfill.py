"""
Backfill NCEP Stage 4 MOSAIC QPE - Hourly from NCAR GDEX historical archive
Monthly tar -> daily tars (ST4.YYYYMMDD) -> ST4/st4_conus.YYYYMMDDHH.01h.grb2
"""

import io
import json
import logging
import os
import tarfile
import tempfile
from datetime import datetime, timedelta, timezone

import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from helpers.downloads import trigger_download
import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1, tzinfo=timezone.utc),
    "end_date": datetime(2022, 12, 31, tzinfo=timezone.utc),
    "catchup": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 6,
    "retry_delay": timedelta(minutes=5),
}

URL_ROOT = "https://osdf-data.gdex.ucar.edu/ncar/gdex/d507005/stage4"
PRODUCT_SLUG = "ncep-stage4-mosaic-01h"


def download_with_resume(url, dest_path, chunk_size=65536, max_attempts=10):
    for attempt in range(1, max_attempts + 1):
        existing_size = os.path.getsize(dest_path) if os.path.exists(dest_path) else 0
        headers = {"Range": f"bytes={existing_size}-"} if existing_size > 0 else {}

        logging.info(f"Download attempt {attempt}, offset={existing_size} bytes: {url}")
        resp = requests.get(url, headers=headers, stream=True, timeout=120)

        # retry on 5xx
        if 500 <= resp.status_code < 600:
            logging.warning(f"Server {resp.status_code} on attempt {attempt}, retrying...")
            continue

        if resp.status_code == 416:
            logging.info("Server returned 416 — file already fully downloaded")
            return

        resp.raise_for_status()

        mode = "ab" if existing_size > 0 else "wb"
        with open(dest_path, mode) as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                f.write(chunk)

        content_length = resp.headers.get("Content-Length")
        if content_length:
            final_size = os.path.getsize(dest_path)
            expected = existing_size + int(content_length)
            if final_size >= expected:
                logging.info(f"Download complete: {final_size} bytes")
                return
            logging.warning(
                f"Incomplete download on attempt {attempt}: "
                f"got {final_size}, expected {expected}. Retrying..."
            )
        else:
            logging.info("Download complete (no Content-Length header to verify)")
            return

    raise RuntimeError(f"Failed to fully download {url} after {max_attempts} attempts")


@dag(
    default_args=default_args,
    schedule="0 0 1 * *",
    tags=["cumulus", "precip", "QPE", "CONUS", "stage4", "NCEP", "backfill"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_ncep_stage4_conus_01h_backfill():
    @task()
    def download_and_extract_stage4_hourly():
        logical_date = get_current_context()["logical_date"]
        yyyymm = logical_date.strftime("%Y%m")

        tar_filename = f"stage4.{yyyymm}.tar"
        tar_url = f"{URL_ROOT}/{tar_filename}"

        logging.info(f"Downloading monthly tar: {tar_url}")

        s3_keys = []

        with tempfile.TemporaryDirectory() as tmpdir:
            monthly_tar_path = os.path.join(tmpdir, tar_filename)
            download_with_resume(tar_url, monthly_tar_path)

            # outer monthly tar
            with tarfile.open(monthly_tar_path) as monthly_tar:
                daily_members = monthly_tar.getmembers()
                logging.info(f"Found {len(daily_members)} daily tars in monthly tar")

                for daily_member in daily_members:
                    daily_name = os.path.basename(daily_member.name)
                    logging.info(f"Opening daily tar: {daily_name}")

                    daily_fileobj = monthly_tar.extractfile(daily_member)
                    if daily_fileobj is None:
                        logging.info(f"Skipping non-file member: {daily_name}")
                        continue

                    # inner daily tar
                    with tarfile.open(fileobj=io.BytesIO(daily_fileobj.read())) as daily_tar:
                        hourly_members = daily_tar.getmembers()
                        logging.info(f"  {len(hourly_members)} files inside {daily_name}:")
                        for m in hourly_members:
                            logging.info(f"    {m.name}")

                        for hourly_member in hourly_members:
                            filename = os.path.basename(hourly_member.name)

                            # st4_conus.YYYYMMDDHH.01h.grb2
                            if not (
                                filename.startswith("st4_conus.")
                                and filename.endswith(".01h.grb2")
                            ):
                                logging.info(f"    Skipping: {filename}")
                                continue

                            dt_str = filename.split(".")[1]  # "YYYYMMDDHH"
                            file_dt = datetime.strptime(dt_str, "%Y%m%d%H").replace(
                                tzinfo=timezone.utc
                            )

                            # extract hourly file to temp and push via trigger_download
                            extracted_path = os.path.join(tmpdir, filename)
                            with open(extracted_path, "wb") as out_f:
                                fobj = daily_tar.extractfile(hourly_member)
                                if fobj is None:
                                    raise ValueError(f"Could not extract: {hourly_member.name}")
                                out_f.write(fobj.read())

                            # upload using same pattern as your live DAG
                            s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
                            trigger_download(
                                url=f"file://{extracted_path}",
                                s3_bucket=cumulus.S3_BUCKET,
                                s3_key=s3_key,
                            )

                            logging.info(
                                f"    Uploaded {filename} -> s3://{cumulus.S3_BUCKET}/{s3_key}"
                            )
                            s3_keys.append(
                                {"datetime": file_dt.isoformat(), "s3_key": s3_key}
                            )

        if not s3_keys:
            raise ValueError(f"No matching st4_conus *.01h.grb2 files found in: {tar_url}")

        return json.dumps(s3_keys)

    @task()
    def notify_cumulus(payload):
        payload = json.loads(payload)
        for p in payload:
            cumulus.notify_acquirablefile(
                acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
                datetime=p["datetime"],
                s3_key=p["s3_key"],
            )

    notify_cumulus(download_and_extract_stage4_hourly())


stage4_backfill_dag = cumulus_ncep_stage4_conus_01h_backfill()
