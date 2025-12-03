"""
Acquire and Process APRFC QPF 06h
"""

import calendar
import json
import re
from datetime import datetime, timedelta, timezone

import helpers.cumulus as cumulus
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from bs4 import BeautifulSoup
from helpers.downloads import s3_file_exists, trigger_download

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(hours=36)).replace(
        minute=0, second=0
    ),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}


QPF_REGEX = re.compile(
    r"^qpf06f_has_"        # literal prefix with underscore
    r".*?"                 # anything up to _awips_
    r"_awips_"             
    r"(\d{12})"            # group 1: 12-digit forecast time (2025121112)
    r"_"                   
    r"(\d{10})"            # group 2:  10-digit issuance (202512030603)
    r"f"
    r"(\d{3})"             # group 3: forecast hour (192)
    r"\.grb(?:\.gz)?$"     # .grb or .grb.gz, no extra .digits
)

def get_latest_files(filenames):
    # Dictionary to store the latest file for each unique timestamp
    latest_files = {}

    for filename in filenames:
        m = QPF_REGEX.match(filename)
        if not m:
            continue
        issue_ts    = m.group(2)  # 10-digit issuance
        base_ts     = m.group(1)  # 12-digit valid/base
        forecast_hr = m.group(3)  # 3-digit forecast hour
        key = f"{issue_ts}_{base_ts}_{forecast_hr}"
        # Update the latest file for the key if it's not present or if the current filename is greater

        if key not in latest_files or filename > latest_files[key]:
            latest_files[key] = filename
    # Return the list of latest files
    return list(latest_files.values())


# ALR QPF filename generator
def get_filenames(edate, url):
    """
    Scrape website, collect matching filenames, then:
      - find the latest issuance timestamp (the 12-digit value after 'awips_')
      - if latest issuance is within 36 hours of edate (Airflow logical_date), return only those files
      - otherwise return an empty list (nothing to download)
    """
    page = requests.get(url, timeout=30)
    soup = BeautifulSoup(page.content, "html.parser")
    links = [node.get("href") for node in soup.find_all("a")]

    candidates = []
    for link in links:
        if not link:
            continue
        link = link.strip()
        m = QPF_REGEX.match(link)
        if m:
            issue_ts = m.group(1)  # 12-digit issuance after _awips_
            candidates.append((link, issue_ts))
            # print(f"DEBUG link: {link}, issue_ts: {issue_ts}")
    
    # No matching files
    if not candidates:
        print("DEBUG: no candidates matched QPF_REGEX")
        return []

    # Find the latest issuance
    latest_issue_str = max(issue for _, issue in candidates)

    # Convert to an aware UTC datetime for comparison against Airflow's logical_date
    latest_issue_dt = datetime.strptime(latest_issue_str, "%Y%m%d%H%M").replace(
        tzinfo=timezone.utc
    )

    # Only proceed if latest issuance is in the past and within the last 36 hours relative to edate (logical_date)
    if latest_issue_dt > edate or (edate - latest_issue_dt) > timedelta(hours=36):
        # Outside the 36-hour window or in the future: do not download anything
        return []
    # print(f"DEBUG found {len(candidates)} matching files; latest_issue_str={latest_issue_str}")
    
    # print(f"DEBUG latest_issue_str: {latest_issue_str}")
    # print(f"DEBUG latest_issue_dt: {latest_issue_dt.isoformat()}")
    # print(f"DEBUG edate (logical_date): {edate.isoformat()}")
    # print(f"DEBUG age_hours: {(edate - latest_issue_dt).total_seconds() / 3600.0}")


    # Filter to only files from the latest issuance
    latest_issue_files = [fn for fn, issue in candidates if issue == latest_issue_str]

    # Deduplicate within that issuance set by issue+forecast key as your existing logic defines
    return get_latest_files(latest_issue_files)


@dag(
    default_args=default_args,
    schedule="25 * * * *",
    tags=["cumulus", "precip", "QPF", "APRFC"],
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,  # Disable backfills
)
def cumulus_aprfc_qpf_06h():
    """This pipeline handles download, processing, and derivative product creation for \n
    APRFC QPE\n
    URL Dir - https://cbt.crohms.org/akgrids
    Files matching qpf06f_has_6f_20200917_18_awips_202009170949.grb - 6 hour\n
    """
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX
    URL_ROOT = f"https://cbt.crohms.org/akgrids"
    PRODUCT_SLUG = "aprfc-qpf-06h"

    @task()
    def download_raw_qpf():
        logical_date = get_current_context()["logical_date"]

        return_list = list()
        filenames = get_filenames(logical_date, URL_ROOT)
        for filename in filenames:
            url = f"{URL_ROOT}/{filename}"
            s3_key = f"{key_prefix}/{PRODUCT_SLUG}/{filename}"
            # Check if the file already exists in S3
            if s3_file_exists(cumulus.S3_BUCKET, s3_key):
               print(f"Skipping existing S3 object: s3://{cumulus.S3_BUCKET}/{s3_key}")
               continue  # Skip to the next file
            print(f"Downloading file: {filename}")
            try:
                trigger_download(url=url, s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key)
                return_list.append(
                    {
                        "execution": logical_date.isoformat(),
                        "s3_key": s3_key,
                        "filename": filename,
                    }
                )
            except:
                print(f"{filename} is not available to download")

        return json.dumps(return_list)

    @task()
    def notify_cumulus(payload):
        payload = json.loads(payload)
        for item in payload:
            print("Notifying Cumulus: " + item["filename"])
            cumulus.notify_acquirablefile(
                acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
                datetime=item["execution"],
                s3_key=item["s3_key"],
            )

    notify_cumulus(download_raw_qpf())


aprfc_qpf_dag = cumulus_aprfc_qpf_06h()
