"""
Download only UCAR EOL NCEP 4km Stage 4 data
"""

import os, json
from datetime import datetime, timedelta
import calendar
import logging

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download, upload_file

import helpers.cumulus as cumulus
from ftplib import FTP

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2002, 1, 1),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    "end_date": datetime(2015, 12, 2),
}


@dag(
    default_args=default_args,
    schedule="0 0 1 * *",
    tags=["cumulus", "precip", "archive"],
    max_active_runs=1,
    max_active_tasks=1,
)
def cumulus_ncep_stage4_archive_download():
    """This is download only for processing later\n
    URL Dir - ftp://data.eol.ucar.edu/pub/download/extra/katz_data/\n

    You must change to that directory in one step, as some intermediate directories (folders)
    cannot be viewed. Then there is a "stage4" directory under that, then the monthly
    directories, so it is best downloaded with a client that operates recursively. The
    data totals approximately 21 GB.

    Files matching ST4.2002010100.01h.Z - Hourly

    This dag was designed for each task to download a month's worth of files.  This was done
    because the ftp session should be closed when a file download is complete and performing a
    single download per task would be opening and closing a ton of ftp sessions which is not nice
    to the FTP server.
    """

    # URL_ROOT = f"ftp://data.eol.ucar.edu/pub/download/extra/katz_data"
    URL_ROOT = f"data.eol.ucar.edu"
    DATA_DIR = "pub/download/extra/katz_data"

    @task()
    def download_ncep_stage4_conus_archive_month():
        execution_date = get_current_context()["logical_date"]

        last_day_of_current_month = calendar.monthrange(
            execution_date.year, execution_date.month
        )[1]

        print(f"Last day of month: {last_day_of_current_month}")

        # Login to ftp site
        ftp = FTP(URL_ROOT)

        # Connect to the FTP server
        ftp.connect(host=URL_ROOT)

        # Log in to the FTP server
        ftp.login()

        # Passive mode (optional, depending on your FTP server)
        ftp.set_pasv(True)

        # change ftp dir to pub/download/extra/katz_data
        ftp.cwd(DATA_DIR)

        # change to stage4/YYYYMM dir
        ftp.cwd(f"stage4/{execution_date.strftime('%Y%m')}")

        # Set binary mode
        ftp.voidcmd("TYPE I")

        file_ext = "Z"
        file_prefix = "ST4"

        # fmt: off
        # ---------------------
        # .gz starts 2013-08
        if execution_date.year > 2013 or (execution_date.year == 2013 and execution_date.month >= 8):
            file_ext = "gz"
        # file prefix and ext changes again in 2020-08
        if execution_date.year > 2020 or (execution_date.year == 2020 and execution_date.month >= 8):
            file_prefix = "st4_conus"
            file_ext = "grb2"
        # ---------------------
        # fmt: on

        # Loop over days in current month
        for day in range(1, last_day_of_current_month + 1):

            # Loop over hours in day
            for hour in range(0, 24):

                # Download file from current ftp dir
                filename = f'{file_prefix}.{execution_date.strftime(f"%Y%m{str(day).zfill(2)}{str(hour).zfill(2)}")}.01h.{file_ext}'
                # Force the S3 key to use the latest filename prefix for consistency
                s3_key = f"cumulus/acquirables/ncep-4km-stage4-conus-archive/{filename.replace('ST4.', 'st4_conus.')}"
                print(f"Downloading {filename}")
                logging.info(f"Downloading {filename}")

                try:
                    with open(filename, "wb") as f:
                        ftp.retrbinary("RETR {}".format(filename), f.write)
                        upload_file(
                            filename=filename,
                            bucket=cumulus.S3_BUCKET,
                            key=s3_key,
                        )
                except:
                    logging.warning(f"Error downloading {filename}")

                    pass

                try:
                    os.remove(filename)
                except OSError:
                    pass

        # Close the FTP connection
        ftp.quit()

        return json.dumps({"datetime": execution_date.isoformat(), "s3_key": s3_key})

    download_ncep_stage4_conus_archive_month()


dag = cumulus_ncep_stage4_archive_download()
