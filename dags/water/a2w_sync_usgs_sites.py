"""
# Sync Water API with USGS Sites

[USGS Site Web Service](https://waterservices.usgs.gov/rest/Site-Service.html)

[Understanding Output](https://waterservices.usgs.gov/rest/Site-Service.html#Understanding)

URL

- https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd={state}&period=P52W&siteType=LK,ST&siteStatus=all&hasDataTypeCd=iv,aw

## USGS Fields

| Field                 | Desc                              |
| --------------------- | --------------------------------- |
| agency_cd             | Agency                            |
| site_no               | Site identification number        |
| station_nm            | Site name                         |
| site_tp_cd            | Site type                         |
| dec_lat_va            | Decimal latitude                  |
| dec_long_va           | Decimal longitude                 |
| coord_acy_cd          | Latitude-longitude accuracy       |
| dec_coord_datum_cd    | Decimal Latitude-longitude datum  |
| alt_va                | Altitude of Gage/land surface     |
| alt_acy_va            | Altitude accuracy                 |
| alt_datum_cd          | Altitude datum                    |
| huc_cd                | Hydrologic unit code              |

"""
import csv
from datetime import datetime, timedelta
from io import StringIO
from textwrap import dedent

import helpers.water as water
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(days=2)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    # 'max_active_runs':1,
    # 'concurrency':4,
}

with DAG(
    default_args=default_args,
    dag_id="a2w_sync_usgs_sites",
    tags=["a2w", "usgs"],
    schedule="@daily",
    doc_md=dedent(__doc__),
) as dag:

    ########################################
    def fetch_and_write(state_abbrev):

        url = f"https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd={state_abbrev}&period=P52W&siteType=LK,ST&siteStatus=all&hasDataTypeCd=iv,aw"
        # print(url)
        r = requests.get(url)
        # print(r.text)
        buff = StringIO(r.text)
        reader = csv.reader(buff, delimiter="\t")

        prepped_data = []
        keys = []
        result = []

        horizontal_datum = {"NAD83": 4269}
        vertical_datum = {"UNKNOWN": 0, "COE1912": 1, "NGVD29": 2, "NAVD88": 3}

        # Cleanup data before parse fields
        # Store header and actual data rows in new variable
        for idx, line in enumerate(reader):

            # only look at the header and data lines
            if line[0].strip() in ["agency_cd", "USGS"]:
                prepped_data.append(line)

        for idx, line in enumerate(prepped_data):

            if idx == 0:
                # this is the header
                keys = line
            else:
                # Build each line (object) by setting the keys and values
                _line = {}
                for i, k in enumerate(keys):
                    _line[k] = line[i].strip()
                result.append(_line)

        state_sites = []

        # Each line (after cleanup) in the results from the USGS API call represents a site
        for line in result:

            site = {}
            site["provider"] = "usgs"
            site["datatype"] = "usgs-site"
            site["code"] = line["site_no"].strip()
            site["state"] = state_abbrev

            # Geometry
            # --------
            geom = {}
            geom["type"] = "Point"
            try:
                geom["coordinates"] = [
                    float(line["dec_long_va"].strip()),
                    float(line["dec_lat_va"].strip()),
                ]
            except:
                geom["coordinates"] = [0, 0]
            site["geometry"] = geom

            # Attributes
            # ---------
            attributes = {}
            attributes["station_name"] = line["station_nm"].strip()
            attributes["site_type"] = line["site_tp_cd"].strip()
            site["attributes"] = attributes

            # site["site_number"] = line["site_no"].strip()
            # site["name"] = line["station_nm"].replace("'", "").strip()
            # site["state_abbrev"] = state_abbrev
            # site["elevation"] = (
            #     float(line["alt_va"].strip()) if line["alt_va"].strip() != "" else None
            # )
            # try:
            #     site["horizontal_datum_id"] = horizontal_datum[
            #         line["dec_coord_datum_cd"]
            #     ]
            # except:
            #     site["horizontal_datum_id"] = 4269
            # site["huc"] = (
            #     f"{line['huc_cd'].strip()}" if line["huc_cd"].strip() != "" else None
            # )
            # try:
            #     site["vertical_datum_id"] = vertical_datum[line["alt_datum_cd"]]
            # except:
            #     site["vertical_datum_id"] = vertical_datum["UNKNOWN"]

            state_sites.append(site)

        # POST to the water API
        # water.sync_usgs_sites(state_sites)
        water_hook = water.WaterHook(method="POST")
        resp = water_hook.request(
            endpoint="/providers/usgs/locations",
            json=state_sites,
        )

    states = water.get_states()

    for state in states:
        fetch_task_id = f"fetch_and_write_{state}"
        get_usgs_sites_task = PythonOperator(
            task_id=fetch_task_id,
            python_callable=fetch_and_write,
            op_kwargs={
                "state_abbrev": state,
            },
        )
