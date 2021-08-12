"""
Header fields:
#  agency_cd       -- Agency
#  site_no         -- Site identification number
#  station_nm      -- Site name
#  site_tp_cd      -- Site type
#  dec_lat_va      -- Decimal latitude
#  dec_long_va     -- Decimal longitude
#  coord_acy_cd    -- Latitude-longitude accuracy
#  dec_coord_datum_cd -- Decimal Latitude-longitude datum
#  alt_va          -- Altitude of Gage/land surface
#  alt_acy_va      -- Altitude accuracy
#  alt_datum_cd    -- Altitude datum
#  huc_cd          -- Hydrologic unit code

https://waterservices.usgs.gov/rest/Site-Service.html#Understanding

URL => https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd={state}&period=P52W&siteType=LK,ST&siteStatus=all&hasDataTypeCd=iv,aw
"""
import requests
from io import StringIO
import json
import csv

from airflow import DAG
from datetime import datetime, timedelta, timezone
from textwrap import dedent

import helpers.water as water


from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator

implementation = {
    'stable': {
        'dag_id': 'a2w_sync_usgs_sites',
        'tags': ['stable', 'a2w', 'usgs'],
    },
    'develop': {
        'dag_id': 'develop_a2w_sync_usgs_sites',
        'tags': ['develop', 'a2w', 'usgs'],
    }
}

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': (datetime.utcnow()-timedelta(days=2)).replace(minute=0, second=0),
    'catchup_by_default': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

def create_dag(**kwargs):
    
    conn_type = kwargs['conn_type']    

    with DAG(
        default_args=default_args,
        dag_id=kwargs['dag_id'],
        tags=kwargs['tags'],
        schedule_interval=kwargs['schedule_interval'],
        doc_md=dedent(__doc__)
    
    ) as dag:

        ########################################
        def fetch_and_write(state_abbrev):
            
            url = f"https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd={state_abbrev}&period=P52W&siteType=LK,ST&siteStatus=all&hasDataTypeCd=iv,aw"
            # print(url)
            r = requests.get(url)
            # print(r.text)
            buff = StringIO(r.text)
            reader = csv.reader(buff, delimiter='\t')

            prepped_data = []
            keys = []
            result= []

            horizontal_datum = {
                'NAD83': 4269
            }
            vertical_datum = {
                'UNKNOWN': 0,
                'COE1912': 1,
                'NGVD29': 2,
                'NAVD88': 3        
            }

            # Cleanup data before parse fields
            # Store header and actual data rows in new variable
            for idx, line in enumerate(reader):
                
                # only look at the header and data lines
                if line[0].strip() in ['agency_cd', 'USGS']:
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
                site['usgs_id'] = line['site_no'].strip()
                site['name'] = line['station_nm'].replace("'", "").strip()
                site['state_abbrev'] = state_abbrev
                site['elevation'] = float(line['alt_va'].strip()) if line['alt_va'].strip() != '' else None
                try:
                    site['horizontal_datum_id'] = horizontal_datum[line['dec_coord_datum_cd']]
                except:
                    site['horizontal_datum_id'] = 4269
                huc = f"'{line['huc_cd'].strip()}'" if line['huc_cd'].strip() != '' else None
                try:
                    site['vertical_datum_id'] = vertical_datum[line['alt_datum_cd']]
                except:
                    site['vertical_datum_id'] = vertical_datum['UNKNOWN']
                geom = {}
                geom['type'] = 'Point'
                try:
                    geom['coordinates'] = [float(line['dec_long_va'].strip()), float(line['dec_lat_va'].strip())]
                except:
                    geom['coordinates'] = [0,0]
                site['geometry'] = geom
        
                state_sites.append(site)
            
                # POST to the water API
                water.sync_usgs_sites(state_sites, conn_type)
                # r = requests.post(
                # "http://water-api_api_1/sync/usgs_sites?key=appkey",
                # json=state_sites,
                # headers={"Content-Type": "application/json"},    
                # )
                # print(r.status_code)

            return
        ########################################

        # Build two DAGSs
        # ----------------
        states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

        for state in states:
            fetch_task_id = f"fetch_and_write_{state}"
            get_usgs_sites_task = PythonOperator(
                task_id=fetch_task_id,
                python_callable=fetch_and_write,
                op_kwargs={                
                    'state_abbrev': state,
                    }
            )
        return dag
        # ----------------    
    
# Expose to the global() allowing airflow to add to the DagBag
for key, val in implementation.items():
    d_id = val['dag_id']
    d_tags = val['tags']
    globals()[d_id] = create_dag(
        dag_id=d_id,
        tags=d_tags,
        conn_type=key,
        schedule_interval='@daily'
    )
