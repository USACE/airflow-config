"""
## Sync Water database with RADAR Locations

RADAR Locations retrieved as JSON string

### Parse:

Extract schema needed for posting to Water API

### Post:

Water API endpoint `/sync/locations`

```{
    'office_id',
    'name',
    'public_name',
    'kind_id',
    'geometry' : {
        'type',
        'coordinates': [
            101, 47
        ]
    }
}```

__North America centriod default for missing coordinates__
"""
import pprint
import json
from airflow.exceptions import AirflowFailException
import numpy as np
import requests
import pandas as pd
from datetime import datetime, timedelta
from requests.api import put

from sqlalchemy.sql.operators import is_

import helpers.sharedApi as sharedApi
import helpers.radar as radar
import helpers.water as water

from airflow import AirflowException
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


pp = pprint.PrettyPrinter(indent=4)

implementation = {
    'stable': {
        'bucket': 'cwbi-data-stable',
        'dag_id': 'a2w_sync_locations',
        'tags': ['stable', 'a2w'],
    },
    'develop': {
        'bucket': 'cwbi-data-develop',
        'dag_id': 'develop_a2w_sync_locations',
        'tags': ['develop', 'a2w'],
    }
}

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': (datetime.utcnow()-timedelta(hours=6)).replace(minute=15, second=0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG
def create_dag(**kwargs):
    # Assign kwargs
    dag_id = kwargs['dag_id']
    tags = kwargs['tags']
    schedule_interval = kwargs['schedule_interval']
    conn_type = kwargs['conn_type']
    offices = kwargs['offices']
    
    # Assign result to variable 'dag'
    @dag(
        default_args=default_args, 
        dag_id=dag_id,
        tags=tags,
        schedule_interval=schedule_interval,
        doc_md=__doc__,
    )
    def sync_locations():
        """
        DAG for Sync Locations
        """

# Task to check if RADAR is running
        @task
        def check_radar_service():
            radar.check_service()
        _check_radar_service = check_radar_service()

# Looping through each office to create a task for each
        for office_symbol in offices.keys():
    # Task to download and parse RADAR Locations
            @task(task_id=f'fetch_radar_{office_symbol}')
            def fetch_radar(office, offices, conn_type, format: str = 'json'):
# Acquire the locations from RADAR
                cwms_data = 'https://cwms-data.usace.army.mil/cwms-data'
                url = f'{cwms_data}/locations?office={office}&name=@&format={format}'
                location_kinds = json.loads(water.get_location_kind(conn_type=conn_type))
                with requests.Session() as s:
                    r = s.get(url, headers={'Content-Type': 'application/json'})
                    radar_locations_str = r.text
                # Check to see if it is html
                if 'DOCTYPE html' in radar_locations_str: raise AirflowFailException

                _radar_locations_str = radar_locations_str.translate(radar_locations_str.maketrans('', '', '\n\r\t')).replace('None', '')
                loc_json = json.loads(_radar_locations_str)
                locations = loc_json['locations']['locations']

                radar_locations = list()
                for location in locations:
                    # Get the kind_id from the location-kind list
                    kind_id = [
                        kind['id']
                        for kind in location_kinds
                        if kind['name'] == location['classification']['location-kind']
                    ]
                    office_id = offices[office]['id']
                    if (bounding_office := location['political']['bounding-office']) is not None:
                        if bounding_office in offices:
                            office_id = offices[bounding_office]['id']
                    
                    # Create a Location dataclass
                    loc = radar.Location(
                        office_id = office_id,
                        name = location['identity']['name'],
                        public_name = location['label']['public-name'],
                        kind_id = kind_id[0],
                        kind = location['classification']['location-kind']
                    )
                    # Create a Geometry dataclass with middle of North America as defaults
                    geo = radar.Geometry()
                    lat = location['geolocation']['latitude']
                    lon = location['geolocation']['longitude']

                    if isinstance(lat, float): geo.latitude = lat
                    if isinstance(lon, float): geo.longitude = lon

                    # Appending dictionary to post
                    radar_locations.append(
                        {
                            'office_id': loc.office_id,
                            'name': loc.name,
                            'public_name': loc.public_name,
                            'kind_id': loc.kind_id,
                            'kind': loc.kind,
                            'geometry' : {
                                'type': geo.type,
                                'coordinates': [
                                    geo.longitude,
                                    geo.latitude,
                                ]
                            }
                        }
                    )

                radar_locations_df = pd.json_normalize(
                    radar_locations
                )
                radar_locations_df = radar_locations_df.drop_duplicates(subset=['office_id', 'name'], ignore_index=True)
                
                radar_locations_json = radar_locations_df.to_json(orient='records').replace('null', 'None')
                
                return radar_locations_json



            @task(task_id=f'sync_to_water_{office_symbol}')
            def sync_to_water(locations, conn_type):
                is_empty = lambda v: 'no value' if v is None else v
                for item in eval(locations):
                    try:
                        water.sync_radar_locations(
                            payload={
                                "office_id": item["office_id"],
                                "name": item["name"],
                                "public_name": is_empty(item["public_name"]),
                                "kind_id": item["kind_id"],
                                "geometry": {
                                    "type": item["geometry.type"],
                                    "coordinates": item["geometry.coordinates"]
                                }
                            },
                            conn_type=conn_type,
                        )
                    except Exception as err:
                        print(err, '\n', item)
                        continue

# Tasks as objects
            _fetch_radar = fetch_radar(office_symbol, offices, conn_type)

            _sync_to_water = sync_to_water(_fetch_radar, conn_type)

            _check_radar_service >> _fetch_radar >> _sync_to_water

# Return the created DAG to the global scope
    return sync_locations()

# Getting a list of offices, create a dictionary from the list, and add NWDM and NWDP
offices_json = json.loads(sharedApi.get_offices())
offices_dict = {
    d['symbol']: d
    for d in offices_json
    if d['symbol'] is not None
}
# Add NWDM and NWDP to the dictionary
for nwd in ['NWDP', 'NWDM']:
    if nwd == 'NWDM':
        offices_dict[nwd] = offices_dict['NWD']
    else:
        offices_dict[nwd] = offices_dict['NWP']
    offices_dict[nwd]['symbol'] = nwd

# Expose to the global() allowing airflow to add to the DagBag
for key, val in implementation.items():
    d_id = val['dag_id']
    d_tags = val['tags']
    d_bucket=val['bucket']
    globals()[d_id] = create_dag(
        dag_id=d_id,
        tags=d_tags,
        schedule_interval='15 */3 * * *',
        conn_type=key,
        offices=offices_dict,
    )
