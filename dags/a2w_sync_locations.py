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

import json
import requests
import pandas as pd
from datetime import datetime, timedelta

from sqlalchemy.sql.operators import is_

import helpers.sharedApi as sharedApi
import helpers.radar as radar
import helpers.water as water

from airflow import AirflowException
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

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

pandas_columns = [
    'office_id',
    'name',
    'public_name',
    'kind_id',
    'geometry',
]

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

# Looping through each office to create a task for each
        for office_symbol in offices.keys():
# Task to download and parse RADAR Locations
            @task(task_id=f'radar_locations_{office_symbol}')
            def radar_locations(office, offices, conn_type, format: str = 'json'):
                cwms_data = 'https://cwms-data.usace.army.mil/cwms-data'
                url = f'{cwms_data}/locations?office={office}&name=@&format={format}'
                location_kinds = json.loads(water.get_location_kind(conn_type=conn_type))
                with requests.Session() as s:
                    r = s.get(url)
                    radar_locations_str = r.text

                if 'DOCTYPE html' in radar_locations_str:
                    raise AirflowException
                else:
                    _radar_locations_str = radar_locations_str.translate(radar_locations_str.maketrans('', '', '\n\r\t')).replace('None', '')
                    loc_json = json.loads(_radar_locations_str)
                    locations = loc_json['locations']['locations']

                    post_locations = list()
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
                        post_locations.append(
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

                return post_locations

# Task to get the water locations, compare to RADAR locations and only get differences
            @task(task_id=f'water_locations_{office_symbol}')
            def water_locations(office_id, locations, conn_type: str='develop'):
                # context = get_current_context()
                # ti = context['ti']
                df_radar = pd.json_normalize(
                    # ti.xcom_pull()
                    locations
                )
                df_water = pd.json_normalize(
                    water.get_location_office_id(office_id, conn_type),
                )
                # These are the ones that are in both, i.e. PUT
                # Append the water.slug column to radar DataFrame
                join_inner = pd.concat([df_water, df_radar], keys=['water', 'radar'], axis=1, join='inner')
                to_update = join_inner.water[[
                        'id',
                        # 'office_id',
                        'state_id',
                        'name',
                        'slug',
                    ]]
                to_update = to_update.join(
                    [
                        join_inner.radar.public_name,
                        join_inner.radar.kind_id,
                        join_inner.radar.kind,
                        join_inner.radar.office_id,
                        join_inner.radar['geometry.type'],
                        join_inner.radar['geometry.coordinates'],
                    ])
                to_update = to_update[to_update['id'].notna()]
                put_json = to_update.to_json(orient='records')
                put_json = put_json.replace('null', 'None')
                put_list = eval(put_json)

                is_empty = lambda v: 'no value' if v is None else v

                put_list_new = [
                    json.dumps({
                        "id": item["id"],
                        "office_id": item["office_id"],
                        "name": item["name"],
                        "public_name": is_empty(item["public_name"]),
                        "slug": item["slug"],
                        "kind_id": item["kind_id"],
                        "kind": item["kind"],
                        "geometry": {
                            "type": item["geometry.type"],
                            "coordinates": item["geometry.coordinates"]
                        }
                     })
                    for item in put_list
                ]

                # This returns locations in RADAR not in Water, i.e. POST
                join_outer = pd.concat([df_water, df_radar], keys=['water', 'radar'], axis=1, join='outer')
                radar_only = join_outer.radar
                post_json = radar_only.to_json(orient='records')
                post_json = post_json.replace('null', 'None')
                post_json = eval(post_json)
                post_list_new = [
                    json.dumps({
                        "office_id": item["office_id"],
                        "name": item["name"],
                        "public_name": is_empty(item["public_name"]),
                        "kind_id": item["kind_id"],
                        "geometry": {
                            "type": item["geometry.type"],
                            "coordinates": item["geometry.coordinates"]
                        }
                     })
                    for item in post_json
                ]

                result = {
                    'put': put_list_new,
                    'post': post_list_new,
                }

                return result

# Task to post the resulting parsed RADAR returned text
            @task(task_id=f'put_locations_{office_symbol}')
            def put_locations(locations, conn_type: str='develop'):
                for loc in locations['put']:
                    _loc = json.loads(loc)
                    try:
                        water.put_location(
                            id=_loc['id'],
                            payload=_loc,
                            conn_type=conn_type,
                        )
                    except Exception as err:
                        print(err, '\n', loc)
                        continue

# Task to post the resulting parsed RADAR returned text
            @task(task_id=f'post_locations_{office_symbol}')
            def post_locations(locations, conn_type: str='develop'):
                for loc in locations['post']:
                    try:
                        water.post_location(
                            payload=json.loads(loc),
                            conn_type=conn_type,
                        )
                    except Exception as err:
                        print(err, '\n', loc)
                        continue

# Tasks as objects
            _check_radar_service = check_radar_service()
            _radar_locations = radar_locations(office_symbol, offices, conn_type)
            _water_locations = water_locations(offices[office_symbol]['id'], _radar_locations, conn_type)
            _put_locations = put_locations(_water_locations, conn_type)
            _post_locations = post_locations(_water_locations, conn_type)
# Order task objects with checking RADAR a single task for all other tasks
            _check_radar_service >> _radar_locations >> _water_locations >> [_post_locations, _put_locations]

# Return the created DAG to the global scope
    return sync_locations()

# Getting a list of offices, create a dictionary from the list, and add NWDM and NWDP
offices_json = json.loads(sharedApi.get_offices())
offices_dict = {
    d['symbol']: d
    for d in offices_json
    if d['symbol'] is not None and d['symbol'] == 'LRH'
}
# Add NWDM and NWDP to the dictionary
# for nwd in ['NWDP', 'NWDM']:
#     if nwd == 'NWDM':
#         offices_dict[nwd] = offices_dict['NWD']
#     else:
#         offices_dict[nwd] = offices_dict['NWP']
#     offices_dict[nwd]['symbol'] = nwd

# Expose to the global() allowing airflow to add to the DagBag
for key, val in implementation.items():
    d_id = val['dag_id']
    d_tags = val['tags']
    d_bucket=val['bucket']
    globals()[d_id] = create_dag(
        dag_id=d_id,
        tags=d_tags,
        schedule_interval='15 */6 * * *',
        conn_type=key,
        offices=offices_dict,
    )
