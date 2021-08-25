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

from airflow.operators.python import get_current_context
import helpers.sharedApi as sharedApi
import helpers.radar as radar
import helpers.water as water
import json
import requests
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow import AirflowException

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
                            kind_id = kind_id[0]
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

# Task to post the resulting parsed RADAR returned text
            @task(task_id=f'post_locations_{office_symbol}')
            def post_locations(conn_type: str='develop'):
                context = get_current_context()
                ti = context['ti']

                for p in ti.xcom_pull():
                    try:
                        water.post_locations(
                            payload=p,
                            conn_type=conn_type,
                        )
                    except Exception as err:
                        print(err, '\n', p)
                        continue
# Tasks as objects
            _radar_locations = radar_locations(office_symbol, offices, conn_type)
            _post_locations = post_locations(conn_type)
# Order task objects with checking RADAR a single task for all other tasks
            _check_radar_service >> _radar_locations >> _post_locations

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
        schedule_interval='15 */6 * * *',
        conn_type=key,
        offices=offices_dict,
    )
