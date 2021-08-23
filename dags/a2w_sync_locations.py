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

from airflow.operators.python import PythonOperator
import helpers.sharedApi as sharedApi
import helpers.radar as radar
import helpers.water as water
import json
import requests
from datetime import datetime, timedelta
from textwrap import dedent

from airflow.models.dag import DAG
from airflow.models.variable import Variable
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
# Get RADAR Locations
def radar_locations(ti, office, offices, conn_type, format: str = 'json'):
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
            lon  = location['geolocation']['longitude']
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
                            geo.latitude,
                            geo.longitude
                        ]
                    }
                }
            )
        return json.dumps(post_locations)

def post_locations(ti, conn_type: str='develop'):
    payload = json.loads(ti.xcom_pull())
    for p in payload:
        try:
            water.post_locations(
                payload=p,
                conn_type=conn_type,
            )
        except Exception as err:
            print(err, '\n', p)
            continue

def create_dag(**kwargs):
    # Assign kwargs
    dag_id = kwargs['dag_id']
    tags = kwargs['tags']
    schedule_interval = kwargs['schedule_interval']
    conn_type = kwargs['conn_type']
    offices = kwargs['offices']
    
    # Assign result to variable 'dag'
    with DAG(
        default_args=default_args, 
        dag_id=dag_id,
        tags=tags,
        schedule_interval=schedule_interval,
        doc_md=__doc__
    ) as dag:
        for office_symbol in offices.keys():
            # Get the RADAR locations task to XCOM
            task_get_radar_locations = PythonOperator(
                task_id=f'radar_locations_{office_symbol}',
                python_callable=radar_locations,
                op_kwargs={
                    'office': office_symbol,
                    'offices': offices,
                    'conn_type': conn_type,
                }
            )
            # Post resulting parse to water-api
            task_post_radar_locations = PythonOperator(
                task_id=f'post_locations_{office_symbol}',
                python_callable=post_locations,
                op_kwargs={
                    'conn_type': conn_type,
                }
            )

            task_get_radar_locations >> task_post_radar_locations

        return dag

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
