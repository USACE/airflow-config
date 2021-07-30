"""
### Documentation
"""

from airflow.operators.python import PythonOperator
import helpers.sharedApi as sharedApi
import helpers.radar as radar
import helpers.water as water
import helpers.downloads as downloads
import json
from typing import Any, Dict, List
import requests
from datetime import datetime, timedelta
from textwrap import dedent

from airflow.models.dag import DAG
from airflow.models.variable import Variable

bucket = 'cwbi-data-develop'
base_key = 'airflow/data_exchange'

default_args = {
    'owner': 'airflow',
    'start_date': datetime.utcnow()-timedelta(minutes=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@hourly',
}

def radar_locations(ti, *, office: str, format: str = 'json'):
    url = Variable.get('CWMSDATA') + f'/locations?office={office}&format={format}'
    key=f'{base_key}/{ti.dag_id}/{ti.task_id}'
    with requests.session() as s:
        r = s.get(url)
        t = r.text
        ti.xcom_push(key='radar_locations', value=t)

def parse_locations(ti, office: str):
    key=f'{base_key}/{ti.dag_id}/{ti.task_id}'
    loc_str = ti.xcom_pull(key='radar_locations', task_ids='radar_locations')
    location_kinds = json.loads(water.get_location_kind())

    _loc_str = loc_str.translate(loc_str.maketrans('', '', '\n\r\t')).replace('None', '')
    loc_json = json.loads(_loc_str)
    locations = loc_json['locations']['locations']
    
    post_locations = list()
    for location in locations:
        # Get the kind_id from the location-kind list
        kind_id = [
            kind['id']
            for kind in location_kinds
            if kind['name'] == location['classification']['location-kind']
        ]
        # Create a Location dataclass
        loc = radar.Location(
            office_id = office,
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
        # Create a Political dataclass to get the bounding office
        political = radar.Political()
        political.bounding_office = location['political']['bounding-office']

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
    
    downloads.upload_string_s3(json.dumps(post_locations), bucket, key)

    ti.xcom_push(key='parse_locations', value=f'{bucket}/{key}')

def post_locations(ti, conn_type: str='develop'):
    bucket, key = ti.xcom_pull(key='parse_locations', task_ids='parse_locations').split('/', maxsplit=1)
    payload = downloads.read_s3key(key, bucket)
    water.post_locations(payload, conn_type)

def create_dag(
    dag_id: str, 
    default_args: Dict[str, Any],
    office_symbol: str, 
    office_id: str,
    tags: List[str]
    ):
    with DAG(default_args=default_args, dag_id=dag_id, tags=tags) as dag:

        task_get_radar_locations = PythonOperator(
            task_id='radar_locations',
            python_callable=radar_locations,
            op_kwargs={
                'office': office_symbol,
            }
        )

        task_parse_radar_locations = PythonOperator(
            task_id='parse_locations',
            python_callable=parse_locations,
            op_kwargs={
                'office': office_id,
            }
        )

        task_post_locations = PythonOperator(
            task_id='post_locations',
            python_callable=post_locations,
        )

        task_get_radar_locations >> task_parse_radar_locations >> task_post_locations

    return dag

offices = json.loads(sharedApi.get_offices())
for _office in offices:
    office = radar.Office(**_office)
    dag_prefix = 'radar_locations'
    dag_id = f'{dag_prefix}_{office.symbol}'

    if office.symbol is not None:
        globals()[dag_id] = create_dag(
                dag_id, 
                default_args,
                office.symbol,
                office.id,
                [dag_prefix],
            )
