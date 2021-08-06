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
from airflow.utils.db import provide_session
from airflow.models import XCom


# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime.utcnow()-timedelta(minutes=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@hourly',
}
# Get RADAR Locations
def radar_locations(ti, office, format: str = 'json'):
    url = Variable.get('CWMSDATA') + f'/locations?office={office.symbol}&name=@&format={format}'
    with requests.Session() as s:
        r = s.get(url)
        t = r.text
    return t

def parse_locations(ti, office: str):
    location_kinds = json.loads(water.get_location_kind())

    radar_locations_str = ti.xcom_pull()
    _radar_locations_str = radar_locations_str.translate(radar_locations_str.maketrans('', '', '\n\r\t')).replace('None', '')
    loc_json = json.loads(_radar_locations_str)
    locations = loc_json['locations']['locations']
    
    post_locations = list()
    for location in locations:
        # Get the kind_id from the location-kind list
        if location['classification']['location-kind'] == 'SITE':
            kind_id = [
                kind['id']
                for kind in location_kinds
                if kind['name'] == location['classification']['location-kind']
            ]
            # Create a Location dataclass
            loc = radar.Location(
                office_id = office.id,
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

@provide_session
def cleanup_xcom(context, session=None):
    dag_id = context["ti"]["dag_id"]
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

# Assign result to variable 'dag'
with DAG(
    default_args=default_args, dag_id='RADAR_SYNC',
    tags=['RADAR-SYNC'],
    on_success_callback=cleanup_xcom,
) as dag:
    offices = json.loads(sharedApi.get_offices())
    for _office in offices:
        office = radar.Office(**_office)
        if office.symbol is not None and office.symbol == 'LRH':
            # Get the RADAR locations task to XCOM
            task_get_radar_locations = PythonOperator(
                task_id=f'radar_locations_{office.symbol}',
                python_callable=radar_locations,
                op_kwargs={
                    'office': office,
                }
            )
            # Parse the XCOM return_value returning a list of payloads
            task_parse_radar_locations = PythonOperator(
                task_id=f'parse_locations_{office.symbol}',
                python_callable=parse_locations,
                op_kwargs={
                    'office': office,
                }
            )
            # Post resulting parse to water-api
            task_post_radar_locations = PythonOperator(
                task_id=f'post_locations_{office.symbol}',
                python_callable=post_locations,
            )

            task_get_radar_locations >> task_parse_radar_locations >> task_post_radar_locations