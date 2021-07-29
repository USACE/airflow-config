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
from airflow.models.baseoperator import BaseOperator
import requests
from datetime import datetime, timedelta
from textwrap import dedent

from airflow.decorators import dag, task
from airflow.models.dag import DAG
from airflow.models.variable import Variable

bucket = 'cwbi-data-develop'
base_key = 'airflow/data_exchange'

default_args = {
    'owner': 'airflow',
    "start_date": datetime.utcnow() - timedelta(hours=2),
}

def radar_locations(ti, *, office: str, format: str = 'json'):
    url = Variable.get('CWMSDATA') + '/location?office={office}&format={format}'
    key=f'{base_key}/{ti.dag_id}/{ti.task_id}'
    with requests.Session() as s:
        r = s.get(url)
        t = r.text
        ti.xcom_push(key='radar_locations', value=t)

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
                'office': office_id,
            }
        )

        task_get_radar_locations

offices = json.loads(sharedApi.get_offices())

for shared_office in offices:
    office = radar.Office(**shared_office)
    dag_prefix = "radar_locations"
    dag_id = f"{dag_prefix}_{office.symbol}"

    globals()[dag_id] = create_dag(
            dag_id, 
            default_args,
            office.symbol,
            office.id,
            [dag_prefix],
        )
