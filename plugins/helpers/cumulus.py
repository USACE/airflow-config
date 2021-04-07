import json
from airflow.hooks.base_hook import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow import AirflowException

S3_ACQUIRABLE_PREFIX = 'cumulus/acquirables'

acquirables = {
    'cbrfc-mpe': '2429db9a-9872-488a-b7e3-de37afc52ca4',
    'nohrsc-snodas-unmasked': '87819ceb-72ee-496d-87db-70eb302302dc',
    'ncep-rtma-ru-anl': '22678c3d-8ac0-4060-b750-6d27a91d0fb3',
    'ncep-mrms-v12-multisensor-qpe-01h-pass1': '87a8efb7-af6f-4ece-a97f-53272d1a151d',
    'ncep-mrms-v12-multisensor-qpe-01h-pass2': 'ccc252f9-defc-4b25-817b-2e14c87073a0',
    'ndgd-ltia98-airtemp': 'b27a8724-d34d-4045-aa87-c6d88f9858d0',
    'ndgd-leia98-precip': '4d5eb062-5726-4822-9962-f531d9c6caef',
    'prism-ppt-early': '099916d1-83af-48ed-85d7-6688ae96023d',
    'prism-tmax-early': '97064e4d-453b-4761-8c9a-4a1b979d359e',
    'prism-tmin-early': '11e87d14-ec54-4550-bd95-bc6eba0eba08',
    'wpc-qpf-2p5km': '0c725458-deb7-45bb-84c6-e98083874c0e'
}

def get_connection():
    
    conn = BaseHook.get_connection('CUMULUS')
    return conn
################################################################ 
def notify_acquirablefile(acquirable_id, datetime, s3_key):
    
    payload = {"datetime": datetime, "file": s3_key, "acquirable_id": acquirable_id}

    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method='POST')
    endpoint = f"/acquirablefiles?key={conn.password}"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, json=payload, headers=headers)       

    return json.dumps(r.json())
################################################################