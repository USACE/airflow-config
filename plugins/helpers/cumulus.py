import json
from airflow.hooks.base_hook import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow import AirflowException

import requests

S3_ACQUIRABLE_PREFIX = 'cumulus/acquirables'

acquirables = {
    'cbrfc-mpe': '2429db9a-9872-488a-b7e3-de37afc52ca4',
    'hrrr-total-precip': 'd4e67bee-2320-4281-b6ef-a040cdeafeb8',
    'nbm-co-01h': 'd4aa1d8d-ce06-47a0-9768-e817b43a20dd',
    'nohrsc-snodas-unmasked': '87819ceb-72ee-496d-87db-70eb302302dc',
    'ncep-rtma-ru-anl-airtemp': '22678c3d-8ac0-4060-b750-6d27a91d0fb3',
    'ncep-mrms-v12-multisensor-qpe-01h-pass1': '87a8efb7-af6f-4ece-a97f-53272d1a151d',
    'ncep-mrms-v12-multisensor-qpe-01h-pass2': 'ccc252f9-defc-4b25-817b-2e14c87073a0',
    'ndfd-conus-qpf-06h': 'f2fee5df-c51f-4774-bd41-8ded1eed6a64',
    'ndfd-conus-airtemp': '5c0f1cfa-bcf8-4587-9513-88cb197ec863',
    'ndgd-ltia98-airtemp': 'b27a8724-d34d-4045-aa87-c6d88f9858d0',
    'ndgd-leia98-precip': '4d5eb062-5726-4822-9962-f531d9c6caef',
    'prism-ppt-early': '099916d1-83af-48ed-85d7-6688ae96023d',
    'prism-tmax-early': '97064e4d-453b-4761-8c9a-4a1b979d359e',
    'prism-tmin-early': '11e87d14-ec54-4550-bd95-bc6eba0eba08',
    'wrf-columbia-precip': 'ec926de8-6872-4d2b-b7ce-6002221babcd',
    'wrf-columbia-airtemp': '552bf762-449f-4983-bbdc-9d89daada260',
    'wpc-qpf-2p5km': '0c725458-deb7-45bb-84c6-e98083874c0e',
    'nsidc-ua-swe-sd-v1': '4b0f8d9c-1be4-4605-8265-a076aa6aa555',
    'serfc-qpf-06h': '355d8d9b-1eb4-4f1d-93b7-d77054c5c267',
    'serfc-qpe-01h': '5365399a-7aa6-4df8-a91a-369ca87c8bd9',
    'lmrfc-qpf-06h': 'fca9e8a4-23e3-471f-a56b-39956055a442',
    'lmrfc-qpe-01h': '660ce26c-9b70-464b-8a17-5c923752545d'
}
################################################################ 
def get_develop_connection():    
    return BaseHook.get_connection('CUMULUS_DEVELOP')
################################################################ 
def get_connection():    
    return BaseHook.get_connection('CUMULUS_STABLE')
################################################################ 
def notify_acquirablefile(acquirable_id, datetime, s3_key, conn_type):

    payload = {"datetime": datetime, "file": s3_key, "acquirable_id": acquirable_id}
    print(f'Sending payload: {payload}')

    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    h = HttpHook(http_conn_id=conn.conn_id, method='POST')
    endpoint = f"/acquirablefiles?key={conn.password}"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, json=payload, headers=headers)       

    return json.dumps(r.json())
################################################################