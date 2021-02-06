import json
from airflow.hooks.base_hook import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow import AirflowException

def get_connection():
    conn = BaseHook.get_connection('MIDAS')

    return conn
################################################################    
def get_aware_param_config(): 

    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method='GET')    
    endpoint = '/aware/data_acquisition_config'
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    return json.dumps(r.json())
################################################################ 
def get_aware_instruments(): 

    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method='GET')    
    endpoint = '/projects/82c07c9a-9ec8-4ff5-850c-b1d74ffb5e14/instruments'
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    return json.dumps(r.json())