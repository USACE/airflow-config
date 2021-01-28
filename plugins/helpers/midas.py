import json
from airflow.hooks.base_hook import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow import AirflowException

def get_connection():
    conn = BaseHook.get_connection('MIDAS')

    return conn
################################################################    
def get_aware_instruments(): 

    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method='GET')    
    endpoint = '/aware/data_acquisition_config'
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    return json.dumps(r.json())