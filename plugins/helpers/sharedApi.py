import json
import requests
# from airflow.hooks.base_hook import BaseHook
# from airflow.providers.http.hooks.http import HttpHook
from airflow import AirflowException

SHARED_API_ROOT = 'https://shared-api.rsgis.dev'


################################################################ 
# def get_develop_connection():    
#     return BaseHook.get_connection('CUMULUS_DEVELOP')
# ################################################################ 
# def get_connection():    
#     return BaseHook.get_connection('CUMULUS_STABLE')
# ################################################################ 
# def notify_acquirablefile(acquirable_id, datetime, s3_key, conn_type):
    
#     payload = {"datetime": datetime, "file": s3_key, "acquirable_id": acquirable_id}
#     print(f'Sending payload: {payload}')

#     if conn_type.lower() == 'develop':
#         conn = get_develop_connection()
#     else:
#         conn = get_connection()

#     h = HttpHook(http_conn_id=conn.conn_id, method='POST')
#     endpoint = f"/acquirablefiles?key={conn.password}"
#     headers = {"Content-Type": "application/json"}
#     r = h.run(endpoint=endpoint, json=payload, headers=headers)       

#     return json.dumps(r.json())
################################################################
def get_offices():
    # Offices endpoint returns a list of objects
    r = requests.get(f'{SHARED_API_ROOT}/offices')
    
    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.text