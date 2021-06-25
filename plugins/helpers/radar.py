import json
import requests
# from airflow.hooks.base_hook import BaseHook
# from airflow.providers.http.hooks.http import HttpHook
from airflow import AirflowException

RADAR_API_ROOT = 'http://cwms-data.usace.army.mil/cwms-data'

##############################################################
def check_service():
    r = requests.get(f'{RADAR_API_ROOT}/offices?format=json')
    if r.status_code == 200:
        return True
    else:
        raise AirflowException("RADAR API SERVICE DOWN!")
##############################################################
