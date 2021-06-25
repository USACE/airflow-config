import json
from airflow.hooks.base_hook import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow import AirflowException

################################################################ 
def get_develop_connection():    
    return BaseHook.get_connection('WATER_DEVELOP')
################################################################ 
def get_connection():    
    return BaseHook.get_connection('WATER_STABLE')
################################################################ 