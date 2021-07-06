from dataclasses import dataclass, field
import json
import re
import requests
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow import AirflowException

RADAR_API_ROOT = 'http://cwms-data.usace.army.mil/cwms-data'

@dataclass
class Office:
    id: str
    name: str
    symbol: str
    parent_id: str

@dataclass
class Location:
    office_id: str = None
    name: str = None
    public_name: str = None
    kind_id: str = None
@dataclass
class Geometry:
    type: str = "Point"
    latitude: float = 47
    longitude: float = 101
    
    
def check_service():
    r = requests.get(f'{RADAR_API_ROOT}/offices?format=json')
    if r.status_code == 200:
        return True
    else:
        raise AirflowException("RADAR API SERVICE DOWN!")
##############################################################
