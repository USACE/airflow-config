import json
from datetime import datetime

from airflow.hooks.base_hook import BaseHook
from airflow.providers.http.hooks.http import HttpHook


def get_connection():
    conn = BaseHook.get_connection('FLASHFLOOD')

    return conn
################################################################
def flashfloodinfo_authenticate():
    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method='POST')
    r = h.run(endpoint='/api/auth/login', data=json.dumps({'username': conn.login, 'password': conn.password}), headers={"Content-Type": "application/json"})

    return json.dumps(r.json())
################################################################
def flashflood_get_customer(token):
    
    token = json.loads(token)['token']

    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method='GET')    
    endpoint = f'/api/auth/user'
    headers = {"Content-Type": "application/json", "X-Authorization": "Bearer "+token}
    r = h.run(endpoint=endpoint, headers=headers)

    return r.json()['customerId']['id']
################################################################
def epoch_ms_to_human(ts):
        """ Convert epoch miliseconds to human datetime string (UTC)

        Arguments: 
            ts {integer}

        """

        return datetime.utcfromtimestamp(ts/1000).strftime('%Y-%m-%dT%H:%M:%SZ')
################################################################
def get_aware_devices(token, customer_id):
    
    token = json.loads(token)['token']
    
    print(f'token: {token}')
    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method='GET')
    endpoint = f'/api/customer/{customer_id}/devices?limit=1000'
    headers = {"Content-Type": "application/json", "X-Authorization": "Bearer "+token}
    r = h.run(endpoint=endpoint, headers=headers)

    response = r.json()
    devices = {}
    
    for device in response['data']:
        d_name  = device['name']    #USACE00504
        d_label = device['label'] 
        d_type  = device['type'] 
        d_entity_type = device['id']['entityType']    #normally 'DEVICE'
        d_id    = device['id']['id']            # UUID of device
        # print(f' entityType: {d_entity_type} - id: {d_id} - name: {d_name} - label: {d_label} - type: {d_type}')
        
        d = {"name": d_name, "type":d_type, "label":d_label, "entityType": d_entity_type}
        if d_name != "USACEQueue":
            devices[d_id] = d   

    return json.dumps(devices)
################################################################
def get_device_ts_data(token, device_id, startTs, endTs, keys, limit):

    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method='GET')
    headers = {"Content-Type": "application/json", "X-Authorization": "Bearer "+token}
    endpoint = (
        f"/api/plugins/telemetry/DEVICE/{device_id}"
        f"/values/timeseries?limit={limit}&agg=NONE&startTs={startTs}&endTs={endTs}&keys={keys}"
    )
    # print(f'calling ts data endpoint: {endpoint} with token {token}')
    r = h.run(endpoint=endpoint, headers=headers)

    return r