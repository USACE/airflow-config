import json
import logging
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
def milliseconds_since_epoch(dt=datetime.now()):
    return int(dt.timestamp() * 1000)
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
################################################################
def get_aware_device_metadata(token, devices, start, end):

    print(f'Calling get_aware_device_metadata() task')
            
    token   = json.loads(token)['token']        
    devices = json.loads(devices)
    limit   = 5
    startTs = int(start)*1000
    endTs   = int(end)*1000
    keys    = "lat,lon,IMEI,elev"

    logging.info('*****************')
    logging.info(f"Start: {datetime.fromtimestamp(int(start)).strftime('%Y-%m-%d %H:%M:%S.%f')}")
    logging.info(f"End: {datetime.fromtimestamp(int(end)).strftime('%Y-%m-%d %H:%M:%S.%f')}")
    logging.info('*****************')

    results_metadata = {}        
    
    for d_id, d_obj in devices.items():
        
        device_metadata = {}                
        print('-'*50)
        print(d_id, d_obj)

        # Query AWARE API for the metadata (which is actually in the timeseries)
        r = get_device_ts_data(token, d_id, startTs, endTs, keys, limit)
        
        # Loop through metadata results
        for field, ts_obj in r.json().items():
            device_metadata[field] = ts_obj[0] #assign the first (latest) value
            # print(f'Adding {field}->{ts_obj[0]} to metdata result payload')

        # Add the name for the device
        device_metadata['name'] = d_obj['name']        

        results_metadata[d_id] = device_metadata        

    #print(json.dumps(results_metadata))
    return json.dumps(results_metadata)
################################################################
def send_device_command(token, device_id, params):

    token   = json.loads(token)['token']
    conn = get_connection()
    
    payload = {}     
    payload['method'] = "AWARECommands"
    payload['timeout'] = 5000

    params['issuer']      = [conn.login]
    params['deviceType']  = ["USACE"]    
    params['cmdSendDate'] = str(milliseconds_since_epoch())
    params['node'][0]     = params['node'][0].replace('USACE', '')
              
    payload['params'] = params
    
    h = HttpHook(http_conn_id=conn.conn_id, method='POST')
    headers = {"Content-Type": "application/json", "X-Authorization": "Bearer "+token}
    endpoint = f"/api/plugins/rpc/twoway/{device_id}"   
    r = h.run(endpoint=endpoint, json=payload, headers=headers)