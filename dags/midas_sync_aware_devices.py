import json
import logging
from airflow import DAG

from airflow import AirflowException
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import task, get_current_context

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import helpers.aware as aware
import helpers.midas as midas

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
with DAG(
    'midas_sync_aware_devices',
    default_args=default_args,
    description='Synchronize AWARE Devices to MIDAS Instruments',
    start_date=(datetime.utcnow()-timedelta(days=1)).replace(hour=18, minute=0, second=0),
    tags=['midas'],
    # schedule_interval='@daily',
    schedule_interval='0 19 * * *'
    
) as dag:
    # dag.doc_md = __doc__
    ##############################################
    def sync_devices_to_midas(aware_devices, metadata, midas_aware_param_config_instruments, midas_aware_instruments):

        aware_devices = json.loads(aware_devices)
        midas_aware_param_config_instruments = json.loads(midas_aware_param_config_instruments)
        midas_aware_instruments = json.loads(midas_aware_instruments)

       
        midas_aware_param_config_instruments_dict = {}
        # Convert midas_aware_param_config_instruments list results to dict with aware_id as key
        for i in midas_aware_param_config_instruments:            
            midas_aware_param_config_instruments_dict[i['aware_id']] = i

        
        instruments_dict = {}
        # Convert midas_aware_instruments list results to dict with aware_id as key
        for x in midas_aware_instruments:            
            instruments_dict[x['id']] = x           


        project_id = "82c07c9a-9ec8-4ff5-850c-b1d74ffb5e14"
        metadata = json.loads(metadata)

        insert_payload = []        

        # Fake new gage not in MIDAS
        # aware_devices['d0574790-4fc3-11eb-a888-07a0a8e5af03'] = {'name': 'USACE00999', 'type': 'USACE', 'label': None, 'entityType': 'DEVICE'}   
        
        for d_id, d_obj in aware_devices.items(): 

            #if not any(obj['name'] == f"AWARE Gage {d_obj['name']}" for obj in midas_aware_instruments):               
            print('-'*30) 

            # Metadata may not be available.  It requires a timeseries call
            # and may not result in values if the window is not large enough
            try:
                lat = float(metadata[d_id]['lat']['value'])
                lon = float(metadata[d_id]['lon']['value'])
            except:
                logging.warning(f"lat/lon not available for: {d_obj['name']}")
                lat = 0.0
                lon = 0.0           
            
            if d_id not in midas_aware_param_config_instruments_dict.keys():            
                
                print('#'*50)
                print(f"AWARE Gage {d_obj['name']} NOT in MIDAS, preparing to add...")           
                instrument_obj = {}                          
                
                instrument_obj['aware_id'] = d_id
                instrument_obj['status_id'] = "e26ba2ef-9b52-4c71-97df-9e4b6cf4174d"
                instrument_obj['status'] = "active"
                instrument_obj['status_time'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                # instrument_obj['slug']: f"aware-gage-{d_obj['name']}"
                instrument_obj['formula'] = None
                instrument_obj['name'] = d_obj['name']
                instrument_obj['type_id'] = "98a61f29-18a8-430a-9d02-0f53486e0984"
                instrument_obj['type'] = "Instrument"
                instrument_obj['geometry'] = {"type": "Point", "coordinates": [lon, lat]}
                instrument_obj['station'] = None
                instrument_obj['offset'] = None
                instrument_obj['project_id'] = project_id

                insert_payload.append(instrument_obj) 

                for k,v in instrument_obj.items():
                    print(f'{k}: {v}')

            else:                
                print(f"{d_obj['name']} already in MIDAS.")
                #
                # Check MIDAS VS AWARE Coordinates and update where needed
                #
                midas_instrument_id = midas_aware_param_config_instruments_dict[d_id]['instrument_id']                
                # print(f"Checking coordinates for: {d_obj['name']}")                
                midas_lon = instruments_dict[midas_instrument_id]['geometry']['coordinates'][0]
                midas_lat = instruments_dict[midas_instrument_id]['geometry']['coordinates'][1]
                
                if round(float(midas_lat),4) != round(float(lat),4) or round(float(midas_lon),4) != round(float(lon),4):
                    print('NOTICE: **MIDAS Coordinates need updated**')
                    print(f'MIDAS Coords: lat={midas_lat}, lon={midas_lon}')
                    print(f'AWARE Coords: lat={lat}, lon={lon}')
                    midas_project_id = midas_aware_param_config_instruments_dict[d_id]['project_id']

                    # Only update MIDAS if the coordinates aren't defaulting to 0,0
                    if int(lat) != 0 and int(lon) != 0:
                        update_payload = {"type":"Point","coordinates":[round(float(lon),4), round(float(lat),4)]}
                    
                        conn = midas.get_connection()
                        h = HttpHook(http_conn_id=conn.conn_id, method='PUT')    
                        # endpoint = f'/projects/{midas_project_id}/instruments/{midas_instrument_id}/geometry?key_id={conn.login}&key={conn.password}'
                        endpoint = f'/projects/{midas_project_id}/instruments/{midas_instrument_id}/geometry?key={conn.password}'
                        headers = {"Content-Type": "application/json"}
                        r = h.run(endpoint=endpoint, json=update_payload, headers=headers)
                    else:
                        print(f'Ignoring AWARE Coordinates:{lat},{lon}')

                else:
                    print('MIDAS/AWARE Coordinates match. All good :-)')


        print(json.dumps(insert_payload)) 

        if len(insert_payload) > 0:
            conn = midas.get_connection()
            h = HttpHook(http_conn_id=conn.conn_id, method='POST')    
            # endpoint = f'/projects/{project_id}/instruments?key_id={conn.login}&key={conn.password}'
            endpoint = f'/projects/{project_id}/instruments?key={conn.password}'
            headers = {"Content-Type": "application/json"}
            r = h.run(endpoint=endpoint, json=insert_payload, headers=headers)

        ########### Example payload ###################
        # [{
        #     "aware_id": "fdbe0c62-bcf7-49be-af7b-316af478dfc2",
        #     "status_id": "e26ba2ef-9b52-4c71-97df-9e4b6cf4174d",
        #     "status": "active",
        #     "status_time": "2021-01-21T19:51:55.261Z",
        #     "slug": "aware-gage-1",
        #     "formula": null,
        #     "name": "Demo Piezometer 2",
        #     "type_id": "98a61f29-18a8-430a-9d02-0f53486e0984",
        #     "type": "Instrument",
        #     "geometry": {
        #         "type": "Point",
        #         "coordinates": [
        #             -80.8,
        #             26.7
        #         ]
        #     },
        #     "station": null,
        #     "offset": null,
        #     "project_id": "82c07c9a-9ec8-4ff5-850c-b1d74ffb5e14"
        # }]

        return    
    ##############################################    

    flashflood_authenticate_task = PythonOperator(
            task_id='flashflood_authenticate',
            python_callable=aware.flashfloodinfo_authenticate
        )

    flashflood_get_customer = PythonOperator(
            task_id='flashflood_get_customer',
            python_callable=aware.flashflood_get_customer,
            op_kwargs={                
                'token': "{{task_instance.xcom_pull(task_ids='flashflood_authenticate')}}"
                }
        )    

    get_aware_devices_task = PythonOperator(
            task_id='get_aware_devices',
            python_callable=aware.get_aware_devices,
            op_kwargs={                
                'token': "{{task_instance.xcom_pull(task_ids='flashflood_authenticate')}}",
                'customer_id': "{{task_instance.xcom_pull(task_ids='flashflood_get_customer')}}"
                }
        )

    get_midas_aware_instruments_task = PythonOperator(
            task_id='get_midas_aware_instruments',
            python_callable=midas.get_aware_instruments,
            op_kwargs={
                'conn_type': "stable",
            }           
        )

    get_midas_aware_param_config_task = PythonOperator(
            task_id='get_midas_aware_param_config',
            python_callable=midas.get_aware_param_config,
            op_kwargs={
                'conn_type': "stable",
            }          
        )

    get_aware_devices_metadata_task = PythonOperator(
            task_id='get_aware_device_metadata',
            python_callable=aware.get_aware_device_metadata,
            op_kwargs={                
                'token': "{{task_instance.xcom_pull(task_ids='flashflood_authenticate')}}",
                'devices': "{{task_instance.xcom_pull(task_ids='get_aware_devices')}}",
                'start': "{{ (execution_date - macros.timedelta(hours=720)).int_timestamp }}",
                'end': "{{ (execution_date + macros.timedelta(hours=48)).int_timestamp }}",
                }
        )    

    sync_devices_to_midas_task = PythonOperator(
            task_id='sync_devices_to_midas',
            python_callable=sync_devices_to_midas,
            op_kwargs={                
                'aware_devices': "{{task_instance.xcom_pull(task_ids='get_aware_devices')}}",
                #'token': "{{task_instance.xcom_pull(task_ids='midas_authenticate')}}",
                'metadata': "{{task_instance.xcom_pull(task_ids='get_aware_device_metadata')}}",
                'midas_aware_param_config_instruments': "{{task_instance.xcom_pull(task_ids='get_midas_aware_param_config')}}",
                'midas_aware_instruments': "{{task_instance.xcom_pull(task_ids='get_midas_aware_instruments')}}"
                }
        ) 

    [get_midas_aware_param_config_task, get_midas_aware_instruments_task] >> flashflood_authenticate_task >> flashflood_get_customer >> get_aware_devices_task >> get_aware_devices_metadata_task >> sync_devices_to_midas_task