
import json
import requests
import logging
import pprint

# The DAG object; we'll need this to instantiate a DAG
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
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'midas_sync_aware_ts',
    default_args=default_args,
    description='AWARE Timeseries to MIDAS',
    start_date=(datetime.utcnow()-timedelta(hours=6)).replace(minute=0, second=0),
    # start_date=datetime(2021, 3, 27),
    tags=['midas'],    
    # schedule_interval='*/15 * * * *'
    schedule_interval='@hourly',
    catchup=True
    
) as dag:
    dag.doc_md = __doc__
    ##############################################################################
    def ff_fetch(instrument, token, start, end):
        """ Fetch single instrument data from FlashFlood API
        
        Arguments: 
            instrument {dictionary} - instrument dictionary
            token {string} - token object as string
            start {string} - miliseconds since epoch
            end {string}   - miliseconds since epoch
        """

        logging.info(f"Fetching AWARE device id: {instrument['aware_id']}")
        logging.info(f"MIDAS instrument id: {instrument['instrument_id']}")

        # Convert token object string to string
        token = json.loads(token)['token']
        logging.debug(f'token: {token}')
        
        aware_keys = []
        # aware_keys.append('pict')

        for aware_param, midas_ts_id in instrument['aware_parameters'].items():
            if midas_ts_id is not None:
                aware_keys.append(aware_param)

        # logging.info(f'MIDAS instrument timeseries count: {len(timeseries)}')        
        keys = ','.join(aware_keys) 

        logging.info('*****************')
        logging.info(f"Start: {datetime.fromtimestamp(int(start)).strftime('%Y-%m-%d %H:%M:%S.%f')}")
        logging.info(f"End: {datetime.fromtimestamp(int(end)).strftime('%Y-%m-%d %H:%M:%S.%f')}")
        logging.info('*****************')

        startTs = int(start)*1000
        endTs   = int(end)*1000
        limit   = 24

        if len(keys) > 0:
            # Query AWARE API for the timeseries)
            r = aware.get_device_ts_data(token, instrument['aware_id'], startTs, endTs, keys, limit)
            return json.dumps(r.json()) 
        else:
            logging.info('Instrument has not timeseries enabled')
            return json.dumps({})
    ##############################################################################
    def write_to_midas(instrument, aware_data):
        """ Write timeseries data from FlashFlood API to MIDAS API (single instrument)
        
        Arguments: 
            instrument {dictionary} - instrument dictionary
            aware_data {string} - aware_data object as string
        """

        # Convert string to dict
        aware_response = json.loads(aware_data)

        # Return from function if aware data not present
        if len(aware_response) == 0:
            return

        logging.info(f'instrument: {instrument}')
        logging.debug(f'aware_data: {aware_data}')       

        payload = []

        for aware_param, midas_ts_id in instrument['aware_parameters'].items():
            if midas_ts_id is not None:
                tsv_obj = {}
                tsv_obj['timeseries_id'] = midas_ts_id
                tsv_list = []

                print(f"AWARE values for {aware_param}:")
                # Get the list that cooresponds to the AWARE param
                aware_tsv_list = aware_response[aware_param]
                for tsv in aware_tsv_list:
                    tsv_list.append({"time": aware.epoch_ms_to_human(tsv['ts']), "value": float(tsv['value'])})
                
                tsv_obj['items'] = tsv_list
                payload.append(tsv_obj)       
        

        # pp = pprint.PrettyPrinter(depth=6)
        # pp.pprint(json.dumps(midas_payload))
        print(f'payload: {json.dumps(payload)}')
        
              
        conn = midas.get_connection()
        h = HttpHook(http_conn_id=conn.conn_id, method='POST')    
        # endpoint = f"/projects/{instrument['project_id']}/timeseries_measurements?key_id={conn.login}&key={conn.password}"
        endpoint = f"/timeseries_measurements?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)           

        return
    ##############################################################################    
    get_midas_task = PythonOperator(
        task_id='midas_query',
        python_callable=midas.get_aware_param_config,
        op_kwargs={
                'conn_type': "stable",
            }
    )    

    flashflood_authenticate_task = PythonOperator(
        task_id='flashflood_authenticate',
        python_callable=aware.flashfloodinfo_authenticate
    )

    instruments = json.loads(midas.get_aware_param_config(conn_type='stable')) 

    for i in instruments:
        
        print(f"MIDAS Instrument UUID: {i['instrument_id']}")
        
        fetch_task_id = f"fetch_{i['instrument_id']}"
        
        fetch_task = PythonOperator(
            task_id=fetch_task_id, 
            python_callable=ff_fetch, 
            op_kwargs={
                'instrument': i, 
                'token':"{{task_instance.xcom_pull(task_ids='flashflood_authenticate')}}",
                'start': "{{ (execution_date - macros.timedelta(hours=1)).int_timestamp }}",
                'end': "{{ (execution_date + macros.timedelta(hours=1)).int_timestamp }}",
            }
        )

        write_midas_task = PythonOperator(
            task_id=f"write_midas_{i['instrument_id']}",            
            python_callable=write_to_midas,
            op_kwargs={
                'instrument': i, 
                'aware_data': "{{{{task_instance.xcom_pull(task_ids='{}')}}}}".format(fetch_task_id)
                }
        )

        flashflood_authenticate_task >> fetch_task >> write_midas_task
           

    get_midas_task >> flashflood_authenticate_task