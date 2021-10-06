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

'''
Note, from documentation:

GPS-related values update either when the unit first powers on or when 
the user requests a GPS update. If GPS is requested by the user then the
Lat/Lon/Elev key/values are updated at 1800 UTC.

It’s important to note that even after a command is sent, the unit will not 
execute that command until the next reporting cycle. For example, if a 60s sampling
mode change command is queued at 12:05 p.m. and the unit reports in at 12:10 p.m., 
then the command is sent at 12:10 p.m. You will not see that parameter change until 
it reports in again at 12:20 p.m. Requesting an image and rebooting the unit will 
result in the same outcome. An image request command will start transmitting to the
server, or the unit will reboot and start reporting back in after a few minutes.
'''



# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
with DAG(
    'aware_send_gps_cmd',
    default_args=default_args,
    description='Send GPS Sync Command to AWARE Devices via API',
    start_date=(datetime.utcnow()-timedelta(days=1)).replace(hour=16, minute=0, second=0),
    tags=['midas'],
    # schedule_interval='@daily',
    schedule_interval='0 17 * * *'
    
) as dag:
    # dag.doc_md = __doc__
    ##############################################
    def send_commands(token, metadata):

        devices = json.loads(metadata)

        for d_id, d_obj in devices.items():
            print('#'*30)
            if d_obj.keys():
                # print(d_id, '->', d_obj)
                # for k,v in d_obj.items():
                    # print(f"{k} -> {v['value']} @ {aware.epoch_ms_to_human(v['ts'])}")

                if 'IMEI' in d_obj.keys():                
                    params = {
                            "destinationIMEI": [d_obj['IMEI']['value']],                        
                            "command" : ["G:i"],                        
                            "node" : [d_obj['name']],                        
                            }
                    try:
                        # during testing setup the server was responding with 
                        # a 408 server timeout no matter how long I set the timeout.
                        # The command will still go through, but this pass is needed
                        # to keep things going.
                        aware.send_device_command(token, d_id, params)
                    except:
                        pass
                else:
                    print(f'WARNING:No IMEI value available for: {d_id}')

            else:
                print(f'WARNING:No Metadata available for: {d_id}')

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

    get_aware_devices_metadata_task = PythonOperator(
        task_id='get_aware_device_metadata',
        python_callable=aware.get_aware_device_metadata,
        op_kwargs={                
            'token': "{{task_instance.xcom_pull(task_ids='flashflood_authenticate')}}",
            'devices': "{{task_instance.xcom_pull(task_ids='get_aware_devices')}}",
            'start': "{{ (execution_date - macros.timedelta(hours=72)).int_timestamp }}",
            'end': "{{ (execution_date + macros.timedelta(hours=48)).int_timestamp }}",
            }
    )

    flashflood_send_commands_task = PythonOperator(
        task_id='flashflood_send_commands',
        python_callable=send_commands,
        op_kwargs={                
            'token': "{{task_instance.xcom_pull(task_ids='flashflood_authenticate')}}",
            'metadata': "{{task_instance.xcom_pull(task_ids='get_aware_device_metadata')}}",
            }
    ) 

    flashflood_authenticate_task >> flashflood_get_customer >> get_aware_devices_task >> get_aware_devices_metadata_task >> flashflood_send_commands_task