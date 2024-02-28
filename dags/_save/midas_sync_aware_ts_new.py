# The DAG object; we'll need this to instantiate a DAG
import logging
import json

from airflow import DAG

from airflow import AirflowException
from datetime import datetime, timedelta

# from airflow.hooks.base import BaseHook
# from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import task, get_current_context
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import helpers.aware as aware
import helpers.midas as midas

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=5)}
with DAG(
    "midas_sync_aware_ts_new",
    default_args=default_args,
    description="AWARE Timeseries to MIDAS",
    # start_date=(datetime.utcnow()-timedelta(hours=6)).replace(minute=0, second=0),
    start_date=datetime(2021, 7, 21),
    tags=["midas"],
    schedule="15 * * * *",
    max_active_runs=2,
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    ############################################################################
    def ff_fetch(instrument):

        instrument = json.loads(instrument)
        # logging.info(f"NEW-Fetching AWARE device id: {instrument['aware_id']}")
        # logging.info(f"NEW-MIDAS instrument id: {instrument['instrument_id']}")

        print(f"NEW-Fetching AWARE device id: {instrument['aware_id']}")
        print(f"NEW-MIDAS instrument id: {instrument['instrument_id']}")

    ############################################################################
    def write_to_midas(instrument):
        print(f"Write to instrument: {instrument['instrument_id']}")

    ############################################################################
    def generate_tasks(instruments, ff_auth_task):

        print("##########")
        print(instruments)
        print("##########")

        instruments = json.loads(instruments)
        tasks = []

        # for i in instruments:

        # print(f"MIDAS Instrument UUID: {i['instrument_id']}")

        # fetch_task_id = f"fetch_{i['instrument_id']}"

        # fetch_task = PythonOperator(
        #     task_id=fetch_task_id,
        #     python_callable=ff_fetch,
        #     op_kwargs={
        #         'instrument': i,
        #         # 'token':"{{task_instance.xcom_pull(task_ids='flashflood_authenticate')}}",
        #         # 'start': "{{ (execution_date - macros.timedelta(hours=1)).int_timestamp }}",
        #         # 'end': "{{ (execution_date + macros.timedelta(hours=1)).int_timestamp }}",
        #     }
        # )

        # print('*****')
        # print(ff_auth_task)
        # print('*****')
        # ff_auth_task.set_downstream(fetch_task)
        # tasks.append(fetch_task_id)

    ##############################################################################
    get_midas_configs_task = PythonOperator(
        task_id="midas_query",
        python_callable=midas.get_aware_param_config,
        op_kwargs={
            "conn_type": "develop",
        },
    )

    flashflood_authenticate_task = PythonOperator(
        task_id="flashflood_authenticate",
        python_callable=aware.flashfloodinfo_authenticate,
    )

    # generate_tasks_task = PythonOperator(
    #     task_id='generate_tasks',
    #     python_callable=generate_tasks,
    #     op_kwargs={
    #             'instruments': "{{task_instance.xcom_pull(task_ids='midas_query')}}",
    #             'ff_auth_task': flashflood_authenticate_task
    #         }
    # )

    instruments = json.loads(midas.get_aware_param_config(conn_type="stable"))

    processing_groups = []

    # The // operator will be available to request floor division unambiguously.
    group0 = [*range(0, len(instruments) // 2)]
    group1 = [*range((len(instruments) // 2), len(instruments))]
    processing_groups.append(group0)
    processing_groups.append(group1)

    # for pg_list in processing_groups:

    task_groups = []

    print(f"processing_groups: {processing_groups}")

    for pg_id, pg_list in enumerate(processing_groups):
        tg_id = f"group{pg_id}"
        print(f"top-Task group is: {tg_id}")

        with TaskGroup(group_id=tg_id) as tg:

            # t1 = DummyOperator(task_id='task1')
            print(pg_list)
            for i in pg_list:
                print(f"i is: {i}")

                fetch_task_id = (
                    f"task_{str(i).zfill(3)}_fetch_{instruments[i]['instrument_id']}"
                )
                tg.fetch_task = PythonOperator(
                    task_id=fetch_task_id,
                    python_callable=ff_fetch,
                    op_kwargs={
                        "instrument": instruments[i],
                        "token": "{{task_instance.xcom_pull(task_ids='flashflood_authenticate')}}",
                        "start": "{{ (execution_date - macros.timedelta(hours=1)).int_timestamp }}",
                        "end": "{{ (execution_date + macros.timedelta(hours=1)).int_timestamp }}",
                    },
                )

                tg.write_midas_task = PythonOperator(
                    task_id=f"task_{str(i).zfill(3)}_write_midas_{instruments[i]['instrument_id']}",
                    python_callable=write_to_midas,
                    op_kwargs={
                        "instrument": instruments[i],
                        "aware_data": "{{{{task_instance.xcom_pull(task_ids='{}')}}}}".format(
                            fetch_task_id
                        ),
                    },
                )

                tg.fetch_task >> tg.write_midas_task
            print(f"Task group is: {tg_id}")
            print(f"Appending {tg}")
            task_groups.append(tg)

    (
        get_midas_configs_task
        >> flashflood_authenticate_task
        >> task_groups[0]
        >> task_groups[1]
    )
