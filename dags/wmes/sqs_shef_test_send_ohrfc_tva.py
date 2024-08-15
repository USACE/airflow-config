import json
import time

# from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from helpers.sqs import send_sqs_message

# from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
# from airflow.operators.dummy import DummyOperator

from airflow.models import Variable

WMES_SHEF_QUEUE_NAME = Variable.get("WMES_SHEF_QUEUE_NAME")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(hours=1)).replace(
        minute=0, second=0
    ),
    # "start_date": datetime(2022, 7, 1),
    "catchup_by_default": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    default_args=default_args,
    schedule="5 * * * *",
    tags=["wmes", "shef", "sqs"],
    max_active_runs=1,
    max_active_tasks=1,
)
def sqs_shef_test_send_ohrfc_tva():
    """This pipeline handles ... \n"""

    @task()
    def send_shef_messages():

        ohrfc_message = {
            "name": "wmes-shef",
            "product": {"slug": "ohrfc-lrl-qpf-res", "feedtype": "EXP", "filename": ""},
            "key": "products/ohrfc-lrl-qpf-res/ohrfc_LRL_operQPF_RES_2024081506_1723705159",
            "datetime": "2024-08-15T06:59:19.543649Z",
            "metadata": {
                "filename": "ohrfc_LRL_operQPF_RES_2024081506",
                "size": 11250,
                "content_type": "text/plain; charset=utf-8",
                "ext": "",
            },
            "callback_url": "http://internal-ldm-api-alb-internal-1705209920.us-east-1.elb.amazonaws.com/api/ldm/products/ohrfc-lrl-qpf-res/ohrfc_LRL_operQPF_RES_2024081506_1723705159",
        }
        tva_message = {
            "name": "wmes-shef",
            "product": {"slug": "tva-forecast-shef", "feedtype": "EXP", "filename": ""},
            "key": "products/tva-forecast-shef/MEMRR7MRX_PROD_1723705930.shef",
            "datetime": "2024-08-15T07:12:10.954621Z",
            "metadata": {
                "filename": "MEMRR7MRX_PROD.shef",
                "size": 70859,
                "content_type": "text/plain; charset=utf-8",
                "ext": ".shef",
            },
            "callback_url": "http://internal-ldm-api-alb-internal-1705209920.us-east-1.elb.amazonaws.com/api/ldm/products/tva-forecast-shef/MEMRR7MRX_PROD_1723705930.shef",
        }

        send_sqs_message(WMES_SHEF_QUEUE_NAME, json.dumps(ohrfc_message))
        time.sleep(1)
        send_sqs_message(WMES_SHEF_QUEUE_NAME, json.dumps(tva_message))
        time.sleep(1)

    send_shef_messages()


sqs_shef_test_send_ohrfc_tva()
