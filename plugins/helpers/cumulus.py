import json
from airflow.hooks.base_hook import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable

# from airflow import AirflowException
import requests

S3_ACQUIRABLE_PREFIX = "cumulus/acquirables"
S3_BUCKET = Variable.get("S3_BUCKET")

acquirables = {
    "ncrfc-rtmat-01h": "6c879d18-2eca-4b35-9fab-2b5f78262fa6",
    "ncrfc-fmat-01h": "28d16afe-2834-4d2c-9df2-fdf2c40e510f",
    "ncep-mrms-v12-msqpe01h-p2-carib": "a483aa42-4388-4289-a41e-6b78998066a7",
    "ncep-mrms-v12-msqpe01h-p1-carib": "e5dfeef2-f070-49dc-8f3c-1c9230000f96",
    "ncep-mrms-v12-msqpe01h-p2-alaska": "1860dfa9-0d2c-4b75-84ed-516792d940ee",
    "ncep-mrms-v12-msqpe01h-p1-alaska": "cf75d07d-d527-4be0-b066-0bfa86565ab5",
    "marfc-rtmat-01h": "5fc5d74a-6684-4ffb-886a-663848ba22d9",
    "marfc-nbmt-03h": "e2228d8c-204a-4c7e-849b-a9a7e5c13eca",
    "marfc-nbmt-01h": "af651a3b-03ad-424d-8cf7-9ca7230309ed",
    "marfc-fmat-06h": "7093dd22-2fa4-4172-b67d-5abc586e5eb6",
    "cbrfc-mpe": "2429db9a-9872-488a-b7e3-de37afc52ca4",
    "hrrr-total-precip": "d4e67bee-2320-4281-b6ef-a040cdeafeb8",
    "nbm-co-01h": "d4aa1d8d-ce06-47a0-9768-e817b43a20dd",
    "nohrsc-snodas-unmasked": "87819ceb-72ee-496d-87db-70eb302302dc",
    "ncep-rtma-ru-anl-airtemp": "22678c3d-8ac0-4060-b750-6d27a91d0fb3",
    "ncep-mrms-v12-multisensor-qpe-01h-pass1": "87a8efb7-af6f-4ece-a97f-53272d1a151d",
    "ncep-mrms-v12-multisensor-qpe-01h-pass2": "ccc252f9-defc-4b25-817b-2e14c87073a0",
    "ndfd-conus-qpf-06h": "f2fee5df-c51f-4774-bd41-8ded1eed6a64",
    "ndfd-conus-airtemp": "5c0f1cfa-bcf8-4587-9513-88cb197ec863",
    "ndgd-ltia98-airtemp": "b27a8724-d34d-4045-aa87-c6d88f9858d0",
    "ndgd-leia98-precip": "4d5eb062-5726-4822-9962-f531d9c6caef",
    "prism-ppt-early": "099916d1-83af-48ed-85d7-6688ae96023d",
    "prism-tmax-early": "97064e4d-453b-4761-8c9a-4a1b979d359e",
    "prism-tmin-early": "11e87d14-ec54-4550-bd95-bc6eba0eba08",
    "prism-ppt-stable": "c1b5f8a5-f357-4c1a-9ec1-854db35c71d9",
    "prism-tmax-stable": "3952d221-502f-4937-b860-db8d4b3df435",
    "prism-tmin-stable": "8a20fb67-7c47-46be-b61d-73be8584300f",
    "ncep-stage4-mosaic-01h": "29b1e90b-3f8c-484f-a7fa-7055aec4d5b8",
    "ncep-stage4-mosaic-06h": "1011b702-9cb7-4b86-9638-ccbf2c19086f",
    "ncep-stage4-mosaic-24h": "758958c4-0938-428e-8221-621bd07e9a34",
    "wrf-columbia-precip": "ec926de8-6872-4d2b-b7ce-6002221babcd",
    "wrf-columbia-airtemp": "552bf762-449f-4983-bbdc-9d89daada260",
    "wpc-qpf-2p5km": "0c725458-deb7-45bb-84c6-e98083874c0e",
    "nsidc-ua-swe-sd-v1": "4b0f8d9c-1be4-4605-8265-a076aa6aa555",
    "serfc-qpf-06h": "355d8d9b-1eb4-4f1d-93b7-d77054c5c267",
    "serfc-qpe-01h": "5365399a-7aa6-4df8-a91a-369ca87c8bd9",
    "lmrfc-qpf-06h": "fca9e8a4-23e3-471f-a56b-39956055a442",
    "lmrfc-qpe-01h": "660ce26c-9b70-464b-8a17-5c923752545d",
    "nohrsc-snodas-assimilated": "21a331c1-4694-41d2-8cdf-7d44f38be66d",
}


def get_connection():
    return BaseHook.get_connection("CUMULUS")


def notify_acquirablefile(acquirable_id, datetime, s3_key):

    payload = {"datetime": datetime, "file": s3_key, "acquirable_id": acquirable_id}
    print(f"Sending payload: {payload}")

    conn = get_connection()

    h = HttpHook(http_conn_id=conn.conn_id, method="POST")
    endpoint = f"/acquirablefiles?key={conn.password}"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, json=payload, headers=headers)

    return json.dumps(r.json())
