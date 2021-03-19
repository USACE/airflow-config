import requests

API_ENDPOINT = 'http://localhost:8000/api/v1'

dag_id = 'cumulus_download_and_process_snodas'
dag_run_id = 'scheduled__2014-01-01T01:00:00+00:00'
task_id = 'snodas_process_cogs'

task_update_endpoint = f'{API_ENDPOINT}/dags/{dag_id}/updateTaskInstancesState'
headers={"Content-Type": "application/json"}
payload = {
  "dry_run": False,
  "execution_date": "2014-01-01T01:00:00+00:00",
  "include_downstream": False,
  "include_future": False,
  "include_past": False,
  "include_upstream": False,
  "new_state": "failed",
  "task_id": task_id
}
r = requests.post(task_update_endpoint, json=payload, auth=('geoprocess_user', 'airflow'))
print(r.text)