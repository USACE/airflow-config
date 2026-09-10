# Batch Events registry schedules

The new `cwms_batch_events_scheduled_jobs` DAG reads Batch Events schedules every minute and posts due script IDs to `/jobs`. It does not wait for job completion; Batch Events retains dispatch, status, and logs. Each due script is mapped to a separate task.

The DAG starts paused and does not replace the existing hourly/daily DAGs. Deploy the supporting Batch Events schedule API before unpausing it.

Configure these Airflow variables through the existing secret backend or Airflow's managed variable configuration:

| Variable | Purpose |
| --- | --- |
| `BATCH_EVENTS_API_ROOT` | Batch Events API URL, including its `/api` prefix where deployed |
| `BATCH_EVENTS_API_KEY` | Dedicated CDA account API key accepted by Batch Events |
| `BATCH_EVENTS_SCHEDULED_OFFICES` | Optional comma-separated office rollout list |
| `BATCH_EVENTS_API_KEY_<OFFICE>` | Optional office-specific key, taking precedence over the default |

Environment-backed variables use the `AIRFLOW_VAR_` prefix. The account needs `CWMS Users` and the roles required by each registered job in the target office. It does not need script-admin privileges merely to read schedules and submit jobs. The existing Batch Events CDA profile lookup supplies authorization. No new Keycloak account/client is needed; Keycloak client credentials remain a later M2M change. Scheduler keys are never forwarded to the job container.

For each migrated job, remove its old hourly/daily trigger, enable the registry schedule, and unpause the registry DAG. An explicit office list is filtered even when a fallback key can access additional offices.

Schedules use five numeric cron fields (lists, ranges, and steps on `*` or ranges), or an hourly minute, evaluated in each row's IANA timezone. Missing spring times are skipped; repeated fall times run only at their first occurrence. Evaluation uses Airflow's `data_interval_end`, the due minute of the cron tick, rather than its one-minute-earlier logical date.

Catchup and automatic task retries are disabled. `/jobs` does not yet have an idempotency key, so an ambiguous POST response must be checked in Batch Events before manually rerunning the task. This does not guarantee exactly-once execution after operator retries or outages.

Runtime selection and new images are not prerequisites for scheduling existing Python jobs. No Airflow infrastructure code change is required if these variables can be supplied through the existing configuration mechanism.
