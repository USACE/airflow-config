# Batch Events registry schedules

The `cwms_batch_events_scheduled_jobs` DAG reads Batch Events schedules every minute and posts due script IDs to `/jobs`. Each due script is mapped to a separate task that completes after submission. Batch Events handles job execution, status, and logs.

The DAG starts paused. Configure its API connection and credentials before unpausing it.

Configure these Airflow variables through the secret backend or Airflow's managed variable configuration:

| Variable | Purpose |
| --- | --- |
| `BATCH_EVENTS_API_ROOT` | Batch Events API URL, including its `/api` prefix where deployed |
| `BATCH_EVENTS_API_KEY` | Dedicated CDA account API key accepted by Batch Events |
| `BATCH_EVENTS_SCHEDULED_OFFICES` | Optional comma-separated list of offices to schedule |
| `BATCH_EVENTS_API_KEY_<OFFICE>` | Optional office-specific key, taking precedence over the default |

Environment-backed variables use the `AIRFLOW_VAR_` prefix. The account needs `CWMS Users` and the roles required by each registered job in the target office. Batch Events uses the account's CDA profile to authorize schedule access and job submission. Scheduler keys are never forwarded to the job container.

For each migrated job, remove its old hourly/daily trigger, enable the registry schedule, and unpause the registry DAG. An explicit office list is filtered even when a fallback key can access additional offices.

Schedules use five numeric cron fields (lists, ranges, and steps on `*` or ranges), or an hourly minute, evaluated in each row's IANA timezone. Missing spring times are skipped; repeated fall times run only at their first occurrence. Evaluation uses Airflow's `data_interval_end`, the due minute of the cron tick, rather than its one-minute-earlier logical date.

Catchup and automatic task retries are disabled. If a submission response is lost or unclear, check Batch Events before manually rerunning the task: resubmitting can create a duplicate job.
