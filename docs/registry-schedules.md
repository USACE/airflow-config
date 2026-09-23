# Batch Events registry schedules

The `cwms_batch_events_scheduled_jobs` DAG reads Batch Events schedules every minute and posts due script IDs to `/jobs`. Each due script is mapped to a separate task that completes after submission. Batch Events handles job execution, status, and logs.

The DAG starts paused. Configure its API connection and credentials before unpausing it.

Configure these Airflow variables through the secret backend or Airflow's managed variable configuration:

| Variable | Purpose |
| --- | --- |
| `BATCH_EVENTS_API_ROOT` | Batch Events API URL, including its `/api` prefix where deployed |
| `BATCH_EVENTS_API_KEY` | CDA API key accepted by Batch Events; Airflow's existing key can be reused if its account has the required office/job roles |
| `BATCH_EVENTS_SCHEDULED_OFFICES` | Optional comma-separated list of offices to schedule |
| `BATCH_EVENTS_API_KEY_<OFFICE>` | Optional office-specific key, taking precedence over the default |

Environment-backed variables use the `AIRFLOW_VAR_` prefix. The account needs `CWMS Users` and the roles required by each registered job in the target office. Batch Events uses the account's CDA profile to authorize schedule access and job submission.

A new dedicated account or key is not required when Airflow's existing key has those permissions. Expose that existing secret as `BATCH_EVENTS_API_KEY` (or `AIRFLOW_VAR_BATCH_EVENTS_API_KEY` for environment-backed configuration). If it is currently exposed under another name, its configuration must be mapped to the expected name. Office-specific key variables can remain unset when using the shared key. `BATCH_EVENTS_SCHEDULED_OFFICES` can limit rollout to selected offices without changing the key's permissions.

Airflow uses this key only to read schedules and submit jobs through the Batch Events API. Batch Events launches each job through the office's Batch job definition, whose secret configuration supplies the running container's CDA credentials. Scheduler keys are never forwarded to the job container; the office execution keys do not need to be added to Airflow. These credentials are injected at container startup, not built into the image.

For each migrated job, remove its old hourly/daily trigger, enable the registry schedule, and unpause the registry DAG. An explicit office list is filtered even when a fallback key can access additional offices.

Schedules use five numeric cron fields (lists, ranges, and steps on `*` or ranges), or an hourly minute, evaluated in each row's IANA timezone. Missing spring times are skipped; repeated fall times run only at their first occurrence. Evaluation uses Airflow's `data_interval_end`, the due minute of the cron tick, rather than its one-minute-earlier logical date.

Catchup and automatic task retries are disabled. If a submission response is lost or unclear, check Batch Events before manually rerunning the task: resubmitting can create a duplicate job.
