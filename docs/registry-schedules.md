# Batch Events registry schedules

The `cwms_batch_events_scheduled_jobs` DAG reads Batch Events schedules every minute and posts due script IDs to `/jobs`. Each due script is mapped to a separate task that completes after submission. Batch Events handles job execution, status, and logs.

The DAG starts paused. Configure its API connection and credentials before unpausing it.

Configure these Airflow variables through the secret backend or Airflow's managed variable configuration:

| Variable | Purpose |
| --- | --- |
| `BATCH_EVENTS_API_ROOT` | Batch Events API URL, including its `/api` prefix where deployed |
| `API_KEY` | Existing shared Airflow CDA API key; used when no Batch Events override is configured |
| `BATCH_EVENTS_API_KEY` | Optional override of the shared `API_KEY` for Batch Events |
| `BATCH_EVENTS_SCHEDULED_OFFICES` | Optional comma-separated list of offices to schedule |
| `BATCH_EVENTS_API_KEY_<OFFICE>` | Optional office-specific key, taking precedence over the default |

Environment-backed variables use the `AIRFLOW_VAR_` prefix. The account needs `CWMS Users` and the roles required by each registered job in the target office. Batch Events uses the account's CDA profile to authorize schedule access and job submission.

Credential precedence is `BATCH_EVENTS_API_KEY_<OFFICE>`, then `BATCH_EVENTS_API_KEY`, then `API_KEY`. With no overrides, the helper reads `Variable.get("API_KEY")`, including its environment-backed `AIRFLOW_VAR_API_KEY` value. A new dedicated account, key, or alias is not required when the existing key has the required permissions.

The dev WMES Airflow scheduler and worker task definitions were inspected on 2026-09-23: both already inject `AIRFLOW_VAR_API_KEY` from Secrets Manager. They also inject `AIRFLOW_CONN_CDA` and `AIRFLOW_VAR_CDA_URL`, but this helper uses the `API_KEY` variable, not the CDA connection. No infrastructure change is needed to expose that existing key to this DAG in the inspected deployment. This inspection verified secret references only, not credential validity or office/job roles; verify those permissions before unpausing.

Configure `BATCH_EVENTS_API_ROOT` for the Batch Events API; `CDA_URL` is a different service and is not a substitute. Office-specific key variables can remain unset when using the shared key. `BATCH_EVENTS_SCHEDULED_OFFICES` can limit rollout to selected offices without changing the key's permissions. The separate Batch Events schedule API must also be deployed before enabling this DAG.

Airflow uses this key only to read schedules and submit jobs through the Batch Events API. Batch Events launches each job through the office's Batch job definition, whose secret configuration supplies the running container's CDA credentials. Scheduler keys are never forwarded to the job container; the office execution keys do not need to be added to Airflow. These credentials are injected at container startup, not built into the image.

For each migrated job, remove its old hourly/daily trigger, enable the registry schedule, and unpause the registry DAG. An explicit office list is filtered even when a fallback key can access additional offices.

Schedules use five numeric cron fields (lists, ranges, and steps on `*` or ranges), or an hourly minute, evaluated in each row's IANA timezone. Missing spring times are skipped; repeated fall times run only at their first occurrence. Evaluation uses Airflow's `data_interval_end`, the due minute of the cron tick, rather than its one-minute-earlier logical date.

Catchup and automatic task retries are disabled. If a submission response is lost or unclear, check Batch Events before manually rerunning the task: resubmitting can create a duplicate job.
