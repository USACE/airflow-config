## Database

https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html

- since SqlAlchemy does not expose a way to target a specific schema in the database URI, you need to ensure schema `public`` is in your Postgres user’s search_path.

If you run the `airflow_migrate` container manually after everything has been setup and works, it will:

- Nuke the users and roles of the UI users, restoring it back to the default user and roles.
- It nukes all db metadata

The `airflow_ui` container will likely need to be restarted after running `airflow_migrate`.

## Batch Events scheduled jobs

`dags/wmes/cwms_batch_events_scheduled_jobs.py` is the Batch Events scheduled-job driver. It runs every minute, reads `/scripts/scheduled` from Batch Events using the Airflow Keycloak service client, and triggers any registry entries due at the current Airflow logical date. When triggering a due script, the helper passes the script office into the token lookup so Airflow can use office-specific scheduler client credentials when configured. The registry supports hourly-at-minute entries and five-field cron expressions. Due scripts are triggered through dynamic task mapping so one office trigger does not block the others due in the same minute.

Airflow's responsibility for this path stops after `POST /jobs` succeeds. It does not poll AWS Batch, wait for job completion, or read job logs; Batch Events owns dispatch, status, office-scoped log lookup, and runtime broker state. This keeps a long-running district job from occupying the Airflow scheduled driver and delaying other office triggers.

The schedule, office, runtime, resource profile, script path, roles, environment variables, and allowed secret names live in the Batch Events script registry. Airflow should not create one DAG or AWS Batch job definition per office for this path.

`dags/wmes/cwms_hourly_jobs.py` and `dags/wmes/cwms_daily_jobs.py` are retained as manual compatibility DAGs and no longer have active schedules. They no longer submit AWS Batch jobs directly. Instead, they read scheduled Batch Events registry entries for the configured offices and trigger those scripts through Batch Events using dynamic task mapping, so one office's job trigger does not block another and Airflow does not need office-specific AWS Batch job definitions. The every-minute scheduled driver is the production cron-like path for registry schedules.

The scheduler authenticates with Batch Events using the configured Keycloak
service account values:

- `BATCH_EVENTS_API_ROOT`
- `BATCH_EVENTS_KEYCLOAK_TOKEN_URL`
- `BATCH_EVENTS_KEYCLOAK_CLIENT_ID`
- `BATCH_EVENTS_KEYCLOAK_CLIENT_SECRET`
- optional office-suffixed overrides such as
  `BATCH_EVENTS_KEYCLOAK_CLIENT_ID_SWT` and
  `BATCH_EVENTS_KEYCLOAK_CLIENT_SECRET_SWT`
- optional `BATCH_EVENTS_KEYCLOAK_OFFICE_CLIENTS` JSON map, such as
  `{"SWT":{"clientId":"cwms-batch-airflow-swt","clientSecret":"..."}}`
- optional `BATCH_EVENTS_SCHEDULED_OFFICES` comma-separated list. When set,
  Airflow lists scheduled registry rows once per office using that office's
  scheduler client; when blank, it uses the default scheduler client once.
- `BATCH_EVENTS_KEYCLOAK_SCOPE`
- `BATCH_EVENTS_KEYCLOAK_TOKEN_HOST_HEADER` when an environment must reach the
  Keycloak token endpoint through one host while preserving the public issuer
  host header

In AWS these are expected to come from Airflow variables exposed by the Airflow
CDK app as `AIRFLOW_VAR_BATCH_EVENTS_*` secrets. Airflow should not require
direct AWS Batch `SubmitJob` permissions for this registry-driven path.
