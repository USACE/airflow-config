## Database

https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html

- since SqlAlchemy does not expose a way to target a specific schema in the database URI, you need to ensure schema `public`` is in your Postgres user’s search_path.

If you run the `airflow_migrate` container manually after everything has been setup and works, it will:

- Nuke the users and roles of the UI users, restoring it back to the default user and roles.
- It nukes all db metadata

The `airflow_ui` container will likely need to be restarted after running `airflow_migrate`.

## Batch Events scheduled jobs

`dags/wmes/cwms_batch_events_swt_hourly.py` now acts as the Batch Events scheduled-job driver. It runs every minute, reads `/scripts/scheduled` from Batch Events using the Airflow Keycloak service client, and triggers any registry entries due at the current Airflow logical date. The registry supports hourly-at-minute entries and five-field cron expressions. Due scripts are triggered through dynamic task mapping so one office trigger does not block the others due in the same minute.

The schedule, office, runtime, resource profile, script path, roles, environment variables, and allowed secret names live in the Batch Events script registry. Airflow should not create one DAG or AWS Batch job definition per office for this path.

`dags/wmes/cwms_hourly_jobs.py` and `dags/wmes/cwms_daily_jobs.py` are retained as manual compatibility DAGs and no longer have active schedules. They no longer submit AWS Batch jobs directly. Instead, they read scheduled Batch Events registry entries for the configured offices and trigger those scripts through Batch Events, so Airflow does not need office-specific AWS Batch job definitions.

The scheduler authenticates with Batch Events using the configured Keycloak
service account values:

- `BATCH_EVENTS_API_ROOT`
- `BATCH_EVENTS_KEYCLOAK_TOKEN_URL`
- `BATCH_EVENTS_KEYCLOAK_CLIENT_ID`
- `BATCH_EVENTS_KEYCLOAK_CLIENT_SECRET`
- `BATCH_EVENTS_KEYCLOAK_SCOPE`

In AWS these are expected to come from Airflow variables exposed by the Airflow
CDK app as `AIRFLOW_VAR_BATCH_EVENTS_*` secrets. Airflow should not require
direct AWS Batch `SubmitJob` permissions for this registry-driven path.
