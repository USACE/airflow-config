## Database

https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html

- since SqlAlchemy does not expose a way to target a specific schema in the database URI, you need to ensure schema `public`` is in your Postgres user’s search_path.

If you run the `airflow_migrate` container manually after everything has been setup and works, it will:

- Nuke the users and roles of the UI users, restoring it back to the default user and roles.
- It nukes all db metadata

The `airflow_ui` container will likely need to be restarted after running `airflow_migrate`.

## Batch Events scheduled jobs

`dags/wmes/cwms_batch_events_swt_hourly.py` now acts as the Batch Events scheduled-job driver. It runs every minute, reads `/scripts/scheduled` from Batch Events using the Airflow Keycloak service client, and triggers any hourly registry entries whose `scheduleMinute` matches the current Airflow logical date minute. Due scripts are triggered through dynamic task mapping so one office trigger does not block the others due in the same minute.

The schedule, office, runtime, resource profile, script path, roles, environment variables, and allowed secret names live in the Batch Events script registry. Airflow should not create one DAG or AWS Batch job definition per office for this path.

`dags/wmes/cwms_hourly_jobs.py` is retained as a legacy/manual DAG and no longer has an active schedule. It still shows the older direct AWS Batch pattern that submits `cwms-{office}-jobs-jobdef`.
