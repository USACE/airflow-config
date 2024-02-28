## Database

https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html

- since SqlAlchemy does not expose a way to target a specific schema in the database URI, you need to ensure schema `public`` is in your Postgres user’s search_path.

If you run the `airflow_migrate` container manually after everything has been setup and works, it will:

- Nuke the users and roles of the UI users, restoring it back to the default user and roles.
- It nukes all db metadata

The `airflow_ui` container will likely need to be restarted after running `airflow_migrate`.
