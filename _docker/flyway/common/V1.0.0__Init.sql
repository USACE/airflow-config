-- reference --> https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html

-- Database is already created by specifying database_name = 'airflow' in 
-- the RDS infrastructure code

DO $$
BEGIN
  CREATE USER ${APP_USERNAME} WITH ENCRYPTED PASSWORD '${APP_PASSWORD}';
  EXCEPTION WHEN DUPLICATE_OBJECT THEN
  RAISE NOTICE 'not creating user airflow -- it already exists';
END
$$;

GRANT ALL PRIVILEGES ON DATABASE airflow TO ${APP_USERNAME};
-- PostgreSQL 15 requires additional privileges:
-- USE airflow;
GRANT ALL ON SCHEMA public TO ${APP_USERNAME};