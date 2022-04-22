# Airflow Configuration and DAGS

start up using `docker compose up`

Configuration is set to log to both locally and to minio (to mock logging to S3).

To start minio using `docker compose -f docker-compose.minio.yml up` or `./minio.sh up`

## Connection configuration
- config found in `airflow.env`

## Notes:
- You may need to adjust the network or remove it completely.

### Airflow Web UI: http://localhost:8000 
- user: admin
- pw: admin