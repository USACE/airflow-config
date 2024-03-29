#!/bin/bash

# Build Image
docker build -t airflow-config-tests:latest -f tests-Dockerfile .

docker run --rm --env-file tests/airflow.env airflow-config-tests:latest
