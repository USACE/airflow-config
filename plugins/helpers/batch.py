import json
import os
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.providers.docker.operators.docker import DockerOperator

from airflow import DAG
from datetime import datetime
from typing import Any, Dict, List
from airflow.models import Variable
import json


def get_office_groups(offices: List[str]) -> Dict[str, List[Dict[str, Any]]]:

    OFFICE_GROUPS = json.loads(Variable.get("OFFICE_GROUPS"))
    job_configs = [
        entry for entry in OFFICE_GROUPS if entry.get("office") in offices]

    # Organize configs by office_group
    groups = {}
    for config in job_configs:
        groups.setdefault(config["office_group"], []).append(config)

    return groups


# Check if running in AWS (check environment variable for AWS_REGION or AWS_DEFAULT_REGION)
def batch_operator(
    dag: DAG, task_id: str, local_command: List[str], **kwargs: Dict[str, Any]
):
    """
    Wrapper function for AWSBatchOperator that will default to using DockerOperator in local mode for mocking/testing.

    :param dag: DAG instance
    :param task_id: Task ID for the operator
    :param local_command: Local use only with Docker
    :param kwargs: Additional arguments for the operator (like job name, queue, job definition, etc.)
    :return: Either a DockerOperator or AWSBatchOperator instance based on environment
    """

    # Check the AWS_DEFAULT_REGION environment variable
    aws_region = os.getenv("AWS_DEFAULT_REGION", "")

    # If the region is "us-east-1", assume local; otherwise, use AWS Batch
    is_local = aws_region == "us-east-1"

    if is_local:  # If running locally, use DockerOperator
        return DockerOperator(
            task_id=task_id,
            image=kwargs.get("local_image", ""),
            command=local_command,
            docker_url="unix://var/run/docker.sock",  # Docker URL for local Docker engine
            network_mode="bridge",  # Local network mode
            mount_tmp_dir=False,
            dag=dag,
        )
    else:  # If running in AWS, use AWSBatchOperator
        now = datetime.now()

        return BatchOperator(
            task_id=task_id,
            # The job name in AWS Batch is a temporary, unique identifier for each individual job run
            job_name=task_id,
            job_definition=kwargs.get(
                "job_definition",
                "arn:aws:batch:REGION:ACCOUNT_ID:job-definition/YOUR_JOB_DEFINITION_NAME",
            ),  # Default ARN
            job_queue=kwargs.get(
                "job_queue",
                "arn:aws:batch:REGION:ACCOUNT_ID:job-queue/YOUR_JOB_QUEUE_NAME",
            ),  # Default ARN
            container_overrides=kwargs.get("container_overrides", {}),
            aws_conn_id=kwargs.get(
                "aws_conn_id", "aws_default"
            ),  # Default AWS connection ID
            # region_name=kwargs.get(
            #     "region_name", os.getenv("AWS_DEFAULT_REGION", "us-east-1")
            # ),  # Default to the environment's AWS region
            dag=dag,
        )
