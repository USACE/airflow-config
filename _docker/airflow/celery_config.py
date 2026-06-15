import os
from airflow.config_templates.default_celery import DEFAULT_CELERY_CONFIG

# If Using SQS Message Broker, Pre-Define Queues
if os.getenv('AIRFLOW__CELERY__BROKER_URL', "").startswith('sqs://'):
    BROKER_TRANSPORT_OPTIONS = {
        **DEFAULT_CELERY_CONFIG["broker_transport_options"],
        "predefined_queues": {
                "airflow": { "url": os.getenv("CELERY_QUEUE_URL_AIRFLOW", "")}
            }
    }
else:
    BROKER_TRANSPORT_OPTIONS = DEFAULT_CELERY_CONFIG["broker_transport_options"]


# Defined so celery_config.CELERY_CONFIG can be referenced in environment variables
CELERY_CONFIG = {
    **DEFAULT_CELERY_CONFIG,
    "broker_transport_options": BROKER_TRANSPORT_OPTIONS 
}
