import os

from airflow.providers.amazon.aws.hooks.sqs import SQSHook


SQS_BASE_URL = os.getenv("SQS_BASE_URL", default='http://elasticmq:9324/queue')

def trigger_sqs(*args, **kwargs):

    queue_name = kwargs['queue_name']
    message = kwargs['message']

    print(f'SEND MESSAGE; queue {SQS_BASE_URL}/{queue_name}; message: {message}')
    SQS = SQSHook(aws_conn_id='SQS')
    SQS.send_message(f'{SQS_BASE_URL}/{queue_name}', message)
    
    return
