import os
import socket

def get_host_ip():
    host_name = socket.gethostname()
    return socket.gethostbyname(host_name)

SERVICE_DISOVERY_NAME = os.environ.get('SERVICE_DISOVERY_NAME', default=get_host_ip())