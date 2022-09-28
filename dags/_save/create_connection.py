from airflow.hooks.base import BaseHook
from airflow import settings
from airflow.models import Connection


def get_connection():
    conn = Connection(
        conn_id="RADAR",
        conn_type="http",
        host="https://cwms-data.usace.army.mil/cwms-data",
        login="nologin",
        password="nopass",
        port=None,
    )
    # create a connection object
    session = settings.Session()  # get the session

    # Check to see if connection already exists
    conn_name = (
        session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    )
    if str(conn_name) == str(conn.conn_id):
        # return existing connection
        return BaseHook.get_connection("RADAR")

    # otherwise connection doesn't exist, so let's add it
    session.add(conn)
    session.commit()
    return conn
