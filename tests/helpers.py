"""
This module includes utility/helper methods to use in our tests.
"""

from airflow.models import Connection
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2020, 1, 1)

def get_connection(conn_id=None):
    """
    Can be used as patch for hook.get_connection.
    """
    return Connection(
        conn_id=conn_id,
        conn_type='http',
        schema='https',
        host='testhost',)
