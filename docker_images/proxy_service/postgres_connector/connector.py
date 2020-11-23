import psycopg2
from contextlib import contextmanager


class PostgresConnector:
    def __init__(self, host, port=5432, user='postgres', password='password', db=None):
        self._credentials = dict(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=db
        )
        
    @contextmanager
    def _get_cursor(self):
        connection = psycopg2.connect(**self._credentials)
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
            connection.close()
        
    def execute(self, query, params=None):
        with self._get_cursor() as cursor:
            cursor.execute(query, params)
            if cursor.description:
                return cursor.fetchall()
