from contextlib import contextmanager

import psycopg2
from psycopg2.extensions import cursor


class PostgresConnector:
    def __init__(self, host, port=5432, user='postgres', password='password', db=None):
        self._credentials = dict(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=db
        )

        self._connection_obj = None

    @property
    def _connection(self):
        if self._connection_obj is None or self._connection_obj.closed:
            try:
                self._connection_obj = psycopg2.connect(**self._credentials)
            except psycopg2.OperationalError:
                raise ConnectionRefusedError('Postgres not available')
        return self._connection_obj

    @contextmanager
    def cursor(self):  # type: () -> cursor
        """
        Контекстный менеджер для получения курсора. По выходу из контекста все транзакции будут автоматически
        закоммичены, а соединение закрыто
        
        :return: Курсор для взаимодействия с СУБД
        """
        
        with self._connection, self._connection.cursor() as cur:
            yield cur
    
    def execute(self, query, params=None):
        """
        Выполнить SQL запрос с параметрами и вернуть **все** строки, если запрос должен что-то вернуть
        
        :param query: Запрос
        :param params: Параметры запроса (опционально)
        :return: Результат выборки, если запрос должен что-то вернуть
        """
        with self.cursor() as cur:
            cur.execute(query, params)
            if cur.description:
                return cur.fetchall()
    
    def __repr__(self):
        return f'{self.__class__.__name__}({", ".join(f"{key}={value}" for key, value in self.__dict__.items())})'
