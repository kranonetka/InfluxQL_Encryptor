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
    
    @contextmanager
    def cursor(self, use_db=True):  # type: (bool) -> cursor
        """
        Контекстный менеджер для получения курсора. По выходу из контекста все транзакции будут автоматически
        закоммичены, а соединение закрыто
        
        :param use_db: Использовать ли базу данных при подключении
        :return: Курсор для взаимодействия с СУБД
        """
        if use_db:
            conn = psycopg2.connect(**self._credentials)
        else:
            conn = psycopg2.connect(**dict(self._credentials, dbname=None))
        
        with conn, conn.cursor() as cur:
            yield cur
    
    def execute(self, query, params=None, use_db=True):
        """
        Выполнить SQL запрос с параметрами и вернуть **все** строки, если запрос должен что-то вернуть
        
        :param query: Запрос
        :param params: Параметры запроса (опционально)
        :param use_db: Использовать ли базу данных при подключении
        :return: Результат выборки, если запрос должен что-то вернуть
        """
        with self.cursor(use_db=use_db) as cur:
            cur.execute(query, params)
            if cur.description:
                return cur.fetchall()
    
    def __repr__(self):
        return f'{self.__class__.__name__}({", ".join(f"{key}={value}" for key, value in self.__dict__.items())})'
