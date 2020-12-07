from collections import abc
from datetime import datetime
from itertools import chain
from typing import Tuple, Optional

from psycopg2 import sql

from parsers import WriteParser


class WriteAggregator:
    @staticmethod
    def assemble(tokens: dict) -> Tuple[sql.Composable, Optional[abc.Sequence]]:
        table_name = tokens['measurement']
        
        columns = map(sql.Identifier, chain(tokens['tags'].keys(), tokens['fields'].keys(), ('time',)))
        values = map(sql.Literal, chain(tokens['tags'].values(), tokens['fields'].values()))
        
        if (utc_timestamp := tokens.get('time')) is not None:
            time = sql.Literal(datetime.utcfromtimestamp(utc_timestamp))
        else:
            time = sql.SQL('now()')
        
        values = chain(values, (time,))
        
        query = sql.SQL('INSERT INTO {table_name}({columns}) VALUES ({values})').format(
            table_name=sql.Identifier(table_name),
            columns=sql.SQL(',').join(columns),
            values=sql.SQL(',').join(values)
        )
        
        return query, None


if __name__ == "__main__":
    payload = 'laptop,host=sever,name=acer memory=10i,freq=100 10'
    wp = WriteParser()
    tokens = wp.parse(payload)[0]
    print(tokens)
    print(WriteAggregator.assemble(tokens))
