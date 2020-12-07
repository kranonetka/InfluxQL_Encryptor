from collections import abc
from datetime import timedelta
from typing import Tuple, Union, Optional

from psycopg2 import sql

from parsers import Action


class QueryAggregator:
    def __init__(self, phe_n: int):
        self._phe_n = phe_n
    
    def assemble(self, tokens: dict) -> Tuple[sql.Composable, Optional[abc.Sequence]]:
        action = tokens.get('action')
        if action == Action.SELECT:
            assembler = self._assemble_select
        
        elif action == Action.SHOW_RETENTION_POLICIES:
            assembler = self._assemble_show_retention_policies
        
        elif action == Action.SHOW_MEASUREMENTS:
            assembler = self._assemble_show_measurements
        
        elif action == Action.SHOW_TAG_VALUES:
            assembler = self._assemble_show_tag_values
        
        else:
            raise NotImplementedError(str(action))
        
        return assembler(tokens)
    
    def _assemble_select(self, tokens: dict) -> Tuple[sql.Composable, Optional[abc.Sequence]]:
        query = [sql.SQL('SELECT')]
        
        selectors = []
        group_by = tokens.get('group_by')
        if isinstance(group_by, timedelta):
            duration = group_by.total_seconds()
            if int(duration) == duration:
                duration = int(duration)
            selectors.append(sql.SQL('time_bucket({duration}, {time})').format(
                duration=sql.Literal(f'{duration}s'),
                time=sql.Identifier('time')
            ))
        else:
            raise NotImplementedError('Grouping by time intervals only')  # TODO
        
        field_key = tokens['field_key']
        if (aggregation := tokens.get('aggregation')) is not None:
            if aggregation in ('mean', 'sum'):
                selectors.append(sql.SQL('phe_sum({field_key}, {modulus})').format(
                    field_key=sql.Identifier(field_key),
                    modulus=sql.Literal(self._phe_n))
                )
                if aggregation == 'mean':
                    selectors.append(sql.SQL('count({field_key})').format(
                        field_key=sql.Identifier(field_key))
                    )
            else:
                selectors.append(sql.SQL('{aggregation}({field_key})').format(
                    aggregation=sql.SQL(aggregation),
                    field_key=sql.Identifier(field_key))
                )
        else:
            selectors.append(sql.Identifier(field_key))
        
        query.append(sql.SQL(',').join(selectors))
        
        query.append(sql.SQL('FROM {table_name}').format(
            table_name=sql.Identifier(tokens['measurement'])
        ))
        
        where_conditions = []
        for key, operator, value in tokens.get('where_conditions', ()):
            where_conditions.append(sql.SQL('{key} {operator} {value}').format(
                key=sql.Identifier(key),
                operator=sql.SQL(operator),
                value=sql.Literal(value)
            ))
        
        if where_conditions:
            query.append(sql.SQL('WHERE ') + sql.SQL(' AND ').join(where_conditions))
        
        query.append(sql.SQL('GROUP BY 1 ORDER BY 1'))
        
        return sql.SQL(' ').join(query), None
    
    def _assemble_show_measurements(self, tokens: dict) -> Tuple[sql.Composable, Optional[abc.Sequence]]:
        query = sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        
        if (limit := tokens.get('limit')) is not None:
            query += sql.SQL(' LIMIT {limit}').format(
                limit=sql.Literal(limit)
            )
        
        return query, None
    
    def _assemble_show_retention_policies(self, tokens: dict) -> Tuple[sql.Composable, Optional[abc.Sequence]]:
        return sql.SQL('SELECT datname FROM pg_database;'), None
    
    def _assemble_show_tag_values(self, tokens: dict) -> Tuple[sql.Composable, Union[abc.Sequence, None]]:
        return sql.SQL('SELECT DISTINCT {col_name_literal}, {col_name} FROM {table_name}').format(
            col_name_literal=sql.Literal(tokens['tag_key']),
            col_name=sql.Identifier(tokens['tag_key']),
            table_name=sql.Identifier(tokens['measurement'])
        ), None
