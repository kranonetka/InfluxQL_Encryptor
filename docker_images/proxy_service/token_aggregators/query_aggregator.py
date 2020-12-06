from collections import abc
from datetime import timedelta
from typing import Tuple, Union

from parsers import Action


class QueryAggregator:
    @staticmethod
    def assemble(tokens: dict) -> Tuple[str, Union[abc.Sequence, None]]:
        action = tokens.get('action')
        if action == Action.SELECT:
            assembler = QueryAggregator._assemble_select
        
        elif action == Action.SHOW_RETENTION_POLICIES:
            assembler = QueryAggregator._assemble_show_retention_policies
        
        elif action == Action.SHOW_MEASUREMENTS:
            assembler = QueryAggregator._assemble_show_measurements
        
        elif action == Action.SHOW_TAG_VALUES:
            assembler = QueryAggregator._assemble_show_tag_values
        
        else:
            raise NotImplementedError(str(action))
        
        return assembler(tokens)
    
    @staticmethod
    def _assemble_select(tokens: dict) -> Tuple[str, Union[abc.Sequence, None]]:
        query = ['SELECT']
        
        selectors = []
        group_by = tokens.get('group_by')
        if isinstance(group_by, timedelta):
            duration = group_by.total_seconds()
            if int(duration) == duration:
                duration = int(duration)
            selectors.append(f'time_bucket(\'{duration}s\', "time")')
        else:
            raise NotImplementedError('Grouping by time intervals only')  # TODO
        
        field_key = tokens['field_key']
        if (aggregation := tokens.get('aggregation')) is not None:
            if aggregation == 'mean':
                selectors.append(f'phe_sum({field_key}), count({field_key})')
            else:
                selectors.append(f'{aggregation}({field_key})')
        else:
            selectors.append(field_key)
        
        query.append(', '.join(selectors))
        
        query.append(f'FROM {tokens["measurement"]}')
        query.append('WHERE')
        
        params = []
        where_conditions = []
        for key, operator, value in tokens.get('where_conditions', ()):
            print(f'{key} {operator} {value}')
            where_conditions.append(f'{key} {operator} %s')
            params.append(value)
        
        query.append(' AND '.join(where_conditions))
        
        query.append('GROUP BY 1 ORDER BY 1')
        
        return ' '.join(query), params
    
    @staticmethod
    def _assemble_show_measurements(tokens: dict) -> Tuple[str, Union[abc.Sequence, None]]:
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        if (limit := tokens.get('limit')) is not None:
            query += f' LIMIT {limit}'
        return query, None
    
    @staticmethod
    def _assemble_show_retention_policies(tokens: dict) -> Tuple[str, Union[abc.Sequence, None]]:
        return 'SELECT datname FROM pg_database;', None
    
    @staticmethod
    def _assemble_show_tag_values(tokens: dict) -> Tuple[str, Union[abc.Sequence, None]]:
        return f'SELECT DISTINCT %s, {tokens["tag_key"]} FROM {tokens["measurement"]}', [tokens["tag_key"]]
