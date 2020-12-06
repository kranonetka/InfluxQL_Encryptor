from collections import abc
from datetime import timedelta
from typing import Tuple, Union

from parsers import Action


class QueryAggregator:
    def __init__(self, phe_n: int):
        self._phe_n = phe_n
    
    def assemble(self, tokens: dict) -> Tuple[str, Union[abc.Sequence, None]]:
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
    
    def _assemble_select(self, tokens: dict) -> Tuple[str, Union[abc.Sequence, None]]:
        query = ['SELECT']
        params = []
        
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
                selectors.append(f'phe_sum({field_key}, %s), count({field_key})')
                params.append(self._phe_n)
            else:
                selectors.append(f'{aggregation}({field_key})')
        else:
            selectors.append(field_key)
        
        query.append(', '.join(selectors))
        
        query.append(f'FROM {tokens["measurement"]}')
        query.append('WHERE')
        
        where_conditions = []
        for key, operator, value in tokens.get('where_conditions', ()):
            print(f'{key} {operator} {value}')
            where_conditions.append(f'{key} {operator} %s')
            params.append(value)
        
        query.append(' AND '.join(where_conditions))
        
        query.append('GROUP BY 1 ORDER BY 1')
        
        return ' '.join(query), params
    
    def _assemble_show_measurements(self, tokens: dict) -> Tuple[str, Union[abc.Sequence, None]]:
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        if (limit := tokens.get('limit')) is not None:
            query += f' LIMIT {limit}'
        return query, None
    
    def _assemble_show_retention_policies(self, tokens: dict) -> Tuple[str, Union[abc.Sequence, None]]:
        return 'SELECT datname FROM pg_database;', None
    
    def _assemble_show_tag_values(self, tokens: dict) -> Tuple[str, Union[abc.Sequence, None]]:
        return f'SELECT DISTINCT %s, {tokens["tag_key"]} FROM {tokens["measurement"]}', [tokens["tag_key"]]
