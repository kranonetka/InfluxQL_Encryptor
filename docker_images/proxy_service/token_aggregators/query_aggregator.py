from datetime import datetime, timedelta
from typing import Tuple, Union

from parsers import Action
from collections import abc


def _datetime_to_iso8601(dt: datetime):
    retstr = dt.strftime('%Y-%m-%dT%H:%M:%S')
    if dt.microsecond:
        retstr += dt.strftime('.%f')[:-3].rstrip('0')
    return retstr + 'Z'


_func_mapping = dict(
    mean='avg'
)


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
            aggregation = _func_mapping.get(aggregation, aggregation)
            selectors.append(f'{aggregation}({field_key})')
        else:
            selectors.append(field_key)
        
        query.append(', '.join(selectors))
        
        query.append(f'FROM {tokens["measurement"]}')
        query.append('WHERE')
        
        where_conditions = []
        time_boundaries = [datetime.utcnow()] * 2
        for key, operator, value in tokens.get('where_conditions', ()):
            if key == 'time':
                index = 0 if operator == '>=' else 1
                time_boundaries[index] = value
            else:
                where_conditions.append(f"{key} {operator} '{value}'")
        
        time_boundaries = map(_datetime_to_iso8601, time_boundaries)
        
        where_conditions.append("\"time\" BETWEEN '{}' AND '{}'".format(*time_boundaries))
        
        query.append(' AND '.join(where_conditions))
        
        query.append('GROUP BY 1 ORDER BY 1')
        
        return ' '.join(query), None
    
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
