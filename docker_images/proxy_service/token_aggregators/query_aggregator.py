from datetime import datetime, timedelta


def _datetime_to_iso8601(dt: datetime):
    retstr = dt.strftime('%Y-%m-%dT%H:%M:%S')
    if dt.microsecond:
        retstr += dt.strftime('.%f')[:-3].rstrip('0')
    return retstr + 'Z'


class QueryAggregator:
    @staticmethod
    def assemble(tokens: dict) -> str:
        if tokens.get('action') == 'select':
            return QueryAggregator._assemble_select_query(tokens)
        else:
            raise NotImplementedError()
        # TODO:
        #  * show_retention_policies_stmt
        #  * show_field_keys_stmt
        #  * show_measurements_stmt
    
    @staticmethod
    def _assemble_select_query(tokens: dict) -> str:
        query = ['SELECT']
        
        selectors = []
        group_by = tokens.get('group_by')
        if isinstance(group_by, timedelta):
            duration = group_by.total_seconds()
            if int(duration) == duration:
                duration = int(duration)
            selectors.append(f'time_bucket(\'{duration}s\', "time")')
        
        field_key = tokens['field_key']
        if (aggregation := tokens.get('aggregation')) is not None:
            # TODO: mean -> avg and so on
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
            
        return ' '.join(query)
