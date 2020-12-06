from typing import List

from ._encryptor import Encryptor


class ResultDecryptor(Encryptor):
    def decrypt(self, query_result: List[tuple], db: str, tokens: dict):
        field = self._types.get(db, {}).get(tokens['measurement'], {}).get(tokens['field_key'])
        field_type, supported_operation = field['type'], field['operations'][0]
        
        if supported_operation == '>':
            decrypt = self.ope_decrypt
        else:  # +
            decrypt = self.phe_decrypt
        
        query_result = [
            [time, decrypt(int(value)), *others]
            for time, value, *others in query_result
        ]
        
        if field_type == 'float':
            query_result = [
                [time, self.int_to_float(value), *others]
                for time, value, *others in query_result
            ]
        
        if tokens.get('aggregation') == 'mean':
            query_result = [(row[0], row[1] / row[2],) for row in query_result]
        
        return query_result
