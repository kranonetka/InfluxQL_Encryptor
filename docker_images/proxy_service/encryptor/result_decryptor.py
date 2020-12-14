from typing import List

from parsers import Action
from ._encryptor import Encryptor


class ResultDecryptor(Encryptor):
    def decrypt(self, query_result: List[tuple], db: str, tokens: dict):
        action = tokens['action']
        if action == Action.SHOW_TAG_VALUES:
            return self._decrypt_show_tag_values(query_result)
        elif action == Action.SELECT:
            return self._decrypt_select(query_result, db, tokens)
        else:
            raise NotImplementedError(str(action))
    
    def _decrypt_show_tag_values(self, query_result):
        query_result = [
            (tag_key, self.decrypt_string(tag_value)) for tag_key, tag_value in query_result
        ]
        return query_result
    
    def _decrypt_select(self, query_result, db, tokens):
        if tokens.get('aggregation') == 'count':
            return query_result
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
            query_result = [(row[0], row[1] / row[2]) for row in query_result]
        
        return query_result
