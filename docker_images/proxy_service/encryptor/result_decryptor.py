from operator import itemgetter, truediv
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
    
    def _decrypt_select(self, query_result: List[tuple], db, tokens):
        
        if tokens.get('aggregation') == 'count':
            return query_result
        
        field_description = self._columns.get(db, {}).get(tokens['measurement'], {}).get(tokens['field_key'])
        field_type, supported_operation = field_description['type'], field_description['operations'][0]
        
        if supported_operation == '>':
            decrypt = self.ope_decrypt
        else:  # +
            decrypt = self.phe_decrypt
        
        final_iterator = map(decrypt, map(int, map(itemgetter(1), query_result)))
        
        if field_type == 'float':
            final_iterator = map(self.int_to_float, final_iterator)
        
        if tokens.get('aggregation') == 'mean':
            third_column = map(itemgetter(2), query_result)
            final_iterator = map(truediv, final_iterator, third_column)
        
        time_column = map(itemgetter(0), query_result)
        
        return list(zip(time_column, final_iterator))
