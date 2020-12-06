from copy import deepcopy

from .encryptor import Encryptor


class QueryEncryptor(Encryptor):
    def encrypt(self, tokens: dict) -> dict:
        encrypted_tokens = deepcopy(tokens)
        if where_conditions := encrypted_tokens.get('where_conditions'):
            for i, condition in enumerate(where_conditions):
                lhs, operator, rhs = condition
                if lhs != 'time':
                    rhs = self.encrypt_string(rhs)
                    where_conditions[i] = (lhs, operator, rhs)
        return encrypted_tokens
