from encryptor import Encryptor


class WriteTokensEncryptor(Encryptor):
    
    def encrypt(self, tokens, db):
        encrypted = dict()
        encrypted["measurement"] = tokens["measurement"]
        encrypted["tags"] = {}
        encrypted["fields"] = {}
        
        for tag_key, tag_value in tokens.get("tags", {}).items():
            encrypted_tag_value = self.encrypt_string(tag_value)
            encrypted["tags"][tag_key] = encrypted_tag_value
        
        for field_key, field_value in tokens.get("fields", {}).items():
            type_of_value = self._types.get(db, {}).get(tokens.get("measurement")).get(field_key).get("type")
            
            if type_of_value == "string":
                encrypted_field_value = self.encrypt_string(field_value)
            
            elif type_of_value in ('int', 'float'):
                if type_of_value == 'float':
                    field_value = self.float_to_int(field_value)
                
                operations = self._types.get(db, {}).get(tokens.get("measurement")).get(field_key).get("operations")
                
                if operations[0] == '>':
                    encrypted_field_value = self.ope_encrypt(field_value)
                elif operations[0] == '+':
                    encrypted_field_value = self.phe_encrypt(field_value)
                else:
                    raise TypeError(f'Unsupported operation {operations[0]}')
            else:
                raise TypeError(f'Unsupported type {type_of_value}')
            
            encrypted["fields"][field_key] = encrypted_field_value
        
        return encrypted
