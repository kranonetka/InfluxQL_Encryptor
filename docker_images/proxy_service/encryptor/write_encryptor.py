from paillier.crypto import encrypt as phe_encrypt

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
        
        for field_key in tokens.get("fields").keys():
            field_value = tokens.get("fields").get(field_key)
            type_of_value = self.types.get(db, {}).get(tokens.get("measurement")).get(field_key).get("type")
            operations = self.types.get(db, {}).get(tokens.get("measurement")).get(field_key).get("operations")
            if type_of_value == "string":
                encrypted_field_value = self.encrypt_string(field_value)
                encrypted["fields"][field_key] = encrypted_field_value
            elif type_of_value == "int":
                if operations[0] == ">":
                    encrypted_field_value = self._ope_cipher.encrypt(field_value)
                    encrypted["fields"][field_key] = encrypted_field_value
                elif operations[0] == "+":
                    encrypted_field_value = phe_encrypt(self.paillier_pub_key, field_value)
                    encrypted["fields"][field_key] = encrypted_field_value
            elif type_of_value == "float":
                if operations[0] == ">":
                    encrypted_field_value = self._ope_cipher.encrypt(self.float_to_int(field_value))
                    encrypted["fields"][field_key] = encrypted_field_value
                elif operations[0] == "+":
                    encrypted_field_value = phe_encrypt(self.paillier_pub_key, self.float_to_int(field_value))
                    encrypted["fields"][field_key] = encrypted_field_value
        return encrypted
