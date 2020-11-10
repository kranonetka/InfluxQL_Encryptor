import pickle
from base64 import b64encode
from typing import Any

from parsimonious.nodes import Node

from ._base_encryptor import _BaseEncryptor
from ._grammars import write_grammar


class WriteEncryptor(_BaseEncryptor):
    grammar = write_grammar
    
    def __init__(self, *args, **kwargs):
        super(WriteEncryptor, self).__init__(*args, **kwargs)
        self._types = dict()
    
    @property
    def types(self):
        retval = self._types
        self._types = dict()
        return retval
    
    def _encrypt_object(self, obj: Any) -> str:
        bytes_dump = pickle.dumps(obj)
        encrypted = self._encrypt_bytes(bytes_dump)
        encoded = b64encode(encrypted).rstrip(b'=')
        return encoded.decode()
    
    def visit_field(self, node: Node, visited_children: tuple):
        field_key = node.children[0].text
        field_type = node.children[2].children[0].expr_name
        self._types[field_key] = field_type
        return self.generic_visit(node, visited_children)
    
    def visit_identifier(self, node: Node, visited_children: tuple):
        ident = node.text
        encrypted = self._encrypt_bytes(ident.encode())
        encoded = b64encode(encrypted).rstrip(b'=')
        return encoded.decode()
    
    def visit_int_lit(self, node: Node, visited_children: tuple):
        int_value = int(node.text[:-1])
        encrypted_value = self._ope_cipher.encrypt(int_value)
        return f'{encrypted_value}i'
    
    def visit_float_lit(self, node: Node, visited_children: tuple):
        float_value = float(node.text)
        int_value = self._float_to_int(float_value)
        encrypted_value = self._ope_cipher.encrypt(int_value)
        return f'{encrypted_value}i'
    
    # def visit_bool_lit(self, node: Node, visited_children: list):
    #     if node.text in {'t', 'true', 'T', 'True', 'TRUE'}:
    #         value = True
    #     else:
    #         value = False
    #     return self._get_encrypted_pickle_bytes(value)
    
    def visit_quoted_string(self, node: Node, visited_children: tuple):
        string = node.text[1:-1].encode()
        encrypted_string = self._encrypt_bytes(string)
        encoded = b64encode(encrypted_string).rstrip(b'=')
        return f'"{encoded.decode()}"'

    def visit_unquoted_string(self, node: Node, visited_children: tuple):
        string = node.text.encode()
        encrypted_string = self._encrypt_bytes(string)
        encoded = b64encode(encrypted_string).rstrip(b'=')
        return f'{encoded.decode()}'

