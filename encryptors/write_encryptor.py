import pickle
from base64 import b64encode

from parsimonious.nodes import Node

from encryptors import _BaseEncryptor


class WriteEncryptor(_BaseEncryptor):
    def __init__(self, *args, **kwargs):
        super(WriteEncryptor, self).__init__(*args, **kwargs)
        self._types = dict()

    @property
    def types(self):
        retval = self._types
        self._types = dict()
        return retval

    def _float_to_int(self, float_value):
        ratio = (1 << 50)
        r = (float_value * ratio).as_integer_ratio()
        assert r[1] == 1, 'Not enough precision'
        return r[0]

    def _get_encrypted_pickle_bytes(self, data):
        pickle_bytes = pickle.dumps(data)
        enc = self._encrypt_bytes(pickle_bytes)
        enc = b64encode(enc).rstrip(b'=')
        return enc.decode()

    def visit_field(self, node: Node, visited_children: list):
        column = node.children[0].text
        type_of_column = node.children[2].children[0].expr_name
        self._types[column] = type_of_column
        return self.generic_visit(node, visited_children)

    def visit_identifier(self, node: Node, visited_children: tuple):
        ident = node.text
        enc = self._encrypt_bytes(ident.encode())
        enc = b64encode(enc).rstrip(b'=')
        return enc.decode()

    def visit_int_lit(self, node: Node, visited_children: list):
        value = int(node.text[:-1])
        encrypted_value = self._ope_cipher.encrypt(value)
        return f'{encrypted_value}i'

    def visit_float_lit(self, node: Node, visited_children: list):
        value = float(node.text)
        value_in_int = self._float_to_int(value)
        encrypted_value = self._ope_cipher.encrypt(value_in_int)
        return f'{encrypted_value}i'

    # def visit_bool_lit(self, node: Node, visited_children: list):
    #     if node.text in {'t', 'true', 'T', 'True', 'TRUE'}:
    #         value = True
    #     else:
    #         value = False
    #     return self._get_encrypted_pickle_bytes(value)

    def visit_string_lit(self, node: Node, visited_children: list):
        value = node.text.encode()
        enc_value = self._encrypt_bytes(value)
        enc = b64encode(enc_value).rstrip(b'=')
        return enc.decode()
