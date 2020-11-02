from base64 import b64encode
import pickle

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from parsimonious.nodes import NodeVisitor, Node


class _BaseEncryptor(NodeVisitor):
    def __init__(self, key: bytes):
        super().__init__()
        self._cipher_factory = Cipher(
            algorithm=algorithms.AES(key),
            mode=modes.CBC(
                initialization_vector=b'\x00' * 16
            )
        )
        self._padder_factory = padding.PKCS7(8 * len(key))
    
    def _encrypt_bytes(self, payload: bytes) -> bytes:
        padder = self._padder_factory.padder()
        
        padded = padder.update(payload) + padder.finalize()
        
        encryptor = self._cipher_factory.encryptor()
        return encryptor.update(padded) + encryptor.finalize()
    
    def _decrypt_bytes(self, payload: bytes) -> bytes:
        decryptor = self._cipher_factory.decryptor()
        decrypted = decryptor.update(payload) + decryptor.finalize()
        
        unpadder = self._padder_factory.unpadder()
        return unpadder.update(decrypted) + unpadder.finalize()
    
    def generic_visit(self, node: Node, visited_children: list):
        return ''.join(visited_children) or node.text


class WriteEncryptor(_BaseEncryptor):
    def visit_identifier(self, node: Node, visited_children: list):
        ident = node.text
        enc = self._encrypt_bytes(ident.encode())
        enc = b64encode(enc).rstrip(b'=')
        return enc.decode()

    def visit_float_lit(self, node: Node, visited_children: list):
        value = node.text
        value = float(value)
        pickle_bytes = pickle.dumps(value)
        enc = self._encrypt_bytes(pickle_bytes)
        enc = b64encode(enc).rstrip(b'=')
        return enc.decode()

    def visit_bool_lit(self, node: Node, visited_children: list):
        value = node.text
        if value in {'t', 'true', 'T', 'True', 'TRUE'}:
            value = True
        else:
            value = False
        pickle_bytes = pickle.dumps(value)
        enc = self._encrypt_bytes(pickle_bytes)
        enc = b64encode(enc).rstrip(b'=')
        return enc.decode()

    def visit_string_lit(self, node: Node, visited_children: list):
        value = node.text
        value = str(value)
        pickle_bytes = pickle.dumps(value)
        enc = self._encrypt_bytes(pickle_bytes)
        enc = b64encode(enc).rstrip(b'=')
        return enc.decode()

    def visit_int_lit(self, node: Node, visited_children: list):
        value = node.text
        value = int(value.rstrip("i"))
        pickle_bytes = pickle.dumps(value)
        enc = self._encrypt_bytes(pickle_bytes)
        enc = b64encode(enc).rstrip(b'=')
        return enc.decode()


class QueryEncryptor(_BaseEncryptor):
    pass


if __name__ == '__main__':
    pass