from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from parsimonious.nodes import NodeVisitor, Node
from pyope.ope import OPE, ValueRange


class _BaseEncryptor(NodeVisitor):
    def __init__(self, key: bytes, float_converting_ratio=2 ** 50):
        super().__init__()
        self._cipher_factory = Cipher(
            algorithm=algorithms.AES(key),
            mode=modes.CBC(
                initialization_vector=b'\x00' * 16
            ),
            backend=default_backend()
        )
        self._padder_factory = padding.PKCS7(8 * len(key))
        self._float_converting_ratio = float_converting_ratio
        self._ope_cipher = OPE(
            key=key,
            in_range=ValueRange(-1125899906842624000, 1125899906842624000),
            out_range=ValueRange(-9223372036854775808, 9223372036854775807)
        )
    
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
    
    def _float_to_int(self, value: float) -> int:
        r = (value * self._float_converting_ratio).as_integer_ratio()
        assert r[1] == 1, 'Not enough precision'
        return r[0]
    
    def _int_to_float(self, value: int) -> float:
        return value / self._float_converting_ratio
    
    def generic_visit(self, node: Node, visited_children: tuple):
        return ''.join(visited_children) or node.text
