from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from parsimonious.nodes import NodeVisitor, Node
from pyope.ope import OPE, ValueRange


class _BaseEncryptor(NodeVisitor):
    def __init__(self, key: bytes, ope_key: bytes):
        super().__init__()
        self._cipher_factory = Cipher(
            algorithm=algorithms.AES(key),
            mode=modes.CBC(
                initialization_vector=b'\x00' * 16
            )
        )
        self._padder_factory = padding.PKCS7(8 * len(key))
        self._ope_cipher = OPE(ope_key, in_range=ValueRange(0, 2**60 - 1), out_range=ValueRange(0, 2**64 - 1))

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

    def generic_visit(self, node: Node, visited_children: tuple):
        return ''.join(visited_children) or node.text


if __name__ == '__main__':
    pass
