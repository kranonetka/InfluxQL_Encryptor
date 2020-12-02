import base64
from functools import partial

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from paillier import crypto as paillier_crypto
from paillier.keygen import PublicKey, SecretKey
from pyope.ope import OPE, ValueRange


class Encryptor:
    def __init__(
            self, key: bytes, types: dict, paillier_pub_key: PublicKey, paillier_priv_key: SecretKey,
            float_converting_ratio=2 ** 50):
        
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
            in_range=ValueRange(0, 2 ** 64 - 1),
            out_range=ValueRange(2 ** 64, 2 ** 128)
        )
        
        self._phe_encryptor = partial(paillier_crypto.encrypt, pk=paillier_pub_key)
        self._phe_decryptor = partial(paillier_crypto.decrypt, pk=paillier_pub_key, sk=paillier_priv_key)
        
        self.types = types
    
    def encrypt_bytes(self, payload: bytes) -> bytes:
        """
        Зашифровать детерменированным шифром последовательность байтов
        
        :param payload: Последовательность байтов
        :return: Зашифрованные байты
        """
        padder = self._padder_factory.padder()
        
        padded = padder.update(payload) + padder.finalize()
        
        encryptor = self._cipher_factory.encryptor()
        return encryptor.update(padded) + encryptor.finalize()
    
    def phe_encrypt(self, value: int) -> int:
        """
        Зашифровать значение шифром, сохраняющим операцию суммирования
        
        :param value: Значение
        :return: Зашифрованное значение
        """
        return self._phe_encryptor(value)
    
    def phe_decrypt(self, encrypted_value: int) -> int:
        """
        Расшифровать значение, зашифрованное ``Encryptor.phe_encrypt``
        
        :param encrypted_value: Зашифрованное значение
        :return: Расшифрованное значение
        """
        return self._phe_decryptor(encrypted_value)
    
    def ope_encrypt(self, value: int) -> int:
        """
        Зашифрование значение шифром, сохраняющим порядок
        
        :param value: Значение
        :return: Зашифрованное значение
        """
        return self._ope_cipher.encrypt(value)
    
    def ope_decrypt(self, encrypted_value: int) -> int:
        """
        Расшифровать значение, зашифрованное ``Encryptor.ope_encrypt``
        
        :param encrypted_value: Зашифрованное значение
        :return: Расшифрованное значение
        """
        return self._ope_cipher.decrypt(encrypted_value)
    
    def decrypt_bytes(self, payload: bytes) -> bytes:
        """
        Расшифровать последовательность байтов, зашифрованных ``Encryptor.encrypt_bytes``
        
        :param payload: Зишифрованные байты
        :return: Расшифрованные байты
        """
        decryptor = self._cipher_factory.decryptor()
        decrypted = decryptor.update(payload) + decryptor.finalize()
        
        unpadder = self._padder_factory.unpadder()
        return unpadder.update(decrypted) + unpadder.finalize()
    
    def encrypt_string(self, payload: str) -> str:
        """
        Зашифровать строку детерменированным шифром
        
        :param payload: Исходная строка
        :return: base64 от зашифрованной строки
        """
        encrypted = self.encrypt_bytes(payload.encode())
        
        return base64.b64encode(encrypted).decode()
    
    def decrypt_string(self, encrypted_payload: str) -> str:
        """
        Расшифровать строку, зашифрованную ``Encryptor.decrypt_string``
        
        :param encrypted_payload: Зашифрованная строка
        :return: Расшифрованная строка
        """
        decoded = base64.b64decode(encrypted_payload)
        
        return self.decrypt_bytes(decoded).decode()
    
    def float_to_int(self, value: float) -> int:
        r = (value * self._float_converting_ratio).as_integer_ratio()
        
        if r[1] != 1:
            raise ValueError(f'Too small float converting ratio ({self._float_converting_ratio}) for value {value}.'
                             f' Residual denominator {r[1]}')
        
        return r[0]
    
    def int_to_float(self, value: int) -> float:
        return value / self._float_converting_ratio
