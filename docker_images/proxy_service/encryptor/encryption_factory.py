from .query_encryptor import QueryEncryptor
from .result_decryptor import ResultDecryptor
from .write_encryptor import WriteTokensEncryptor


class EncryptionFactory:
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
    
    def result_decryptor(self):
        return ResultDecryptor(*self._args, **self._kwargs)
    
    def query_encryptor(self):
        return QueryEncryptor(*self._args, **self._kwargs)
    
    def write_encryptor(self):
        return WriteTokensEncryptor(*self._args, **self._kwargs)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
