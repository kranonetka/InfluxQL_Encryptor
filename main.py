import json
import uuid
from base64 import b64decode, b64encode
from functools import partial

import requests
from requests import Response

from encryptors import WriteEncryptor
from grammars import write_grammar

url = 'http://localhost:8086'


def encrypt_write(payload: str, key: bytes) -> str:
    tree = write_grammar.parse(payload)
    encryptor = WriteEncryptor(key)
    return encryptor.visit(tree)


def decrypt_identifier(encrypted_identifier: str, key: bytes) -> str:
    try:
        rpad_name = encrypted_identifier + '=' * (len(encrypted_identifier) % 4)
        raw = b64decode(rpad_name)
        cryptor = WriteEncryptor(key)
        return cryptor._decrypt_bytes(raw).decode()
    except:
        return encrypted_identifier


def encrypt_identifier(identifier: str, key: bytes) -> str:
    cryptor = WriteEncryptor(key)
    enc = b64encode(cryptor._encrypt_bytes(identifier.encode())).rstrip(b'=')
    return enc.decode()


def write_payload(payload: str):
    requests.post(url + '/query', params=dict(q='CREATE DATABASE test'))
    payload = payload.encode()
    return requests.post(url + '/write', params=dict(db='test'), data=payload)



if __name__ == '__main__':
    from itertools import islice, cycle
    
    key = bytes(islice(cycle(b'key'), 0, 32))

    payloads = {
        f'another_meas,uuid={str(uuid.uuid4())} float_value=45.24',
        f'another_meas,uuid={str(uuid.uuid4())} int_value=67i',
        f'another_meas,uuid={str(uuid.uuid4())} bool_value=False',
        f'another_meas,uuid={str(uuid.uuid4())} bool_value=false',
        f'another_meas,uuid={str(uuid.uuid4())} bool_value=f',
        f'another_meas,uuid={str(uuid.uuid4())} bool_value=F',
        f'another_meas,uuid={str(uuid.uuid4())} string_value=Hello'
    }
    
    for payload in map(partial(encrypt_write, key=key), payloads):
        print(f'{payload}: {write_payload(payload)}')