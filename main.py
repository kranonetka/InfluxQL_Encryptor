import uuid
from base64 import b64decode, b64encode
from functools import partial

import requests
from pyope.ope import OPE

from encryptors.write_encryptor import WriteEncryptor
from grammars import write_grammar

url = 'http://localhost:8086'


def encrypt_write(payload: str, key: bytes, ope_key: bytes) -> str:
    tree = write_grammar.parse(payload)
    encryptor = WriteEncryptor(key, ope_key=ope_key)
    res = encryptor.visit(tree)
    types = encryptor.types
    print(f'types: {types}')
    return res


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
    ope_key = OPE.generate_key()

    values = {
        'int_value=25i',
        'float_value=75.8',
        'bool_value=TRUE',
        'bool_value=True',
        'bool_value=T',
        'bool_value=true',
        'bool_value=t',
        'string_value=Hello'
    }

    payloads = set()
    for value in values:
        payloads.add(f'another_meas,uuid={str(uuid.uuid4())} {value}')

    for payload in payloads:
        print(payload)

    for payload in map(partial(encrypt_write, key=key, ope_key=ope_key), payloads):
        print(f'{payload}: {write_payload(payload)}')
