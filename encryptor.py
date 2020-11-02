import json
import sqlite3
import uuid
from base64 import b64decode, b64encode
from functools import partial

import requests
from requests import Response

from encryptors import WriteEncryptor
from grammars import write_grammar

url = 'http://localhost:8086'

with sqlite3.connect('types.sqlite') as conn:
    conn.execute('CREATE TABLE IF NOT EXISTS types (name TEXT, type TEXT)')
    types = dict(conn.execute('SELECT * FROM types'))


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


def select_all(meas_name, key):
    meas_name = encrypt_identifier(meas_name, key)
    res: Response = requests.post(
        url + '/query',
        params=dict(
            db='test',
            q=f'SELECT COUNT("Yb3dWkIvdAT9gThEP7VuBWWC471I3xMn+gCLxoPLP7A") FROM "{meas_name}" '
              f'WHERE \'1800-01-01T00:00:00Z\' <= time AND time < \'1900-01-01T00:00:00Z\' + 1d GROUP BY time(7s)'
        )
    )
    print(res.elapsed.total_seconds())
    res: dict = res.json()
    try:
        batch = res['results'][0]['series'][0]
    except:
        return res
    columns: list = batch['columns']
    columns = list(map(partial(decrypt_identifier, key=key), columns))
    return [dict(zip(columns, values)) for values in batch['values']]


if __name__ == '__main__':
    from itertools import islice, cycle
    
    key = bytes(islice(cycle(b'key'), 0, 32))
    payloads = {
        f'another_meas,uuid={str(uuid.uuid4())} value=45.24 -2208988800',
        f'another_meas,uuid={str(uuid.uuid4())} v=67i 1800-01-01T00:00:00Z'
    }
    
    for payload in map(partial(encrypt_write, key=key), payloads):
        print(f'{payload}: {write_payload(payload)}')
    
    print(json.dumps(select_all(meas_name='another_meas', key=key), indent=4))
