import random
import uuid
from base64 import b64encode

import requests
from itertools import combinations
from itertools import islice, cycle

from docker_images.proxy_service.encryptors import WriteEncryptor, QueryEncryptor

key = bytes(islice(cycle(b'key'), 0, 32))
url = 'http://localhost:8086'

write_encryptor = WriteEncryptor(key)
query_encryptor = QueryEncryptor(key)


def encrypt_write(payload: str) -> str:
    res = write_encryptor.parse(payload)
    types = write_encryptor.types
    print(f'types: {types}')
    return res


def write_payload(payload: str):
    requests.post(url + '/query', params=dict(q='CREATE DATABASE test'))
    payload = payload.encode()
    return requests.post(url + '/write', params=dict(db='test'), data=payload)


if __name__ == '__main__':
    
    values = (
        lambda: f'int_value={random.randint(-1000, 1000)}i',
        lambda: f'float_value={random.uniform(-1000, 1000)}',
    )
    
    payload_tpl = 'another_meas,uuid={} {}'
    payloads = list()
    for comb_len in range(1, 6):
        for value_generators in combinations(values, comb_len):
            payload = payload_tpl.format(
                str(uuid.uuid4()),
                ','.join(gen() for gen in value_generators)
            )
            encrypted_payload = encrypt_write(payload)
            print(payload)
            print(encrypted_payload)
            # resp = write_payload(encrypted_payload)
            # try:
            #     resp.raise_for_status()
            # except requests.HTTPError:
            #     raise Exception(f'{resp.status_code}: {resp.text}')
