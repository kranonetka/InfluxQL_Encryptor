import requests
import psutil
import socket
import os

for env_var in {'TARGET_HOST', 'TARGET_PORT'}:
    if env_var not in os.environ:
        raise RuntimeError(f'Missing environment variable {env_var}')

query_url = f'http://{os.environ["TARGET_HOST"]}:{os.environ["TARGET_PORT"]}/query'
write_url = f'http://{os.environ["TARGET_HOST"]}:{os.environ["TARGET_PORT"]}/write'

if __name__ == '__main__':
    payload_tpl = f'laptop_meas,hostname={socket.gethostname()} ' \
                  'cpu_percent={cpu_percent},cpu_freq={cpu_freq},memory_used={memory_used}i'
    print(payload_tpl)

    # requests.post(query_url, params=dict(q='CREATE DATABASE laptop'))
    params = dict(db='laptop')
    while True:
        payload = payload_tpl.format(
            cpu_percent=psutil.cpu_percent(),
            cpu_freq=psutil.cpu_freq().current,
            memory_used=psutil.virtual_memory().used
        ).encode()
        requests.post(write_url, params=params, data=payload)
