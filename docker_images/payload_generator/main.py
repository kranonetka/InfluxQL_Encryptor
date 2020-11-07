import requests
import psutil
import socket

query_url = 'http://influxdb:8086/query'
write_url = 'http://influxdb:8086/write'

if __name__ == '__main__':
    payload_tpl = f'laptop_meas,hostname={socket.gethostname()} ' \
                  'cpu_percent={cpu_percent},cpu_freq={cpu_freq},memory_used={memory_used}i'
    print(payload_tpl)
   
    requests.post(query_url, params=dict(q='CREATE DATABASE laptop'))
    params = dict(db='laptop')
    while True:
        payload = payload_tpl.format(
            cpu_percent=psutil.cpu_percent(),
            cpu_freq=psutil.cpu_freq().current,
            memory_used=psutil.virtual_memory().used
        ).encode()
        requests.post(write_url, params=params, data=payload)
