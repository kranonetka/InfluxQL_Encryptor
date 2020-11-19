import os
from itertools import islice, cycle

from flask import Flask, request

from encryptors import QueryEncryptor, WriteEncryptor

# for env_var in {'INFLUXDB_HOST', 'INFLUXDB_PORT'}:
#     if env_var not in os.environ:
#         raise RuntimeError(f'Missing environment variable {env_var}')

app = Flask(__name__)

key = bytes(islice(cycle(b'key'), 0, 32))
write_encryptor = WriteEncryptor(key)
query_encryptor = QueryEncryptor(key)


@app.route('/')
def index():
    return f'INFLUXDB_HOST={os.environ["INFLUXDB_HOST"]}, INFLUXDB_PORT={os.environ["INFLUXDB_PORT"]}'


@app.route('/ping')
def ping():
    return '/ping'


@app.route('/query')
def query():
    return '/query'


@app.route('/write')
def write():
    return '/write'


if __name__ == '__main__':
    key = b'\x00' * 32
    visitor = QueryEncryptor(key=key)
    
    query = """SELECT
    mean("memory_used")
FROM
    "laptop_meas"
WHERE
    ("hostname" = '694d42a595f2')
    AND time >= now() - 5m
GROUP BY
    time(200ms)
    fill(null)"""
    
#     query = """SELECT
#     mean("memory_used")
# FROM
#     "laptop_meas"
# WHERE
#     ("hostname" = '8d89770d7eb1')
#     AND time >= 1605680400000ms and time <= 1605680700000ms
# GROUP BY
#     time(200ms)
#     fill(null)"""
    
    print(query)
    print(visitor.parse(query))
    
    exit()
    app.run(debug=True, host='0.0.0.0', port=os.environ.get('FLASK_PORT'))
