from itertools import islice, cycle

import requests
from encryptors import QueryEncryptor, WriteEncryptor
from flask import Flask, request

for env_var in {'INFLUXDB_HOST', 'INFLUXDB_PORT'}:
    if env_var not in os.environ:
        raise RuntimeError(f'Missing environment variable {env_var}')

INFLUX_HOST = os.environ["INFLUXDB_HOST"]
INFLUX_PORT = os.environ["INFLUXDB_PORT"]
FLASK_PORT = os.environ.get('FLASK_PORT')

app = Flask(__name__)

key = bytes(islice(cycle(b'key'), 0, 32))
write_encryptor = WriteEncryptor(key)
query_encryptor = QueryEncryptor(key)


@app.route('/')
def index():
    return f'INFLUXDB_HOST={INFLUX_HOST}, INFLUXDB_PORT={INFLUX_HOST}'


@app.route('/debug', defaults={'path': ''})
@app.route('/debug/<path:path>', methods=["GET"])
def debug(path):
    params = request.args.to_dict()
    headers = request.headers
    response = requests.get(f'http://{INFLUX_HOST}:{INFLUX_PORT}/debug/{path}', params=params, headers=headers)
    return response.content


@app.route('/ping', methods=['GET'])
def ping():
    response = requests.get(f'http://{INFLUX_HOST}:{INFLUX_PORT}/ping')
    return response.content, response.status_code, response.headers.items()


@app.route('/query', methods=["POST", "GET"])
def query():
    params = request.args.to_dict()
    headers = request.headers

    # query_plain = params.get("q")
    # print(f"Plain query: {query_plain}")
    # query_encrypted = encrypt_query(query_plain)
    # print(f"Encrypted query: {query_encrypted}")
    # params["q"] = query_encrypted

    if request.method == 'GET':
        # SELECT, SHOW
        response = requests.get(f'http://{INFLUX_HOST}:{INFLUX_PORT}/query', params=params, headers=headers)
        return response.text, response.status_code
    elif request.method == 'POST':
        # ALTER CREATE DELETE DROP GRANT KILL REVOKE
        response = requests.post(f'http://{INFLUX_HOST}:{INFLUX_PORT}/query', params=params)
        return response.text, response.status_code


@app.route('/write', methods=["POST"])
def write():
    params = request.args
    headers = request.headers
    database = params.get("db")
    data = request.get_data().decode()  # bytes
    data = data.lstrip(' ')  # First character from influx client is space
    data = data.rstrip('\n')  # Last character from influx client is newline char

    encrypted_data = write_encryptor.parse(data).encode()

    # type_info = {
    #     "db": database,
    #     'measurement': data.split(',')[0],
    #     'field_types': write_encryptor.types
    # }
    #
    # print(type_info)

    response = requests.post(f'http://{INFLUX_HOST}:{INFLUX_PORT}/write', params=params, data=encrypted_data)
    return response.content, response.status_code, response.headers.items()


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=FLASK_PORT)
