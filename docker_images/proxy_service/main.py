import os
from itertools import islice, cycle

import requests
from flask import Flask, request

from encryptors import QueryEncryptor, WriteEncryptor, WriteVisitor
from helpers import *

for env_var in {'INFLUXDB_HOST', 'INFLUXDB_PORT',
                'FLASK_PORT',
                'POSTGRES_HOST', 'POSTGRES_PORT',
                'POSTGRES_USERNAME', 'POSTGRES_PASSWORD',
                'POSTGRES_DATABASE'}:
    if env_var not in os.environ:
        raise RuntimeError(f'Missing environment variable {env_var}')

INFLUX_HOST = os.environ["INFLUXDB_HOST"]
INFLUX_PORT = os.environ["INFLUXDB_PORT"]
FLASK_PORT = os.environ['FLASK_PORT']
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_USERNAME = os.environ["POSTGRES_USERNAME"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_DATABASE = os.environ["POSTGRES_DATABASE"]

app = Flask(__name__)

conn = psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, user=POSTGRES_USERNAME,
                        password=POSTGRES_PASSWORD, dbname=POSTGRES_DATABASE)

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
    db_name = params.get("db")
    query_plain = params.get("q")

    if request.method == 'GET':
        qe = query_encryptor.parse(query_plain)

        if qe["action"] == "show retention policies":
            if db_is_exists_in_postgres(qe["database"]):
                return {"results": [{"statement_id": 0, "series": [
                    {"columns": ["name", "duration", "shardGroupDuration", "replicaN", "default"],
                     "values": [["autogen", "0s", "168h0m0s", 1, True]]}]}]}
            else:
                return {"results": [{"statement_id": 0, "error": f"database not found: {qe['database']}"}]}

        if qe["action"] == "show measurements":
            tables = get_tables_from_postgres(db_name)
            return {"results": [{"statement_id": 0, "series": [
                {"name": "measurements", "columns": ["name"], "values": [*tables]}]}]}

        if qe["action"] == "show field keys":
            field_keys = get_field_keys(db_name, qe['measurement'])
            return {"results": [{"statement_id": 0, "series": [
                {"name": qe['measurement'], "columns": ["fieldKey", "fieldType"],
                 "values": field_keys}]}]}

    print("AAA")
    return "AAA"


@app.route('/write', methods=["POST"])
def write():
    params = request.args
    headers = request.headers
    database = params.get("db")
    data = request.get_data().decode()  # bytes
    data = data.lstrip(' ')  # First character from influx client is space
    data = data.rstrip('\n')  # Last character from influx client is newline char

    info = WriteVisitor().parse(data)
    encryped_info = encrypt_fields(info, database)
    print(info)
    print(encryped_info)

    query, data = get_query_and_data(info)
    print(query, data)

    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit()
    cur.close()

    return 'posted'


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=FLASK_PORT)
