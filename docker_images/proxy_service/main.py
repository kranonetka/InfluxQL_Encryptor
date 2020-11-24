import os
import pprint

from flask import Flask, request, Response

from helpers import db_is_exists_in_postgres, get_field_keys, get_query_and_data, get_tables_from_postgres, \
    encrypt_fields
from parsers import QueryParser, WriteParser
from postgres_connector import PostgresConnector
from token_aggregators import QueryAggregator, WriteAggregator

try:
    FLASK_PORT = os.environ['FLASK_PORT']
    POSTGRES_HOST = os.environ['POSTGRES_HOST']
    POSTGRES_PORT = os.environ['POSTGRES_PORT']
    POSTGRES_USERNAME = os.environ['POSTGRES_USERNAME']
    POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
    POSTGRES_DATABASE = os.environ['POSTGRES_DATABASE']
except KeyError as key:
    raise RuntimeError(f'{key} missing in environment')

app = Flask(__name__)

postgres_connector = PostgresConnector(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USERNAME,
    password=POSTGRES_PASSWORD,
    db=POSTGRES_DATABASE
)

query_encryptor = QueryParser()
write_parser = WriteParser()

query_aggregator = QueryAggregator()
write_aggregator = WriteAggregator()


@app.route('/')
def index():
    return f'<pre>{pprint.pformat(globals(), indent=4)}</pre>'


@app.route('/debug', defaults={'path': ''})
@app.route('/debug/<path:path>', methods=["GET"])
def debug(path):
    return 'ok'  # TODO


@app.route('/ping', methods=['GET'])
def ping():
    return 'ok'  # TODO


@app.route('/query', methods=["POST", "GET"])
def query():
    params = request.args.to_dict()
    # headers = request.headers
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
    # a = parse()
    # b = encrypt(a)
    # c = assemble(b)
    # postgres_exeute(c)
    
    params = request.args
    # headers = request.headers
    database = params.get("db")
    data = request.get_data().strip().decode()
    
    single_line_tokens = write_parser.parse(data)[0]
    encryped_info = encrypt_fields(single_line_tokens, database)
    print(single_line_tokens)
    print(encryped_info)
    
    query, data = get_query_and_data(single_line_tokens)
    print(query, data)
    
    postgres_connector.execute(query, data)
    return Response(status=204)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=FLASK_PORT)
