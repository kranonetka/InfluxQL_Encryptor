import os
import pprint

from flask import Flask, request, Response

from helpers import get_field_keys
from parsers import QueryParser, WriteParser, Action
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

query_parser = QueryParser()
write_parser = WriteParser()


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
    db_name = params.get("db")
    influxql_query = params.get("q")
    
    if request.method == 'GET':
        tokens = query_parser.parse(influxql_query)
        
        postgres_query = QueryAggregator.assemble(tokens)
        
        result = postgres_connector.execute(query)
        
        if tokens["action"] == Action.SHOW_RETENTION_POLICIES:
            if postgres_connector.is_db_exists(tokens["database"]):
                return {"results": [{"statement_id": 0, "series": [
                    {"columns": ["name", "duration", "shardGroupDuration", "replicaN", "default"],
                     "values": [["autogen", "0s", "168h0m0s", 1, True]]}]}]}
            else:
                return {"results": [{"statement_id": 0, "error": f"database not found: {tokens['database']}"}]}
        
        if tokens["action"] == Action.SHOW_MEASUREMENTS:
            tables = postgres_connector.get_tables()
            return {"results": [{"statement_id": 0, "series": [
                {"name": "measurements", "columns": ["name"], "values": [*tables]}]}]}
        
        if tokens["action"] == Action.SHOW_FIELD_KEYS:
            field_keys = get_field_keys(db_name, tokens['measurement'])
            return {"results": [{"statement_id": 0, "series": [
                {"name": tokens['measurement'], "columns": ["fieldKey", "fieldType"],
                 "values": field_keys}]}]}

    # print("AAA")
    return "AAA"


@app.route('/write', methods=["POST"])
def write():
    # a = parse()
    # b = encrypt(a)
    # c = assemble(b)
    # postgres_exeute(c)

    data = request.get_data().strip().decode()
    single_line_tokens = write_parser.parse(data)[0]
    query, data = WriteAggregator.assemble(single_line_tokens)

    postgres_connector.execute(query, data)
    return Response(status=204)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=FLASK_PORT)
