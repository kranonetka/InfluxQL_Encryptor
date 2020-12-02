import json
import os
import pprint

from flask import Flask, request, Response
import itertools
from helpers import get_field_keys
from parsers import QueryParser, WriteParser, Action
from postgres_connector import PostgresConnector
from result_aggregator import ResultAggregator
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
    if request.method == 'GET':
        params = request.args.to_dict()
        db_name = params.get("db")
        influxql_query = params.get("q")
        
        tokens = query_parser.parse(influxql_query)
        
        if tokens["action"] == Action.SHOW_FIELD_KEYS:
            field_keys = get_field_keys(db_name, tokens['measurement'])
            return {
                "results": [
                    {
                        "statement_id": 0,
                        "series": [
                            {
                                "name": tokens['measurement'],
                                "columns": [
                                    "fieldKey",
                                    "fieldType"
                                ],
                                "values": field_keys
                            }
                        ]
                    }
                ]
            }
        
        elif tokens['action'] == Action.SHOW_TAG_KEYS:
            with open('tags.json', 'r') as fp:
                data = json.load(fp)
            tags = data.get(db_name, {}).get(tokens['measurement'], [])
            res = {
                "results": [
                    {
                        "statement_id": 0,
                        "series": [
                            {
                                "name": tokens['measurement'],
                                "columns": [
                                    "tagKey"
                                ],
                                "values": tags
                            }
                        ]
                    }
                ]
            }
            return res
        
        postgres_query, params = QueryAggregator.assemble(tokens)
        
        result = postgres_connector.execute(postgres_query, params)
        
        return ResultAggregator.assemble(result, tokens)
    
    return Response(status=404)


@app.route('/write', methods=["POST"])
def write():
    payload = request.get_data().strip().decode()
    
    single_line_tokens = write_parser.parse(payload)[0]  # TODO: multiple lines (not only 1st)
    
    postgres_query, params = WriteAggregator.assemble(single_line_tokens)
    
    postgres_connector.execute(postgres_query, params)
    
    return Response(status=204)


if __name__ == '__main__':
    # TODO: Create UDF ope_sum
    app.run(debug=True, host='0.0.0.0', port=FLASK_PORT)
