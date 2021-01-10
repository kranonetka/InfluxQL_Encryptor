import json
import logging
import os
import pprint
from pathlib import Path

from flask import Flask, request, Response

from encryptor import EncryptionFactory
from helpers import get_field_keys, get_tag_keys
from key_storage import load_keys
from parsers import QueryParser, WriteParser, Action
from postgres_connector import PostgresConnector
from result_aggregator import ResultAggregator
from token_aggregators import QueryAggregator, WriteAggregator

logger = logging.getLogger('proxy')
logger.setLevel(logging.DEBUG)

try:
    FLASK_PORT = os.environ['FLASK_PORT']
    POSTGRES_HOST = os.environ['POSTGRES_HOST']
    POSTGRES_PORT = os.environ['POSTGRES_PORT']
    POSTGRES_USERNAME = os.environ['POSTGRES_USERNAME']
    POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
    POSTGRES_DATABASE = os.environ['POSTGRES_DATABASE']
except KeyError as ope_key:
    raise RuntimeError(f'{ope_key} missing in environment')

app = Flask(__name__)

postgres_connector = PostgresConnector(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USERNAME,
    password=POSTGRES_PASSWORD,
    db=POSTGRES_DATABASE
)

with Path(__file__).parent as _root:
    with (_root / 'columns.json').open('r') as fp:
        columns = json.load(fp)

phe_public_key, phe_private_key, ope_key = load_keys()

query_parser = QueryParser()
write_parser = WriteParser()

with EncryptionFactory(columns=columns,
                       float_converting_ratio=2 ** 55,
                       paillier_pub_key=phe_public_key,
                       paillier_priv_key=phe_private_key,
                       ope_key=ope_key) as encryption_factory:
    write_tokens_encryptor = encryption_factory.write_encryptor()
    query_tokens_encryptor = encryption_factory.query_encryptor()
    result_decryptor = encryption_factory.result_decryptor()

query_tokens_aggregator = QueryAggregator(
    phe_n=phe_public_key.n
)


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
        
        logger.debug(f'{influxql_query=} on {db_name=}')
        
        tokens = query_parser.parse(influxql_query)
        logger.debug(f'{tokens=}')
        
        if tokens['action'] in (Action.SHOW_FIELD_KEYS, Action.SHOW_TAG_KEYS):
            if tokens['action'] == Action.SHOW_TAG_KEYS:
                func = get_tag_keys
            else:
                func = get_field_keys
            result = func(columns, db_name, tokens['measurement'])
            return ResultAggregator.assemble(result, tokens)
        
        encrypted_tokens = query_tokens_encryptor.encrypt(tokens)
        logger.debug(f'{encrypted_tokens=}')
        
        postgres_query, params = query_tokens_aggregator.assemble(encrypted_tokens)
        logger.debug(f'{postgres_query=}, {params=}')
        
        result = postgres_connector.execute(postgres_query, params)
        logger.debug(f'{len(result)=}')
        
        if tokens['action'] in (Action.SELECT, Action.SHOW_TAG_VALUES):
            result = result_decryptor.decrypt(result, db=db_name, tokens=tokens)
        
        return ResultAggregator.assemble(result, tokens)
    
    return Response(status=404)


@app.route('/write', methods=["POST"])
def write():
    payload = request.get_data().strip().decode()
    logger.debug(f'{payload=}')
    
    for single_line_tokens in write_parser.parse(payload):
        logger.debug(f'{single_line_tokens=}')
        
        encrypted_tokens = write_tokens_encryptor.encrypt(single_line_tokens, db=request.args.get("db"))
        logger.debug(f'{encrypted_tokens=}')
        
        postgres_query, params = WriteAggregator.assemble(encrypted_tokens)
        logger.debug(f'{postgres_query=}, {params=}')
        
        postgres_connector.execute(postgres_query, params)
    
    return Response(status=204)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=FLASK_PORT)
