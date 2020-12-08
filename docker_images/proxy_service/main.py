import json
import os
import pickle
import pprint
from pathlib import Path

from flask import Flask, request, Response
from paillier import keygen

from encryptor import WriteTokensEncryptor, QueryEncryptor, ResultDecryptor
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

with Path(__file__).parent as _root:
    with (_root / 'types.json').open('r') as fp:
        types = json.load(fp)
    
    with (_root / 'key_storage' / 'phe_public_key').open(mode='rb') as fp:
        phe_public_key = pickle.load(fp)  # type: keygen.PublicKey
    
    with (_root / 'key_storage' / 'phe_private_key').open(mode='rb') as fp:
        phe_private_key = pickle.load(fp)

key = b'a' * 32

query_parser = QueryParser()
write_parser = WriteParser()
write_tokens_encryptor = WriteTokensEncryptor(types=types,
                                              float_converting_ratio=2 ** 55,
                                              paillier_pub_key=phe_public_key,
                                              paillier_priv_key=phe_private_key,
                                              key=key)

query_tokens_encryptor = QueryEncryptor(types=types,
                                        float_converting_ratio=2 ** 55,
                                        paillier_pub_key=phe_public_key,
                                        paillier_priv_key=phe_private_key,
                                        key=key)

result_decryptor = ResultDecryptor(types=types,
                                   float_converting_ratio=2 ** 55,
                                   paillier_pub_key=phe_public_key,
                                   paillier_priv_key=phe_private_key,
                                   key=key)

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
        
        tokens = query_parser.parse(influxql_query)
        
        if tokens["action"] == Action.SHOW_FIELD_KEYS:
            field_keys = get_field_keys(db_name, tokens['measurement'])
            return ResultAggregator.assemble(field_keys, tokens)
        
        elif tokens['action'] == Action.SHOW_TAG_KEYS:
            with open('tags.json', 'r') as fp:
                data = json.load(fp)
            tag_keys = data.get(db_name, {}).get(tokens['measurement'], [])
            return ResultAggregator.assemble(tag_keys, tokens)
        
        postgres_query, params = query_tokens_aggregator.assemble(tokens)
        
        result = postgres_connector.execute(postgres_query, params)
        
        if tokens['action'] == Action.SELECT:
            result = result_decryptor.decrypt(result, db=db_name, tokens=tokens)
        
        elif tokens['action'] == Action.SHOW_TAG_VALUES:
            result = result_decryptor.decrypt(result, db=db_name, tokens=tokens)
        
        return ResultAggregator.assemble(result, tokens)
    
    return Response(status=404)


@app.route('/write', methods=["POST"])
def write():
    payload = request.get_data().strip().decode()
    
    single_line_tokens = write_parser.parse(payload)[0]  # TODO: multiple lines (not only 1st)
    
    encrypted_tokens = write_tokens_encryptor.encrypt(single_line_tokens, db=request.args.get("db"))
    
    postgres_query, params = WriteAggregator.assemble(encrypted_tokens)
    
    postgres_connector.execute(postgres_query, params)
    
    return Response(status=204)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=FLASK_PORT)
