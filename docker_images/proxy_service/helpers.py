import json
import os
from pyope.ope import OPE, ValueRange

from postgres_connector import PostgresConnector

try:
    POSTGRES_HOST = os.environ['POSTGRES_HOST']
    POSTGRES_PORT = os.environ['POSTGRES_PORT']
    POSTGRES_USERNAME = os.environ['POSTGRES_USERNAME']
    POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
    POSTGRES_DATABASE = os.environ['POSTGRES_DATABASE']
except KeyError as key:
    raise RuntimeError(f'{key} missing in environment')

postgres_connector = PostgresConnector(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USERNAME,
    password=POSTGRES_PASSWORD,
    db=POSTGRES_DATABASE
)

ope_cipher = OPE(
    key=b"a" * 32,
    in_range=ValueRange(-1125899906842624000, 1125899906842624000),
    out_range=ValueRange(-9223372036854775808, 9223372036854775807)
)


def get_field_keys(db_name, table):
    with open("types.json", "r") as file:
        types: dict = json.load(file)
    
    return [[key, value['type']] for key, value in types.get(db_name, {}).get(table, {}).items()]


def encrypt(data, type, operations):
    return f"encrypted_{data}_{type}_as_{operations}"


def encrypt_fields(payload_info, database):
    with open("types.json", "r") as file:
        types: dict = json.load(file)
    # {'measurement': 'cpu_load_short', 'tags': {'host': 'server01', 'region': 'us-west'}, 'fields': {'value': '2'}, 'time': None}
    encrypted_payload_info = dict()
    encrypted_payload_info["measurement"] = payload_info["measurement"]
    encrypted_tags = dict()
    encrypted_fields = dict()
    for tag_key in payload_info["tags"].keys():
        type_of_tag = types.get(database, {}).get(payload_info["measurement"], {}).get(tag_key, {}).get('type')
        operation = types.get(database, {}).get(payload_info["measurement"], {}).get(tag_key, {}).get('operations')
        encrypted_tags[tag_key] = encrypt(payload_info["tags"][tag_key], type_of_tag, operation)
    encrypted_payload_info["tags"] = encrypted_tags
    
    for field_key in payload_info["fields"].keys():
        type_of_field = types.get(database, {}).get(payload_info["measurement"], {}).get(field_key, {}).get('type')
        operation = types.get(database, {}).get(payload_info["measurement"], {}).get(field_key, {}).get('operations')
        encrypted_fields[field_key] = encrypt(payload_info["fields"][field_key], type_of_field, operation)
    encrypted_payload_info["fields"] = encrypted_fields
    if "time" in payload_info:
        encrypted_payload_info["time"] = payload_info["time"]
    else:
        encrypted_payload_info["time"] = None
    return encrypted_payload_info


def get_query_and_data(info):
    table_name = info['measurement']
    tags_keys = ','.join(info['tags'].keys())
    fields_keys = ','.join(info['fields'].keys())
    number_of_values = len(info['tags']) + len(info['fields'])
    data = (*info['tags'].values(), *info['fields'].values())
    query = f"INSERT INTO {table_name} ({tags_keys},{fields_keys},time) VALUES ({'%s,' * number_of_values}now())"
    return query, data
