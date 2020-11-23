import json
from itertools import chain

import psycopg2
from pyope.ope import OPE, ValueRange

PG_HOST = '127.0.0.1'
PG_PORT = '5432'
PG_USERNAME = 'postgres'
PG_PASSWORD = 'password'

ope_cipher = OPE(
    key=b"a" * 32,
    in_range=ValueRange(-1125899906842624000, 1125899906842624000),
    out_range=ValueRange(-9223372036854775808, 9223372036854775807)
)


def db_is_exists_in_postgres(db_name):
    connection = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USERNAME, password=PG_PASSWORD)
    cur = connection.cursor()
    cur.execute("SELECT datname FROM pg_database;")
    return db_name in chain.from_iterable(cur)


def get_tables_from_postgres(db_name):
    connection = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USERNAME, password=PG_PASSWORD, dbname=db_name)
    cur = connection.cursor()
    cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
    """)
    tables = cur.fetchall()
    return chain.from_iterable(tables)


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
