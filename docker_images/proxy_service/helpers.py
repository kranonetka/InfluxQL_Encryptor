import sqlite3
from itertools import chain
import json
import psycopg2

PG_HOST = '127.0.0.1'
PG_PORT = '5432'
PG_USERNAME = 'postgres'
PG_PASSWORD = 'password'


def set_type(db, meas, field_key, field_type):
    connection = sqlite3.connect("types.db")
    cursor = connection.cursor()
    insert = f"INSERT OR IGNORE INTO types (db, meas, field_key, field_type) VALUES (?,?,?,?);"
    cursor.execute(insert, (db, meas, field_key, field_type))
    connection.commit()


def init_db():
    connection = sqlite3.connect("types.db")
    cursor = connection.cursor()
    create_table = """
    CREATE TABLE IF NOT EXISTS types(
    db text,
    meas text,
    field_key text,
	field_type text,
	UNIQUE(db, meas, field_key, field_type)
    )
    """
    cursor.execute(create_table)


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

    return [list(i) for i in types.get(db_name, {}).get(table, {}).items()]


def get_query_and_data(info):
    table_name = info['measurement']
    tags_keys = ','.join(info['tags'].keys())
    fields_keys = ','.join(info['fields'].keys())
    number_of_values = len(info['tags']) + len(info['fields'])
    data = (*info['tags'].values(), *info['fields'].values())
    query = f"INSERT INTO {table_name} ({tags_keys},{fields_keys},time) VALUES ({'%s,' * number_of_values}now())"
    return query, data
