import psycopg2
from ._base_encryptor import _BaseEncryptor
from ._grammars import write_grammar
from parsimonious.nodes import Node

HOST = '127.0.0.1'
PORT = '5432'
USERNAME = 'postgres'
PASSWORD = 'password'
DATABASE = 'tutorial'


class WriteVisitor(_BaseEncryptor):
    grammar = write_grammar

    def __init__(self):
        self.measurement = None
        self.tags = {}
        self.fields = {}
        self.time = None

    def visit_measurement(self, node: Node, visited_children: tuple):
        self.measurement = node.text

    def visit_tag(self, node: Node, visited_children: tuple):
        self.tags[node.children[0].text] = node.children[2].text

    def visit_field(self, node: Node, visited_children: tuple):
        self.fields[node.children[0].text] = node.children[2].text

    def visit_timestamp(self, node, visited_children):
        self.time = node.text

    def generic_visit(self, node: Node, visited_children: tuple):
        return self.__dict__


def get_query_and_data(info):
    table_name = info['measurement']
    tags_keys = ','.join(info['tags'].keys())
    fields_keys = ','.join(info['fields'].keys())
    number_of_values = len(info['tags']) + len(info['fields'])
    data = (*info['tags'].values(), *info['fields'].values())
    query = f"INSERT INTO {table_name} ({tags_keys},{fields_keys},time) " \
            f"VALUES ({'%s,' * number_of_values}now())"
    return query, data


def main():
    data = "cpu_load_short,host=server01,region=us-west value=2"
    wp = WriteVisitor()
    info = wp.parse(data)

    conn = psycopg2.connect(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, dbname=DATABASE)
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    cur.execute(f"DROP TABLE IF EXISTS {info['measurement']}")
    query_create_sensordata_table = f"""CREATE TABLE {info['measurement']} (
                                              time TIMESTAMPTZ NOT NULL,
                                              host VARCHAR(256),
                                              region VARCHAR(256),
                                              value DOUBLE PRECISION
                                              );"""
    query_create_sensordata_hypertable = f"SELECT create_hypertable('{info['measurement']}', 'time');"
    cur.execute(query_create_sensordata_table)
    cur.execute(query_create_sensordata_hypertable)
    query, data = get_query_and_data(info)
    cur.execute(query, data)
    conn.commit()
    print(query)


if __name__ == "__main__":
    main()
