import psycopg2
from parsimonious.nodes import Node

from _base_encryptor import _BaseEncryptor
from _grammars import write_grammar

HOST = '127.0.0.1'
PORT = '5432'
USERNAME = 'postgres'
PASSWORD = 'password'
DATABASE = 'tutorial'


class WriteVisitor(_BaseEncryptor):
    grammar = write_grammar

    def visit_measurement(self, node: Node, visited_children: tuple):
        return {'measurement': node.text}

    def visit_tag(self, node: Node, visited_children: tuple):
        return {node.children[0].text: node.children[2].text}

    def visit_tag_set(self, node: Node, visited_children: tuple):
        tags = dict()
        tags.update(visited_children[0])
        other_tags = visited_children[1]
        if not isinstance(other_tags, Node):
            for comma_tag in other_tags:
                tags.update(comma_tag[1])
        return {'tags': tags}

    def visit_field(self, node: Node, visited_children: tuple):
        return {node.children[0].text: node.children[2].text}

    def visit_field_set(self, node: Node, visited_children: tuple):
        fields = dict()
        fields.update(visited_children[0])
        other_fields = visited_children[1]
        if not isinstance(other_fields, Node):
            for comma_field in other_fields:
                fields.update(comma_field[1])
        return {'fields': fields}

    def visit_timestamp(self, node, visited_children):
        return {'time': node.text}

    def visit_line(self, node: Node, visited_children: tuple):
        ret = {}
        ret.update(visited_children[0])  # measurement
        tags = visited_children[1]
        if not isinstance(tags, Node):
            ret.update(tags[-1][-1])
        ret.update(visited_children[3])  # fields
        time = visited_children[-1]
        if not isinstance(time, Node):
            ret.update(time[-1][-1])
        return ret

    def visit_lines(self, node: Node, visited_children: list):
        ret = [visited_children[0]]
        other_lines = visited_children[-1]
        if not isinstance(other_lines, Node):
            for line in other_lines:
                ret.append(line[-1])
        return ret


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
    data = "cpu_load_short,host=server01,region=us-west,sdcsd=sdcsdc value=2 3516"
    wp = WriteVisitor()
    info = wp.parse(data)
    print(info)
    conn = psycopg2.connect(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, dbname=DATABASE)
    cur = conn.cursor()
    query_create_sensordata_table = f"""CREATE TABLE IF NOT EXISTS {info['measurement']} (
                                              time TIMESTAMPTZ NOT NULL,
                                              host VARCHAR(256),
                                              region VARCHAR(256),
                                              value DOUBLE PRECISION
                                              );"""
    query_create_sensordata_hypertable = f"SELECT create_hypertable('{info['measurement']}', 'time');"
    cur.execute(query_create_sensordata_table, query_create_sensordata_hypertable)
    query, data = get_query_and_data(info)
    cur.execute(query, data)
    conn.commit()
    print(query)


if __name__ == "__main__":
    main()
