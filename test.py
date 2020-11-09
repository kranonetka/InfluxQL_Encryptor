import random
import string
import struct
import uuid
from base64 import b64encode
from functools import partial
from itertools import chain, cycle, islice
from operator import mul
from pathlib import Path

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from parsimonious.nodes import NodeVisitor, Node

from grammars import influxql_grammar, write_grammar

root = Path(__file__).parent


class InfluxQLEncryptor(NodeVisitor):
    def __init__(self, key: bytes):
        super().__init__()
        self._padder_factory = padding.PKCS7(8 * len(key))
        comp_id = uuid.getnode()
        comp_id = struct.pack('>Q', comp_id)[2:]
        iv = bytes(
            islice(
                cycle(comp_id),
                0,
                16)
        )
        self._cipher_factory = Cipher(
            algorithm=algorithms.AES(key),
            mode=modes.CBC(
                initialization_vector=iv
            )
        )
    
    def _encode_token(self, node: Node, visited_children):
        return f"encoded({b64encode(node.text.encode()).decode()})"
    
    def _encrypt_token(self, node: Node, visited_children):
        padder = self._padder_factory.padder()
        encryptor = self._cipher_factory.encryptor()
        token = node.text.encode()
        padded_token = padder.update(token) + padder.finalize()
        encrypted_token = encryptor.update(padded_token) + encryptor.finalize()
        encrypted_token = b64encode(encrypted_token).decode()
        return f'"{encrypted_token}"'
    
    visit_identifier = _encrypt_token
    
    def generic_visit(self, node, visited_children):
        return ''.join(visited_children) or node.text  # Для неопределенных токенов возвращаем просто их текст


class TreeAssembler(NodeVisitor):
    def generic_visit(self, node: Node, visited_children: list):
        if node.expr_name:
            fstr = f'{node.expr_name}({{}})'
        else:
            fstr = '{}'
        
        return fstr.format(
            ''.join(visited_children) or node.text
        )


def encrypt_query(query: str, key: bytes) -> str:
    tree = influxql_grammar.parse(query)
    visitor = InfluxQLEncryptor(key)
    return visitor.visit(tree)


def test_queries():
    from mimesis.providers import Generic
    g = Generic('en')
    key = bytes(islice(cycle(b'mysuppakey'), 0, 32))
    
    basic_queries = (
        "DROP DATABASE fruits",
        "CREATE DATABASE fruits",
        f"CREATE USER {g.person.username('U')} WITH PASSWORD '{g.person.username('l-U-d')}' WITH ALL PRIVILEGES",
        f"SELECT \"autogen\".\"{''.join(g.food.fruit().split())}\" FROM fruits",
        f"SELECT \"autogen\".\"{''.join(g.food.fruit().split())}\" FROM fruits WHERE n=10",
        f"SELECT \"autogen\".\"{''.join(g.food.fruit().split())}\" FROM fruits WHERE n=10 GROUP BY fruit fill(none)",
        'SELECT "water_level" FROM "h2o_feet" WHERE time > now() - 1h'
    )
    
    for query in chain(basic_queries, ('; '.join(basic_queries),)):
        print(query)
        enc = encrypt_query(query, key)
        print(enc)
        print()


def test_write(float_sensors=1, int_sensors=1, str_sensors=1, bool_sensors=1, duration=1):
    def random_float() -> str:
        """
        :return: Случайное вещественное число из диапазона [0;1000) для записи в InfluxDB
        """
        return '{:.5e}'.format(random.random() * 1000)
    
    def random_int() -> str:
        """
        :return: Случайное целое число из диапазона [0; 10000) для записи в InfluxDB
        """
        return f'{random.randrange(1000)}i'
    
    def random_str() -> str:
        """
        :return: Случайная строка длина 60 для записи в InfluxDB
        """
        return '"{}"'.format(''.join(random.choices(string.ascii_letters, k=60)))
    
    def random_bool() -> str:
        """
        :return: Случайное булево значение для записи в InfluxDB
        """
        return random.choice(('t', 'f', 'T', 'F', 'true', 'false', 'True', 'False', 'TRUE', 'FALSE'))
    
    start_timestamp_ms = random.randint(0, 0xffffffff)
    dot_template = 'python_measurement,thread={} {{{{}}}}={{{{}}}},q=0 {{}}'.format(1)
    
    payload = '\n'.join(
        '\n'.join(
            chain(
                (time_template.format('float', random_float()) for _ in range(float_sensors)),
                (time_template.format('int', random_int()) for _ in range(int_sensors)),
                (time_template.format('str', random_str()) for _ in range(str_sensors)),
                (time_template.format('bool', random_bool()) for _ in range(bool_sensors)),
            )
        )
        for time_template in map(
            dot_template.format,
            map(
                start_timestamp_ms.__add__,
                map(
                    partial(mul, 10),
                    range(duration)
                )
            )
        )
    )
    print(payload)
    tree = write_grammar.parse(payload)
    print(tree)
    assemble_visitor = TreeAssembler()
    assembled = assemble_visitor.visit(tree)
    print(assembled)


def test_selects():
    with (root / 'select_queries.sql').open(mode='r', encoding='utf-8') as fp:
        queries = list(map(str.rstrip, fp.readlines()))
    for query in queries:
        print(query)
        influxql_grammar.parse(query)


if __name__ == '__main__':
    test_selects()
    test_queries()
    test_write(duration=5)
