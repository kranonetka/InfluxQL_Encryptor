import os
from base64 import b64encode
from itertools import chain
from pathlib import Path

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from mimesis.providers import Generic
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor, Node

root = Path(__file__).parent

g = Generic('en')


class InfluxQLEncryptor(NodeVisitor):
    def __init__(self, key: bytes):
        super().__init__()
        self._padder_factory = padding.PKCS7(8 * 32)
        self._cipher_factory = Cipher(
            algorithm=algorithms.AES(key),
            mode=modes.CBC(
                initialization_vector=os.urandom(16)
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
        return f'encrypted({encrypted_token})'
    
    visit_identifier = _encrypt_token
    
    def generic_visit(self, node, visited_children):
        return ''.join(visited_children) or node.text  # Для неопределенных токенов возвращаем просто их текст


with (root / 'influxql.grammar').open(mode='r', encoding='utf-8') as fp:
    influxql_grammar = Grammar(fp.read())  # Чтобы не заморачиваться с экранированием символов

basic_queries = (
    f"DROP DATABASE fruits",
    f"CREATE USER {g.person.username('U')} WITH PASSWORD '{g.person.username('l-U-d')}' WITH ALL PRIVILEGES",
    f"SELECT \"autogen\".\"{''.join(g.food.fruit().split())}\" FROM fruits",
    f"SELECT \"autogen\".\"{''.join(g.food.fruit().split())}\" FROM fruits WHERE n=10",
    f"SELECT \"autogen\".\"{''.join(g.food.fruit().split())}\" FROM fruits WHERE n=10 GROUP BY fruit fill(none)",
)

if __name__ == '__main__':
    key = os.urandom(32)
    visitor = InfluxQLEncryptor(key)

    for query in chain(basic_queries, ('; '.join(basic_queries),)):
        print(query)
        tree = influxql_grammar.parse(query)
        
        res = visitor.visit(tree)
        print(res)
        print()
