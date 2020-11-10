from ._base_encryptor import _BaseEncryptor
from ._grammars import influxql_grammar


class QueryEncryptor(_BaseEncryptor):
    grammar = influxql_grammar
