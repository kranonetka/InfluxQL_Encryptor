from pathlib import Path

from parsimonious import Grammar

_root = Path(__file__).parent

with (_root / 'influxql.grammar').open(mode='r', encoding='utf-8') as fp:
    influxql_grammar = Grammar(fp.read())

with (_root / 'write.grammar').open(mode='r', encoding='utf-8') as fp:
    write_grammar = Grammar(fp.read())
