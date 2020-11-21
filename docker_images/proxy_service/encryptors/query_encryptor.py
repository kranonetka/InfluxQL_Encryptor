from datetime import timedelta

from ._base_encryptor import _BaseEncryptor
from ._grammars import influxql_grammar
from parsimonious.nodes import Node

_duration_unit_multpl = {
    'w': 60 * 24 * 24 * 7,
    'd': 60 * 24 * 24,
    'h': 60 * 24,
    'm': 60,
    's': 1,
    'ms': 10 ** (-3),
    'u': 10 ** (-6),
    'µ': 10 ** (-6),
    'ns': 10 ** (-9)
}


def _parse_duration(duration_lit_node: Node) -> timedelta:
    """
    duration_lit         = int_lit ws? duration_unit
    """
    duration = int(duration_lit_node.children[0].text)
    duration *= _duration_unit_multpl[duration_lit_node.children[2].text]
    return timedelta(seconds=duration)


class QueryEncryptor(_BaseEncryptor):
    grammar = influxql_grammar

    def visit_statement(self, node: Node, visited_children: list):
        return visited_children[0]

    def visit_drop_database_stmt(self, node: Node, visited_children: list):
        return {'action': 'drop database', 'database': node.children[2].text}

    def visit_create_database_stmt(self, node: Node, visited_children: list):
        return {'action': 'create database', 'database': node.children[2].text}
    
    def visit_show_databases_stmt(self, node: Node, visited_children: list):
        return {'action': 'show databases'}

    def visit_select_stmt(self, node: Node, visited_children: list):
        """
        select_stmt          = 'SELECT' ws+ field ws+ from_clause (ws+ where_clause)? (ws+ group_by_clause)?
        """
        from_ = visited_children[4]
        aggregation = visited_children[2]
        return {'action': 'select', **from_, **aggregation}
    
    def visit_from_clause(self, node: Node, visited_children: list):
        return {'measurement': node.children[2].children[0].children[0].children[1].text}

    def visit_field(self, node: Node, visited_children: list):
        field_node: Node = node.children[0].children[0].children[0]
        if field_node.expr_name == 'aggregation':
            aggr_func = field_node.children[0].text
            field_key = field_node.children[2].children[0].children[0]\
                .children[0].children[0].children[0].children[1].text
            return {'aggregation': aggr_func, 'field_key': field_key}
        else:  # measurement
            field_key = field_node.children[0].children[0].children[0].children[1].text
            return {'field_key': field_key}


if __name__ == '__main__':
    pass
