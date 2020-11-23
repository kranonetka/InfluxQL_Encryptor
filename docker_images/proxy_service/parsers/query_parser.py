from datetime import datetime, timedelta
from operator import sub, add

from parsimonious.nodes import Node, NodeVisitor

from ._grammars import influxql_grammar

_operators = {
    '-': sub,
    '+': add
}

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


class QueryParser(NodeVisitor):
    grammar = influxql_grammar
    
    def visit_show_retention_policies_stmt(self, node: Node, visited_children: list):
        return {'action': 'show retention policies', 'database': node.children[2].children[2].text}
    
    def visit_show_measurements_stmt(self, node: Node, visited_children: list):
        return {'action': 'show measurements', 'limit': node.children[2].children[2].text}
    
    def visit_show_field_keys_stmt(self, node: Node, visited_children: list):
        return {'action': 'show field keys',
                'measurement': node.children[2].children[2].children[0].children[0].children[1].text}
    
    def visit_statement(self, node: Node, visited_children: list):
        return visited_children[0]
    
    def visit_drop_database_stmt(self, node: Node, visited_children: list):
        return {'action': 'drop database', 'database': node.children[2].text}
    
    def visit_create_database_stmt(self, node: Node, visited_children: list):
        return {'action': 'create database', 'database': node.children[2].text}
    
    def visit_show_databases_stmt(self, node: Node, visited_children: list):
        return {'action': 'show databases'}
    
    def visit_where_clause(self, node: Node, visited_children: list):
        """
        where_clause         = 'WHERE' ws+ lpar quoted_identifier ws* comp_op ws* string_lit rpar (ws* logical_op ws* where_time)?
        where_time           = time_condition (ws* logical_op ws* time_condition)?
        """
        conditions = []
        
        tag_key = node.children[3].children[1].text
        tag_op = node.children[5].text
        tag_value = node.children[7].children[1].text
        conditions.append((tag_key, tag_op, tag_value))
        
        time_conditions = visited_children[-1]
        
        if not isinstance(time_conditions, Node):  # if latest token present (returns as list)
            conditions += time_conditions[-1][-1]  # Getting result of visit_where_time
        
        return {"where_conditions": conditions}
    
    def visit_where_time(self, node: Node, visited_children: list):
        """
        where_time           = time_condition (ws* logical_op ws* time_condition)?
        """
        conditions = [visited_children[0]]
        if not isinstance(visited_children[-1], Node):
            conditions.append(visited_children[-1][-1][-1])
        return *conditions,
    
    def visit_time_condition(self, node: Node, visited_children: list):
        op = node.children[2].text
        value_node: Node = node.children[4].children[0]
        if value_node.expr_name == 'duration_lit':
            duration = _parse_duration(value_node)
            dt = datetime.fromtimestamp(duration.total_seconds())
        else:
            duration = _parse_duration(value_node.children[4])
            dt = _operators[value_node.children[2].text](datetime.now(), duration)
        return 'time', op, dt
    
    def visit_group_by_clause(self, node: Node, visited_children: list):
        """
        group_by_clause      = 'GROUP BY' ws+ dimension (ws+ 'fill' lpar fill_option rpar)?
        """
        return {'group_by': visited_children[2]['dimension']}
    
    def visit_dimension(self, node: Node, visited_children: list):
        dimension: Node = node.children[0]
        if dimension.expr_name == 'duration':
            return {'dimension': _parse_duration(dimension.children[2].children[0])}
        else:  # measurement
            return {'dimension': node.children[0].children[0].children[0].children[0].children[1].text}
    
    def visit_select_stmt(self, node: Node, visited_children: list):
        """
        select_stmt          = 'SELECT' ws+ field ws+ from_clause (ws+ where_clause)? (ws+ group_by_clause)?
        """
        params = {
            **visited_children[4],  # from
            **visited_children[2]  # field and aggregation
        }
        
        where_tokens = visited_children[-2]
        if not isinstance(where_tokens, Node):  # if present
            conditions = where_tokens[-1][-1]
            params.update(conditions)
        
        group_by_tokens = visited_children[-1]
        if not isinstance(group_by_tokens, Node):
            params.update(group_by_tokens[-1][-1])
        
        return {'action': 'select', **params}
    
    def visit_from_clause(self, node: Node, visited_children: list):
        return {'measurement': node.children[2].children[0].children[0].children[1].text}
    
    def visit_field(self, node: Node, visited_children: list):
        field_node: Node = node.children[0].children[0].children[0]
        if field_node.expr_name == 'aggregation':
            aggr_func = field_node.children[0].text
            field_key = field_node.children[2].children[0].children[0] \
                .children[0].children[0].children[0].children[1].text
            return {'aggregation': aggr_func, 'field_key': field_key}
        else:  # measurement
            field_key = field_node.children[0].children[0].children[0].children[1].text
            return {'field_key': field_key}
        
    def generic_visit(self, node: Node, visited_children: list):
        return visited_children or node