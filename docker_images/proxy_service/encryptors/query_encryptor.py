from ._base_encryptor import _BaseEncryptor
from ._grammars import influxql_grammar
from parsimonious.nodes import Node


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
        ws_where = node.children[5]
        if ws_where.children:
            where_conditions = self._get_where_conditions(ws_where.children[0].children[1])
        else:
            where_conditions = []
        return {'action': 'select', **from_, **aggregation}
    
    def visit_from_clause(self, node: Node, visited_children: list):
        return {'measurement': node.children[2].children[0].children[0].children[1].text}
    
    def _get_where_conditions(self, where_clause_node: Node):
        """
        where_clause         = 'WHERE' ws+ lpar quoted_identifier ws* equal ws* string_lit rpar ws* logical_op ws* where_time
        where_time           = time_word ws* arithmetical_op ws* expr (ws* logical_op ws* time_word ws* arithmetical_op ws* expr)?
        """
        tag_key = where_clause_node.children[3].children[1].text
        tag_value = where_clause_node.children[7].children[1].text
        
        where_time_node = where_clause_node.children[-1]
        print(where_time_node)
        

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
