from parsimonious.nodes import Node, NodeVisitor

from ._grammars import write_grammar


class WriteParser(NodeVisitor):
    grammar = write_grammar
    
    def visit_measurement(self, node: Node, visited_children: list):
        return {'measurement': node.text}
    
    def visit_tag(self, node: Node, visited_children: tuple):
        return {node.children[0].text: node.children[2].text}
    
    def visit_tag_set(self, node: Node, visited_children: list):
        tags = dict()
        
        tags.update(visited_children[0])
        
        if not isinstance(other_tags := visited_children[1], Node):  # If more that one tag
            for comma_tag in other_tags:
                tags.update(comma_tag[1])
        
        return {'tags': tags}
    
    def visit_field(self, node: Node, visited_children: tuple):
        return {node.children[0].text: node.children[2].text}
    
    def visit_field_set(self, node: Node, visited_children: list):
        fields = dict()
        
        fields.update(visited_children[0])
        
        if not isinstance(other_fields := visited_children[1], Node):  # If more that one field
            for comma_field in other_fields:
                fields.update(comma_field[1])
        
        return {'fields': fields}
    
    def visit_timestamp(self, node, visited_children):
        return {'time': node.text}
    
    def visit_line(self, node: Node, visited_children: list):
        ret = dict()
        
        ret.update(visited_children[0])  # measurement
        
        if not isinstance(tags := visited_children[1], Node):  # If tags present
            ret.update(tags[-1][-1])
        
        ret.update(visited_children[3])  # fields
        
        if not isinstance(timestamp := visited_children[-1], Node):  # If timestamp present
            ret.update(timestamp[-1][-1])
        
        return ret
    
    def visit_lines(self, node: Node, visited_children: list):
        ret = [visited_children[0]]
        
        if not isinstance(other_lines := visited_children[-1], Node):  # If more than one line
            for line in other_lines:
                ret.append(line[-1])
        
        return ret
    
    def generic_visit(self, node: Node, visited_children: list):
        return visited_children or node
