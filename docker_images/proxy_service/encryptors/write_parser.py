from _base_encryptor import _BaseEncryptor
from _grammars import write_grammar
from parsimonious.nodes import Node


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


def main():
    data = "cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000"
    wp = WriteVisitor()
    info = wp.parse(data)
    query = f"INSERT INTO {info['measurement']} ({','.join(info['tags'].keys())},{','.join(info['fields'].keys())},time) " \
            f"VALUES ({','.join(info['tags'].values())},{','.join(info['fields'].values())},{info['time']})"
    print(query)


if __name__ == "__main__":
    main()
