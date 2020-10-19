from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor, Node
from base64 import b64encode


class InfluxQLVisitor(NodeVisitor):
    def encode_token(self, node: Node, visited_children):
        return f"'{b64encode(node.text.encode()).decode()}'"
    
    visit_user_name = encode_token
    
    visit_password = encode_token
    
    visit_db_name = encode_token
    
    def generic_visit(self, node, visited_children):
        return ''.join(visited_children) or node.text  # Для неопределенных токенов возвращаем просто их текст


with open('influxql.grammar', 'r', encoding='utf-8') as fp:  # Считывание грамматики из файла
    influxql_grammar = Grammar(fp.read())  # Чтобы не заморачиваться с экранированием символов


queries_for_test = (
    "DROP DATABASE my_secret_db",
    "CREATE USER \"jdoe\" WITH PASSWORD '1337password' WITH ALL PRIVILEGES; DROP DATABASE my_secret_db",
    "SELECT autogen.mymeas FROM python_measurement"
)


if __name__ == '__main__':
    for query in queries_for_test:
        tree = influxql_grammar.parse(query)  # Получение дерева разбора запроса
        
        visitor = InfluxQLVisitor()
        res = visitor.visit(tree)  # Обход дерева с заменой необходимых токенов
        print(query)
        print(res)
        print()
    #print(json.dumps(res, indent=1))

