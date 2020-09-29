from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor, Node
from base64 import b64encode
import random


class InfluxQLVisitor(NodeVisitor):
    def visit_user_name(self, node: Node, visited_children):
        return b64encode(node.text.encode()).decode()  # Вместо имени пользователя возвращаем base64 от него
    
    visit_password = visit_user_name  # То же самое для user_name
    
    visit_db_name = visit_user_name  # То же самое для db_name
    
    def generic_visit(self, node, visited_children):
        return ''.join(visited_children) or node.text  # Для неопределенных токенов возвращаем просто их текст


with open('influxql.grammar', 'r', encoding='utf-8') as fp:  # Считывание грамматики из файла
    influxql_grammar = Grammar(fp.read())  # Чтобы не заморачиваться с экранированием символов

query = """CREATE USER "jdoe" WITH PASSWORD '1337password' WITH ALL PRIVILEGES; DROP DATABASE my_secret_db"""  # Запрос для разбора

if __name__ == '__main__':
    tree = influxql_grammar.parse(query)  # Получение дерева разбора запроса
    
    visitor = InfluxQLVisitor()
    res = visitor.visit(tree)  # Обход дерева с заменой необходимых токенов
    print(res)

