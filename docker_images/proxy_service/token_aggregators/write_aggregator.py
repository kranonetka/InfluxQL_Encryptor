from datetime import datetime

from parsers import WriteParser


class WriteAggregator:
    @staticmethod
    def assemble(tokens: dict) -> tuple:
        table_name = tokens['measurement']
        columns = ','.join([*tokens['tags'].keys(), *tokens['fields'].keys()])
        values = (*tokens['tags'].values(), *tokens['fields'].values())
        if tokens.get('time'):
            timestamp = datetime.fromtimestamp(int(tokens.get('time')))
        else:
            timestamp = "now()"
        insert_query = f"INSERT INTO {table_name}({columns},time) VALUES ({'%s,' * len(values)}{timestamp})"
        return insert_query, values


if __name__ == "__main__":
    payload = 'laptop,host=sever,name=acer memory=10i,freq=100 10'
    wp = WriteParser()
    tokens = wp.parse(payload)[0]
    print(tokens)
    print(WriteAggregator.assemble(tokens))
