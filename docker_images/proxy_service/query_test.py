from parsers import QueryParser
from token_aggregators import QueryAggregator
import os
from postgres_connector import PostgresConnector
from result_aggregator import ResultAggregator


try:
    FLASK_PORT = os.environ['FLASK_PORT']
    POSTGRES_HOST = os.environ['POSTGRES_HOST']
    POSTGRES_PORT = os.environ['POSTGRES_PORT']
    POSTGRES_USERNAME = os.environ['POSTGRES_USERNAME']
    POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
    POSTGRES_DATABASE = os.environ['POSTGRES_DATABASE']
except KeyError as key:
    raise RuntimeError(f'{key} missing in environment')


postgres_connector = PostgresConnector(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USERNAME,
    password=POSTGRES_PASSWORD,
    db=POSTGRES_DATABASE
)

queries = (
    """SELECT
    mean("memory_used")
FROM
    "laptop_meas"
WHERE
    ("hostname" = '694d42a595f2')
    AND time >= now() - 5m
GROUP BY
    time(200ms)
    fill(null)""",
    """SELECT
    mean("memory_used")
FROM
    "laptop_meas"
WHERE
    ("hostname" = '69d117922a5f')
    AND time >= now() - 1d
GROUP BY
    time(2s)
    fill(null)""",
    '''SELECT
    mean("memory_used")
FROM
    "laptop_meas"
WHERE
    ("hostname" = '8d89770d7eb1')
    AND time >= 1605680400000ms and time <= 1605680700000ms
GROUP BY
    "hostname"'''
)


if __name__ == '__main__':
    visitor = QueryParser()
    index = 1
    
    influxql_query = queries[index]
    print(influxql_query, end='\n=========\n')
    
    tokens = visitor.parse(influxql_query)
    print(tokens, end='\n---------\n')
    
    postgres_query = QueryAggregator.assemble(tokens)
    #print(postgres_query, end='\n~~~~~~~~~~~~\n')

    #result = postgres_connector.execute('''SELECT time_bucket('2s', "time"), avg(memory_used) FROM laptop_meas WHERE hostname = '8d89770d7eb1' AND "time" BETWEEN now() - interval '5 min' AND now() GROUP BY 1 ORDER BY 1''')
    result = postgres_connector.execute(postgres_query)
    #print(result, end='\n-=-=-=-=-=-=-=\n')
    
    influxql_resp = ResultAggregator.assemble(result, tokens)
    print(influxql_resp)
