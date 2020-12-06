from main import query_tokens_aggregator, query_tokens_encryptor, postgres_connector, result_decryptor
from parsers import QueryParser
from result_aggregator import ResultAggregator

query = '''SELECT mean("cpu_freq") FROM "laptop_meas" WHERE ("hostname" = 'payload_generator') AND time >= now() - 6h GROUP BY time(15s) fill(null)'''

if __name__ == '__main__':
    visitor = QueryParser()
    
    influxql_query = query
    print(influxql_query, end='\n=========\n')
    
    tokens = visitor.parse(influxql_query)
    print(tokens, end='\n---------\n')
    
    encrypted_tokens = query_tokens_encryptor.encrypt(tokens)
    
    postgres_query, params = query_tokens_aggregator.assemble(encrypted_tokens)
    with postgres_connector.cursor() as cur:
        print(cur.mogrify(postgres_query, params).decode())
        print('~~~~~~~~~~~~')
    
    # result = postgres_connector.execute('''SELECT time_bucket('2s', "time"), avg(memory_used) FROM laptop_meas WHERE hostname = '8d89770d7eb1' AND "time" BETWEEN now() - interval '5 min' AND now() GROUP BY 1 ORDER BY 1''')
    # exit()
    
    encrypted_result = postgres_connector.execute(postgres_query, params=params)
    print(encrypted_result, end='\n-=-=-=-=-=-=-=\n')
    
    decrypted_result = result_decryptor.decrypt(encrypted_result, 'laptop', tokens)
    print(decrypted_result)
    
    influxql_resp = ResultAggregator.assemble(decrypted_result, tokens)
    print(influxql_resp)
