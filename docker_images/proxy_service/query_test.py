from encryptors import QueryEncryptor
from aggregators import QueryAggregator


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
    ("hostname" = '8d89770d7eb1')
    AND time >= now() - 5m and time <= 1605680700000ms
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
    key = b'\x00' * 32
    visitor = QueryEncryptor(key=key)
    
    index = 1
    
    tokens = visitor.parse(queries[index])
    print(tokens)
    print(queries[index])
    print('============')
    print(QueryAggregator.assemble(tokens))

