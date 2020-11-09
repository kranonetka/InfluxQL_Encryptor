import json
import operator
import re
from base64 import b64encode
from statistics import mean

import pandas as pd
import requests
import time
from itertools import groupby
from parsimonious.nodes import Node

from encryptors import _BaseEncryptor
from grammars import influxql_grammar

_duration_lit_re = re.compile(r'(?P<duration>[+-]?\d)+\s*(?P<unit>\w+)')
_units_multiplication = {
    'w': 10 ** 9 * 60 * 60 * 24 * 7,
    'd': 10 ** 9 * 60 * 60 * 24,
    'h': 10 ** 9 * 60 * 60,
    'm': 10 ** 9 * 60,
    's': 10 ** 9,
    'ms': 10 ** 6,
    'u': 1,
    'µ': 1
}


class QueryEncryptor(_BaseEncryptor):
    def __init__(self, *args, **kwargs):
        super(QueryEncryptor, self).__init__(*args, **kwargs)
        self._additional_info = dict()
    
    @property
    def additional_info(self):
        retval = self._additional_info
        self._additional_info = dict()
        return retval
    
    def visit_group_by_clause(self, node: Node, visited_children: tuple):
        self._additional_info['is_group'] = True
        _, _, dimension, *_ = node.children  # type: Node
        dimension = self.lift_child(None, dimension)
        if dimension.expr_name == 'duration':
            _, time_interval, dot_offset, *_ = dimension.children
            time_duration = time_interval.children[0]
            time_interval_value = int(time_duration.children[0].text) * _units_multiplication[
                time_duration.children[2].text]
            if dot_offset.children:
                offset_interval = dot_offset.children[0].children[1]
                offset_duration = offset_interval.children[0]
                offset_interval_value = int(offset_duration.children[0].text) * _units_multiplication[
                    offset_duration.children[2].text]
            else:
                offset_interval_value = 0
            dct = dict(group_dimension='duration', group_time_interval=time_interval_value,
                       group_offset_interval=offset_interval_value)
        else:
            dct = dict(group_dimension='measurement', measurement_name=visited_children[2])
        self._additional_info.update(dct)
        return ''
    
    def visit_aggregation(self, node: Node, visited_children: tuple):
        self._additional_info['aggregation_func'] = visited_children[0]
        
        return visited_children[2]  # Remove it for encryption (needs quotes fix)
        
        field_name = visited_children[2]
        encrypted_field_name = b64encode(self._encrypt_bytes(field_name.encode())).rstrip(b'=')
        return f'"{encrypted_field_name.decode()}"'
    
    def visit_identifier(self, node: Node, visited_children: tuple):
        ident_type: Node = node.children[0]
        if ident_type.expr_name == 'quoted_identifier':
            ident = node.text[1:-1]
        else:
            ident = node.text
        
        return ident  # Remove it for encryption
        
        encrypted_ident = b64encode(self._encrypt_bytes(ident.encode())).rstrip(b'=')
        return f'"{encrypted_ident.decode()}"'
    
    def generic_visit(self, node: Node, visited_children: tuple):
        return ''.join(visited_children) or node.text


def date_to_boundary(date_str: str, duration, offset) -> str:
    for format in ('%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ'):
        try:
            timestamp = pd.to_datetime(date_str, format=format)
        except:
            continue
        else:
            break
    
    boundary = offset + duration * ((timestamp.value - offset) // duration)
    # print(date_str)
    # print(timestamp)
    # print(timestamp.value)
    # print(boundary)
    
    boundary = pd.to_datetime(boundary)
    ret_str = boundary.isoformat()
    if boundary.nanosecond + boundary.microsecond:
        return ret_str.rstrip('0') + 'Z'
    else:
        return ret_str + 'Z'


def process_values(values, aggregation_func, is_group=False, group_dimension=None, group_time_interval=None,
                   group_offset_interval=None):
    funcs = {
        'sum': sum,
        'count': lambda x: len(tuple(x)),
        'min': min,
        'max': max,
        'mean': mean
    }
    
    func = funcs[aggregation_func]
    new_values = []
    for key, group in groupby(values, key=lambda x: date_to_boundary(x[0], group_time_interval, group_offset_interval)):
        new_values.append([key, func(map(operator.itemgetter(1), group))])
    
    return new_values


if __name__ == '__main__':
    enc = QueryEncryptor(b'0' * 16)
    print(b64encode(enc._encrypt_bytes('value'.encode())).rstrip(b'='))
    q = 'SELECT sum("value") FROM "meas"' \
        ' WHERE \'1970-01-01T00:00:00Z\' <= time AND time < \'1970-01-01T00:00:00Z\' + 5d' \
        ' GROUP BY time(5s,7s)'
    
    q = """SELECT mean("cpu_percent") FROM "laptop_meas"
     WHERE ("hostname" = '"e6699e35de45"') AND now() - 5m <= time
      GROUP BY time(5s)"""
    print(q)
    tree = influxql_grammar.parse(q)
    res = enc.visit(tree)
    print(res)
    add_info = enc.additional_info
    print(add_info)
    
    start_time = time.time()
    resp = requests.get('http://localhost:8086/query', params=dict(
        db='laptop',
        q=q
    )).json()
    
    # resp['results'][0]['series'][0]['values'] = process_values(resp['results'][0]['series'][0]['values'], **add_info)
    stop_time = time.time()
    print(f'{stop_time - start_time:.2f}')
    
    with open('res.json', 'w') as fp:
        json.dump(resp, fp, indent=4)
    
    # print(json.dumps(res, indent=4))
