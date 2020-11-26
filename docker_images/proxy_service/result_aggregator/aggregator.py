from typing import List
from parsers import Action
from decimal import Decimal
from datetime import datetime


def normalize(item):
    if isinstance(item, Decimal):
        return float(item)
    elif isinstance(item, datetime):
        return int(item.timestamp() * 1000)
    elif isinstance(item, int):
        return item
    else:
        raise TypeError(f'{item}')
    
    
def normalize_seq(seq):
    return list(map(normalize, seq))


class ResultAggregator:
    @staticmethod
    def assemble(query_result: List[tuple], tokens: dict) -> dict:
        action = tokens.get('action')
        if action == Action.SELECT:
            assembler = ResultAggregator._assemble_select
        else:
            raise NotImplementedError()
        
        return assembler(query_result, tokens)

    @staticmethod
    def _assemble_select(query_result: List[tuple], tokens: dict):
        return {
            "results": [
                {
                    "statement_id": 0,
                    "series": [
                        {
                            "name": tokens.get('measurement'),
                            "columns": [
                                "time",
                                tokens.get('aggregation', tokens['field_key'])
                            ],
                            "values": list(map(normalize_seq, query_result))
                        }
                    ]
                }
            ]
        }
