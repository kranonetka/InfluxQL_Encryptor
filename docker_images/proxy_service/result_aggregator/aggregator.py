from datetime import datetime
from itertools import chain
from typing import List

from parsers import Action


def normalize(item):
    if isinstance(item, datetime):
        return int(item.timestamp() * 1000)
    elif isinstance(item, (int, float)):
        return item
    else:
        raise TypeError(f'{type(item)}')


def normalize_seq(seq):
    return list(map(normalize, seq))


class ResultAggregator:
    @staticmethod
    def assemble(query_result: List[tuple], tokens: dict) -> dict:
        action = tokens.get('action')
        if action == Action.SELECT:
            assembler = ResultAggregator._assemble_select
        elif action == Action.SHOW_TAG_VALUES:
            assembler = ResultAggregator._assemble_tag_values
        elif action == Action.SHOW_RETENTION_POLICIES:
            assembler = ResultAggregator._assemble_show_retention_policies
        elif action == Action.SHOW_MEASUREMENTS:
            assembler = ResultAggregator._assemble_show_measurements
        else:
            raise NotImplementedError(str(action))
        
        return assembler(query_result, tokens)
    
    @staticmethod
    def _assemble_select(query_result: List[tuple], tokens: dict) -> dict:
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
    
    @staticmethod
    def _assemble_tag_values(query_result: List[tuple], tokens: dict) -> dict:
        return {
            "results": [
                {
                    "statement_id": 0,
                    "series": [
                        {
                            "name": tokens['measurement'],
                            "columns": [
                                "key", "value"
                            ],
                            "values": query_result
                        }
                    ]
                }
            ]
        }
    
    @staticmethod
    def _assemble_show_retention_policies(query_result: List[tuple], tokens: dict) -> dict:
        if tokens['database'] in chain.from_iterable(query_result):
            return {
                "results": [
                    {
                        "statement_id": 0,
                        "series": [
                            {
                                "columns": [
                                    "name",
                                    "duration",
                                    "shardGroupDuration",
                                    "replicaN",
                                    "default"
                                ],
                                "values": [
                                    [
                                        "autogen",
                                        "0s",
                                        "168h0m0s",
                                        1,
                                        True
                                    ]
                                ]
                            }
                        ]
                    }
                ]
            }
        else:
            return {
                "results": [
                    {
                        "statement_id": 0,
                        "error": f"database not found: {tokens['database']}"
                    }
                ]
            }
    
    @staticmethod
    def _assemble_show_measurements(query_result: List[tuple], tokens: dict) -> dict:
        return {
            "results": [
                {
                    "statement_id": 0,
                    "series": [
                        {
                            "name": "measurements",
                            "columns": [
                                "name"],
                            "values": query_result
                        }
                    ]
                }
            ]
        }
