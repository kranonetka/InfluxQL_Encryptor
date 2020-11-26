from typing import List
from parsers import Action


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
        print(query_result, tokens)
        return {}
