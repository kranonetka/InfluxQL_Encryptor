def get_field_keys(columns: dict, db_name: str, table: str) -> list:
    return [
        [column_name, attributes['type']]
        for column_name, attributes in columns.get(db_name, {}).get(table, {}).items() if not attributes['isTag']
    ]


def get_tag_keys(columns: dict, db_name: str, table: str) -> list:
    return [
        [column_name]
        for column_name, attributes in columns.get(db_name, {}).get(table, {}).items() if attributes['isTag']
    ]
