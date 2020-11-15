import sqlite3


def set_type(db, meas, field_key, field_type):
    connection = sqlite3.connect("types.db")
    cursor = connection.cursor()
    insert = f"INSERT OR IGNORE INTO types (db, meas, field_key, field_type) VALUES (?,?,?,?);"
    cursor.execute(insert, (db, meas, field_key, field_type))
    connection.commit()


# def get_type():


def init_db():
    connection = sqlite3.connect("types.db")
    cursor = connection.cursor()
    create_table = """
    CREATE TABLE IF NOT EXISTS types(
    db text,
    meas text,
    field_key text,
	field_type text,
	UNIQUE(db, meas, field_key, field_type)
    )
    """
    cursor.execute(create_table)


def get_query_and_data(info):
    table_name = info['measurement']
    tags_keys = ','.join(info['tags'].keys())
    fields_keys = ','.join(info['fields'].keys())
    number_of_values = len(info['tags']) + len(info['fields'])
    data = (*info['tags'].values(), *info['fields'].values())
    query = f"INSERT INTO {table_name} ({tags_keys},{fields_keys},time) VALUES ({'%s,' * number_of_values}now())"
    return query, data
