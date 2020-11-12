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
