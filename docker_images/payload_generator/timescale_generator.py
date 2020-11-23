import socket

import psutil
import psycopg2
from pgcopy import CopyManager
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

HOST = '127.0.0.1'
PORT = '5432'
USERNAME = 'postgres'
PASSWORD = 'password'
DATABASE = 'tutorial'


def create_db():
    conn = psycopg2.connect(host=HOST, port=PORT, user=USERNAME, password=PASSWORD)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {DATABASE}")
    cur.execute(f"CREATE DATABASE {DATABASE};")
    conn.commit()
    cur.close()


def create_table():
    conn = psycopg2.connect(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, dbname=DATABASE)
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    cur.execute("DROP TABLE IF EXISTS laptop_meas")
    query_create_sensordata_table = """CREATE TABLE laptop_meas (
                                              time TIMESTAMPTZ NOT NULL,
                                              hostname VARCHAR(256),
                                              cpu_percent DOUBLE PRECISION,
                                              cpu_freq DOUBLE PRECISION,
                                              memory_used BIGINT
                                              );"""
    query_create_sensordata_hypertable = "SELECT create_hypertable('laptop_meas', 'time');"
    cur.execute(query_create_sensordata_table)
    cur.execute(query_create_sensordata_hypertable)
    conn.commit()
    cur.close()


def simulate_data():
    conn = psycopg2.connect(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, dbname=DATABASE)
    cur = conn.cursor()

    simulate_query = """SELECT  generate_series(now() - interval '24 hour', now(), interval '5 minute') AS time,
        md5(random()::text) as hostname,
        random()*100 AS cpu_percent,
        random()*10000 AS cpu_freq,
        (random()*100000000)::int AS memory_used
        """
    cur.execute(simulate_query)
    values = cur.fetchall()
    cols = ('time', 'hostname', 'cpu_percent', 'cpu_freq', 'memory_used')
    mgr = CopyManager(conn, 'laptop_meas', cols)
    mgr.copy(values)
    conn.commit()
    cur.close()


def main():
    create_db()
    create_table()

    # simulate_data()

    conn = psycopg2.connect(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, dbname=DATABASE)
    cur = conn.cursor()
    payload_tpl = "INSERT INTO laptop_meas (time, hostname, cpu_percent, cpu_freq, memory_used) VALUES (now(), %s, %s, %s, %s);"
    while True:
        cur.execute(payload_tpl, (
            socket.gethostname(), psutil.cpu_percent(), psutil.cpu_freq().current, psutil.virtual_memory().used))
        conn.commit()
    cur.close()


if __name__ == '__main__':
    main()
