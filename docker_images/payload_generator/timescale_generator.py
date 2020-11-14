import socket

import psutil
import psycopg2

HOST = '127.0.0.1'
PORT = '5432'
USERNAME = 'postgres'
PASSWORD = 'password'
DATABASE = 'tutorial'

conn = psycopg2.connect(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, dbname=DATABASE)


def create_table():
    cur = conn.cursor()
    cur.execute("DROP TABLE laptop_meas")
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


def main():
    create_table()
    cur = conn.cursor()
    payload_tpl = "INSERT INTO laptop_meas (time, hostname, cpu_percent, cpu_freq, memory_used) VALUES (now(), %s, %s, %s, %s);"
    while True:
        cur.execute(payload_tpl, (
            socket.gethostname(), psutil.cpu_percent(), psutil.cpu_freq().current, psutil.virtual_memory().used))
        conn.commit()


if __name__ == '__main__':
    main()
