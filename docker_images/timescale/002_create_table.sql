CREATE TABLE laptop_meas (
    time TIMESTAMPTZ NOT NULL,
    hostname VARCHAR(256),
    cpu_percent DOUBLE PRECISION,
    cpu_freq DOUBLE PRECISION,
    memory_used BIGINT
)
