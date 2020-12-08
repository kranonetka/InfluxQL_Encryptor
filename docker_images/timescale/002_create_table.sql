CREATE TABLE laptop_meas
(
    time        TIMESTAMPTZ NOT NULL,
    hostname    VARCHAR(256),
    cpu_percent NUMERIC,
    cpu_freq    NUMERIC,
    memory_used NUMERIC
);

CREATE INDEX ON laptop_meas (hostname, time DESC);
