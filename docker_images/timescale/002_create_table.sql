CREATE TABLE laptop_meas
(
    time   TIMESTAMPTZ NOT NULL,
    thread VARCHAR(256),
    float  NUMERIC
);

SELECT create_hypertable('laptop_meas', 'time');

CREATE INDEX ON laptop_meas ("thread", "time" DESC);
