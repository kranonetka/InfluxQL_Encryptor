CREATE VIEW points_per_sec
    WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 sec', "time") AS "time",
       count(*)                              AS "count"
FROM "laptop_meas"
GROUP BY 1;