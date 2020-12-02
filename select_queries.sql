SELECT *
FROM "h2o_feet"
SELECT "level description", "location", "water_level"
FROM "h2o_feet"
SELECT ("water_level" * 2) + 4
FROM "h2o_feet"
SELECT *
FROM "absolutismus"
WHERE time = '2016-07-31T20:07:00Z'
   OR time = '2016-07-31T23:07:17Z'
SELECT *
FROM "h2o_feet"
WHERE "water_level" > 8
SELECT *
FROM "h2o_feet"
WHERE "level description" = 'below 3 feet'
SELECT *
FROM "h2o_feet"
WHERE "water_level" + 2 > 11.9
SELECT "water_level"
FROM "h2o_feet"
WHERE "location" = 'santa_monica'
SELECT "water_level"
FROM "h2o_feet"
WHERE "location" <> 'santa_monica'
  AND (water_level < -0.59 OR water_level > 9.95)
SELECT *
FROM "h2o_feet"
WHERE time > now() - 7 d
SELECT mean("index")
FROM "h2o_quality"
GROUP BY location, randtag
SELECT "water_level", "location"
FROM "h2o_feet"
WHERE time >= '2015-08-18T00:00:00Z'
  AND time <= '2015-08-18T00:30:00Z'
SELECT count("water_level")
FROM "h2o_feet"
WHERE "location" = 'coyote_creek'
  AND time >= '2015-08-18T00:00:00Z'
  AND time <= '2015-08-18T00:30:00Z'
GROUP BY time(12 m)
SELECT count("water_level")
FROM "h2o_feet"
WHERE time >= '2015-08-18T00:00:00Z'
  AND time <= '2015-08-18T00:30:00Z'
GROUP BY time(12 m), "location"
SELECT "water_level"
FROM "h2o_feet"
WHERE "location" = 'coyote_creek'
  AND time >= '2015-08-18T00:00:00Z'
  AND time <= '2015-08-18T00:18:00Z'
SELECT count("water_level")
FROM "h2o_feet"
WHERE "location" = 'coyote_creek'
  AND time >= '2015-08-18T00:06:00Z'
  AND time < '2015-08-18T00:18:00Z'
GROUP BY time(12 m)
SELECT "water_level"
FROM "h2o_feet"
WHERE "location" = 'coyote_creek'
  AND time >= '2015-08-18T00:00:00Z'
  AND time <= '2015-08-18T00:54:00Z'
SELECT mean("water_level")
FROM "h2o_feet"
WHERE "location" = 'coyote_creek'
  AND time >= '2015-08-18T00:06:00Z'
  AND time <= '2015-08-18T00:54:00Z'
GROUP BY time(6 m)
SELECT mean("water_level")
FROM "h2o_feet"
WHERE "location" = 'coyote_creek'
  AND time >= '2015-08-18T00:06:00Z'
  AND time <= '2015-08-18T00:54:00Z'
GROUP BY time(18 m)
SELECT mean("water_level")
FROM "h2o_feet"
WHERE "location" = 'coyote_creek'
  AND time >= '2015-08-18T00:06:00Z'
  AND time <= '2015-08-18T00:54:00Z'
GROUP BY time(-12 m)
SELECT mean("water_level")
FROM "h2o_feet"
WHERE "location" = 'coyote_creek'
  AND time >= '2015-08-18T00:06:00Z'
  AND time <= '2015-08-18T00:54:00Z'
GROUP BY time(18 m)
SELECT count("water_level")
FROM "h2o_feet"
WHERE "location" = 'coyote_creek'
  AND time >= '2015-08-18T00:06:00Z'
  AND time < '2015-08-18T00:18:00Z'
GROUP BY time(6 m)
SELECT count("water_level")
FROM "h2o_feet"
WHERE "location" = 'coyote_creek'
  AND time >= '2015-08-18T00:06:00Z'
  AND time < '2015-08-18T00:18:00Z'
GROUP BY time(12 m)
SELECT max("water_level")
FROM "h2o_feet"
WHERE "location" = 'coyote_creek'
  AND time >= '2015-09-18T16:00:00Z'
  AND time <= '2015-09-18T16:42:00Z'
GROUP BY time(12 m)
SELECT mean("water_level")
FROM "h2o_feet"
WHERE "location" = 'coyote_creek'
  AND time >= '2015-09-18T22:00:00Z'
  AND time <= '2015-09-18T22:18:00Z'
GROUP BY time(12 m) fill(800)
SELECT max("water_level")
FROM "h2o_feet"
WHERE location = 'coyote_creek'
  AND time >= '2015-09-18T16:24:00Z'
  AND time <= '2015-09-18T16:54:00Z'
GROUP BY time(12 m) fill(previous)
SELECT max("water_level")
FROM "h2o_feet"
WHERE location = 'coyote_creek'
  AND time >= '2015-09-18T16:36:00Z'
  AND time <= '2015-09-18T16:54:00Z'
GROUP BY time(12 m) fill(previous)
SELECT mean("tadpoles")
FROM "pond"
WHERE time > '2016-11-11T21:24:00Z'
  AND time <= '2016-11-11T22:06:00Z'
GROUP BY time(12 m) fill(linear)
SELECT mean("tadpoles")
FROM "pond"
WHERE time >= '2016-11-11T21:36:00Z'
  AND time <= '2016-11-11T22:06:00Z'
GROUP BY time(12 m) fill(linear)
SELECT "water_level"
FROM "h2o_feet"
WHERE "location" = 'santa_monica'
SELECT mean("water_level")
FROM "h2o_feet"
WHERE time >= '2015-08-18T00:00:00Z'
  AND time <= '2015-08-18T00:42:00Z'
GROUP BY time(12 m)
SELECT "water_level"
FROM "h2o_feet"
WHERE time > now() - 1 h
