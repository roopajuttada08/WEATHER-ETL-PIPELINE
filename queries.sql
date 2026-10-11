-- ============================================================
-- Weather ETL — Sample SQL analysis over weather_readings
-- ============================================================

-- 1) Average temperature & humidity per day (trends)
SELECT
    DATE(observed_at)        AS day,
    AVG(temperature_c)       AS avg_temp_c,
    AVG(humidity_pct)        AS avg_humidity,
    MIN(temperature_c)       AS min_temp_c,
    MAX(temperature_c)       AS max_temp_c
FROM weather_readings
GROUP BY day
ORDER BY day;

-- 2) Most common weather condition per city
SELECT
    city,
    condition,
    COUNT(*) AS occurrences,
    RANK() OVER (PARTITION BY city ORDER BY COUNT(*) DESC) AS r
FROM weather_readings
GROUP BY city, condition
QUALIFY r = 1;

-- 3) Temperature change vs previous reading (window function)
SELECT
    city,
    observed_at,
    temperature_c,
    LAG(temperature_c) OVER (PARTITION BY city ORDER BY observed_at) AS prev_temp,
    temperature_c - LAG(temperature_c) OVER (PARTITION BY city ORDER BY observed_at) AS delta
FROM weather_readings
ORDER BY city, observed_at;
