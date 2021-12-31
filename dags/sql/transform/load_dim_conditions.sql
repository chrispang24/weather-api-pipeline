INSERT INTO dim_conditions (
    code,
    name,
    description
)
SELECT DISTINCT
    weather_id,
    weather_main,
    weather_description
FROM
    raw_current_weather
