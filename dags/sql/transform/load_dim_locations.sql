INSERT INTO dim_locations (
    code,
    name,
    longitude,
    latitude
)
SELECT DISTINCT
    id,
    name,
    coord_lon,
    coord_lat
FROM
    raw_current_weather
