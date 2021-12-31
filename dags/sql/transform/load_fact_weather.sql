INSERT INTO fact_weather (
    temp,
    temp_min,
    temp_max,
    feels_like,
    pressure,
    humidity,
    wind_speed,
    wind_deg,
    wind_gust,
    last_hour_rain,
    last_hour_snow,
    cloudiness,
    visibility,
    condition_key,
    location_key,
    date_key
)
SELECT
    raw_current_weather.main_temp,
    raw_current_weather.main_temp_min,
    raw_current_weather.main_temp_max,
    raw_current_weather.main_feels_like,
    raw_current_weather.main_pressure,
    raw_current_weather.main_humidity,
    raw_current_weather.wind_speed,
    raw_current_weather.wind_deg,
    raw_current_weather.wind_gust,
    raw_current_weather.rain_1h,
    raw_current_weather.snow_1h,
    raw_current_weather.clouds_all,
    raw_current_weather.visibility,
    dim_conditions.condition_key,
    dim_locations.location_key,
    dim_dates.date_key
FROM
    raw_current_weather
INNER JOIN dim_conditions
    ON dim_conditions.code = raw_current_weather.weather_id
INNER JOIN dim_locations
    ON dim_locations.code = raw_current_weather.id
INNER JOIN dim_dates
    ON dim_dates.date = DATE(raw_current_weather.dt)
