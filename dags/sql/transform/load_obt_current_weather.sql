INSERT INTO obt_current_weather (
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
    condition_code,
    condition_name,
    condition_description,
    location_code,
    location_name,
    location_longitude,
    location_latitude,
    date,
    year,
    month,
    month_name,
    day,
    day_of_year,
    weekday_name,
    calendar_week,
    weekend
)
SELECT
    fact_weather.temp,
    fact_weather.temp_min,
    fact_weather.temp_max,
    fact_weather.feels_like,
    fact_weather.pressure,
    fact_weather.humidity,
    fact_weather.wind_speed,
    fact_weather.wind_deg,
    fact_weather.wind_gust,
    fact_weather.last_hour_rain,
    fact_weather.last_hour_snow,
    fact_weather.cloudiness,
    fact_weather.visibility,
    dim_conditions.code,
    dim_conditions.name,
    dim_conditions.description,
    dim_locations.code,
    dim_locations.name,
    dim_locations.longitude,
    dim_locations.latitude,
    dim_dates.date,
    dim_dates.year,
    dim_dates.month,
    dim_dates.month_name,
    dim_dates.day,
    dim_dates.day_of_year,
    dim_dates.weekday_name,
    dim_dates.calendar_week,
    dim_dates.weekend
FROM
    fact_weather
INNER JOIN dim_conditions
    ON dim_conditions.condition_key = fact_weather.condition_key
INNER JOIN dim_locations
    ON dim_locations.location_key = fact_weather.location_key
INNER JOIN dim_dates
    ON dim_dates.date_key = fact_weather.date_key
