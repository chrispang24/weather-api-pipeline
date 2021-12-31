CREATE TABLE IF NOT EXISTS fact_weather
(
    temp REAL,
    temp_min REAL,
    temp_max REAL,
    feels_like REAL,
    pressure INTEGER,
    humidity INTEGER,
    wind_speed REAL,
    wind_deg INTEGER,
    wind_gust REAL,
    last_hour_rain REAL,
    last_hour_snow REAL,
    cloudiness INTEGER,
    visibility INTEGER,
    condition_key INTEGER REFERENCES dim_conditions,
    location_key INTEGER REFERENCES dim_locations,
    date_key INTEGER REFERENCES dim_dates
)
