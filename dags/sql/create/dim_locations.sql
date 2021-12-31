CREATE TABLE IF NOT EXISTS dim_locations
(
    location_key SERIAL PRIMARY KEY,
    code INTEGER,
    name TEXT,
    longitude REAL,
    latitude REAL
)
