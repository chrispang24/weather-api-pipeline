CREATE TABLE IF NOT EXISTS dim_dates
(
    date_key SERIAL PRIMARY KEY,
    date DATE,
    year INTEGER,
    month INTEGER,
    month_name TEXT,
    day INTEGER,
    day_of_year INTEGER,
    weekday_name TEXT,
    calendar_week INTEGER,
    weekend TEXT
)
