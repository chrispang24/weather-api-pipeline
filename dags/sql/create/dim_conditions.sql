CREATE TABLE IF NOT EXISTS dim_conditions
(
    condition_key SERIAL PRIMARY KEY,
    code INTEGER,
    name TEXT,
    description TEXT
)
