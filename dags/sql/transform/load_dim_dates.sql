INSERT INTO dim_dates (
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
    record AS date,
    extract(year FROM record) AS year,
    extract(month FROM record) AS month,
    to_char(record, 'TMMonth') AS month_name,
    extract(day FROM record) AS day,
    extract(doy FROM record) AS day_of_year,
    to_char(record, 'TMDay') AS weekday_name,
    extract(week FROM record) AS calendar_week,
    CASE
        WHEN
            extract(isodow FROM record) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS weekend
FROM (
    SELECT date('2021-01-01') + get_sequence.record_date AS record
    FROM generate_series(0, 365 * 2) AS get_sequence(record_date)
    GROUP BY get_sequence.record_date
) seq_records
