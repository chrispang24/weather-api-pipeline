params = {
    "DB_ENGINE": "postgresql+psycopg2://airflow:airflow@postgres/airflow",
    "DB_SCHEMA": "public",
    "DB_RAW_TABLE": "raw_current_weather",
    "OPEN_WEATHER_API_KEY": "<API_KEY>",
    "OPEN_WEATHER_ENDPOINT": "https://api.openweathermap.org/data/2.5/",
    "OPEN_WEATHER_TARGET_CITIES": [
        "Toronto",
        "Montreal",
        "Vancouver",
        "New York",
        "San Francisco",
    ],
}
