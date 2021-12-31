from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "transformer",
    default_args=default_args,
    description="To transform the raw current weather to a modeled dataset",
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["take-home"],
) as dag:

    create_modeled_tables = PostgresOperator(
        task_id="create_modeled_tables",
        sql=[
            "sql/create/dim_dates.sql",
            "sql/create/dim_conditions.sql",
            "sql/create/dim_locations.sql",
            "sql/create/fact_weather.sql",
            "sql/create/obt_current_weather.sql",
        ],
    )

    transform_dim_dates = PostgresOperator(
        task_id="transform_dim_dates",
        sql=["TRUNCATE TABLE dim_dates CASCADE", "sql/transform/load_dim_dates.sql"],
    )

    transform_dim_conditions = PostgresOperator(
        task_id="transform_dim_conditions",
        sql=[
            "TRUNCATE TABLE dim_conditions CASCADE",
            "sql/transform/load_dim_conditions.sql",
        ],
    )

    transform_dim_locations = PostgresOperator(
        task_id="transform_dim_locations",
        sql=[
            "TRUNCATE TABLE dim_locations CASCADE",
            "sql/transform/load_dim_locations.sql",
        ],
    )

    transform_fact_weather = PostgresOperator(
        task_id="transform_fact_weather",
        sql=["TRUNCATE TABLE fact_weather", "sql/transform/load_fact_weather.sql"],
    )

    transform_obt_current_weather = PostgresOperator(
        task_id="transform_obt_current_weather",
        sql=[
            "TRUNCATE TABLE obt_current_weather",
            "sql/transform/load_obt_current_weather.sql",
        ],
    )

    # need to load dimension tables before fact table due to FK dependencies
    (
        create_modeled_tables
        >> [
            transform_dim_dates,
            transform_dim_conditions,
            transform_dim_locations,
        ]
        >> transform_fact_weather
        >> transform_obt_current_weather
    )
