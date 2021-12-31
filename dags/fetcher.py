from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from pipeline import config
from pipeline.ingest_weather_api import ingest_weather_api
from pipeline.load_raw_weather import load_raw_weather

TARGET_CITIES = config.params["OPEN_WEATHER_TARGET_CITIES"]


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "fetcher",
    default_args=default_args,
    description="To fetch the weather data",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["take-home"],
) as dag:

    # map reference tags for cities
    cities_tag_list = {city: city.lower().replace(" ", "_") for city in TARGET_CITIES}

    with TaskGroup(group_id="ingest_api_data") as ingest_api_data:

        for city, tag in cities_tag_list.items():
            PythonOperator(
                task_id=f"ingest_{tag}",
                python_callable=ingest_weather_api,
                provide_context=True,
                op_kwargs={"target_query": city, "tag": tag},
            )

    create_raw_dataset = PostgresOperator(
        task_id="create_raw_dataset", sql="sql/create/raw_current_weather.sql"
    )

    with TaskGroup(group_id="load_raw_dataset") as load_raw_dataset:

        for tag in cities_tag_list.values():
            PythonOperator(
                task_id=f"load_raw_{tag}",
                python_callable=load_raw_weather,
                provide_context=True,
                op_kwargs={"tag": tag},
            )

    ingest_api_data >> create_raw_dataset >> load_raw_dataset
