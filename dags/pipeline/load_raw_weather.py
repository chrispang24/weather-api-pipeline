import pandas as pd
from pipeline import config
from pipeline.helpers.file_store_io import load_json_file
from sqlalchemy import create_engine
from sqlalchemy.sql import text

DB_ENGINE = config.params["DB_ENGINE"]
DB_SCHEMA = config.params["DB_SCHEMA"]
DB_RAW_TABLE = config.params["DB_RAW_TABLE"]


def process_weather_data(data: dict) -> pd.DataFrame:
    """Cleans and normalizes weather data object into dataframe"""

    # flatten data object - keeping only primary weather condition
    data["weather"] = data["weather"][0]
    df = pd.json_normalize(data, sep="_")

    # convert unix epoch to datetime
    for col in ["dt", "sys_sunrise", "sys_sunset"]:
        df[col] = pd.to_datetime(df[col], unit="s")

    return df


def load_raw_weather(tag: str, **context: dict) -> None:
    """Processes weather data and performs upsert load to raw table"""

    extended_tag = f"{tag}-{context.get('ts_nodash')}"
    data = load_json_file(extended_tag)
    data["extract_tag"] = extended_tag

    df = process_weather_data(data)

    # upsert - delete where existing based on tag, then append all source records
    engine = create_engine(DB_ENGINE)
    engine.execute(
        text(
            f"DELETE FROM {DB_RAW_TABLE} WHERE extract_tag = '{extended_tag}'"
        ).execution_options(autocommit=True)
    )
    df.to_sql(DB_RAW_TABLE, engine, schema=DB_SCHEMA, if_exists="append", index=False)
