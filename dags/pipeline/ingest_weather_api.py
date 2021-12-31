import json

import requests
from pipeline import config
from pipeline.helpers.file_store_io import save_json_file

ENDPOINT = config.params["OPEN_WEATHER_ENDPOINT"]
API_KEY = config.params["OPEN_WEATHER_API_KEY"]


def ingest_weather_api(target_query: str, tag: str, **context: dict) -> None:
    """Takes in a query for calling OpenWeatherMap API and stores result with tag"""

    extended_tag = f"{tag}-{context.get('ts_nodash')}"

    url = f"{ENDPOINT}/weather?q={target_query}&appid={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = json.loads(response.text)

    save_json_file(data, extended_tag)
