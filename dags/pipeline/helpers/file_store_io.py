import json

DATA_PATH = "/opt/airflow/data/"


def save_json_file(data: dict, name: str):
    with open(DATA_PATH + name + ".json", "w") as write_file:
        json.dump(data, write_file)


def load_json_file(name: str) -> dict:
    with open(DATA_PATH + name + ".json") as read_file:
        return json.load(read_file)
