import subprocess
import unicodedata
import tempfile
import pandas as pd
import yaml
import os
import requests


def _remove_accents(text):
    return (
        unicodedata.normalize("NFKD", text)
        .encode("ASCII", "ignore")
        .decode("ASCII")
        .upper()
    )


def _drop_forbiden(text):

    forbiden = [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    ]

    words = [t.strip() for t in text.split(" ")]

    for f in forbiden:
        if f in words:
            words.remove(f)

    return " ".join(words)


def treat_text(s):

    s = s.apply(_remove_accents)
    s = s.apply(_drop_forbiden)
    return s


def get_last(_df, sort_by="last_updated"):

    return _df.sort_values(sort_by).groupby(["city_id"]).last().reset_index()


def download_from_drive(url):

    temp_path = tempfile.gettempdir() + "/temp.csv"

    response = subprocess.run(["curl", "-o", temp_path, url + "/export?format=csv&id"])

    return pd.read_csv(temp_path)


def secrets(variable, path="secrets.yaml"):

    local = yaml.load(open(path, "r"), Loader=yaml.FullLoader)

    if local.get(variable):
        return local[variable]
    else:
        return os.getenv(variable)


def get_config(url=os.getenv("CONFIG_URL")):

    return yaml.load(requests.get(url).text, Loader=yaml.FullLoader)

