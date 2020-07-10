import pandas as pd
from utils import download_from_drive, treat_text, match_place_id
from endpoints.helpers import allow_local
from endpoints import get_places_id


def _read_cities_data(country, config):

    tables = ["cities_population", "health_infrastructure"]

    return {
        name: download_from_drive(config[country]["drive_paths"][name])
        for name in tables
    }


@allow_local
def now(config, country="br"):

    cities = _read_cities_data(country, config)
    cities = pd.merge(
        cities["cities_population"],
        cities["health_infrastructure"],
        on="city_id",
        how="left",
        suffixes=("", "_y"),
    )

    matchs = {
        "city_name": "city_id",
        "state_name": "state_id",
        "state_num_id": "state_id",
        "health_region_id": ["health_system_region", "state_id"],
    }
    cities = match_place_id(
        cities.drop([c for c in cities.columns if "_y" in c], 1),
        get_places_id.now(config),
        matchs,
    )
    # cities["city_norm"] = cities["city_name"].apply(treat_text)

    time_cols = [c for c in cities.columns if "last_updated" in c]
    cities[time_cols] = cities[time_cols].apply(pd.to_datetime)

    cities[["number_beds", "number_ventilators"]] = cities[
        ["number_beds", "number_ventilators"]
    ].fillna(0)

    cities["author_number_beds"] = "DataSUS"  # config[country]["health"]["source"]
    cities["author_number_ventilators"] = "DataSUS"

    return cities


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) <= 5570,
    "no negative beds or ventilators": lambda df: len(
        df.query("number_beds < 0 | number_ventilators < 0")
    )
    == 0,
}
