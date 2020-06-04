import pandas as pd
from utils import download_from_drive, treat_text
from endpoints.helpers import allow_local


def _read_cities_data(country, config):

    paths = config[country]["drive_paths"]

    return {name: download_from_drive(url) for name, url in paths.items()}


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
    cities = cities.drop([c for c in cities.columns if "_y" in c], 1)

    cities["city_norm"] = cities["city_name"].apply(treat_text)

    time_cols = [c for c in cities.columns if "last_updated" in c]
    cities[time_cols] = cities[time_cols].apply(pd.to_datetime)

    return cities


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}


if __name__ == "__main__":

    pass
