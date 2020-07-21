import pandas as pd
from utils import download_from_drive, treat_text, match_place_id
from endpoints.helpers import allow_local
from endpoints import get_places_id


def _read_df_data(country, config):

    tables = ["cities_population", "health_infrastructure"]

    dfs = {
        name: download_from_drive(config[country]["drive_paths"][name])
        for name in tables
    }

    df = pd.merge(
        dfs["cities_population"],
        dfs["health_infrastructure"],
        on="city_id",
        how="left",
        suffixes=("", "_y"),
    )
    return df.drop([c for c in df.columns if "_y" in c], axis=1)


@allow_local
def now(config, country="br"):

    df = _read_df_data(country, config)
    places_ids = get_places_id.now(config).assign(
        city_id=lambda df: df["city_id"].astype(int)
    )

    # Fix for default places ids
    df = df.drop(["state_name", "health_system_region", "city_name"], axis=1).merge(
        places_ids, on=["city_id", "state_id"]
    )

    # Fix date types
    time_cols = [c for c in df.columns if "last_updated" in c]
    df[time_cols] = df[time_cols].apply(pd.to_datetime)

    df[["number_beds", "number_ventilators"]] = df[
        ["number_beds", "number_ventilators"]
    ].fillna(0)

    # Add DataSUS author
    df["author_number_beds"] = config[country]["cnes"]["source"]
    df["author_number_ventilators"] = config[country]["cnes"]["source"]

    return df


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) <= 5570,
    "no negative beds or ventilators": lambda df: len(
        df.query("number_beds < 0 | number_ventilators < 0")
    )
    == 0,
}
