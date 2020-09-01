import pandas as pd
import datetime
import numpy as np
from urllib.request import Request, urlopen
import json
import time

from endpoints.get_cities_cases import (
    get_infectious_period_cases,
    _get_growth,
    get_mavg_indicators,
    correct_negatives,
    download_brasilio_table,
    get_until_last,
)
from endpoints import get_health, get_places_id
from endpoints.scripts import get_notification_rate
from endpoints.helpers import allow_local


@allow_local
def now(config, country="br"):

    if country == "br":
        infectious_period = (
            config["br"]["seir_parameters"]["severe_duration"]
            + config["br"]["seir_parameters"]["critical_duration"]
        )

        # Get data & clean table
        df = (
            download_brasilio_table(config["br"]["cases"]["url"])
            .query("place_type == 'city'")
            .dropna(subset=["city_ibge_code"])
            .fillna(0)
            .rename(columns=config["br"]["cases"]["rename"])
            .assign(last_updated=lambda x: pd.to_datetime(x["last_updated"]))
            .sort_values(["city_id", "state_id", "last_updated"])
            .groupby("city_id")
            .apply(lambda group: get_until_last(group))
            .reset_index(drop=True)
            .drop(columns="estimated_population_2019")
        )

        # Fix places_ids by city_id => Get health_region_id
        df = df.merge(
            get_places_id.now(config)
            .assign(state_num_id=lambda df: df["state_num_id"].astype(int),)[
                ["state_name", "state_id", "state_num_id",]
            ]
            .drop_duplicates(),
            on="state_id",
        )

        # Group cases by states
        df = (
            df.groupby(["state_num_id", "state_id", "state_name", "last_updated",])
            .agg(
                confirmed_cases=("confirmed_cases", sum),
                deaths=("deaths", sum),
                daily_cases=("daily_cases", sum),
                new_deaths=("new_deaths", sum),
            )
            .reset_index()
        )

        # Add population data from CNES
        df = df.merge(
            get_health.now(config)
            .assign(state_num_id=lambda df: df["state_num_id"].astype(int),)
            .groupby("state_num_id")["population"]
            .sum()
            .reset_index(),
            on="state_num_id",
        )

        # Transform cases data
        df = (
            df.groupby(["state_num_id", "state_id", "state_name",])
            .apply(correct_negatives)  # correct negative values
            .pipe(
                get_infectious_period_cases,
                infectious_period,
                config["br"]["cases"],
                "state_num_id",
            )  # get infectious period cases
            .rename(columns=config["br"]["cases"]["rename"])
        )

        # Get indicators of mavg & growth
        df = get_mavg_indicators(df, "daily_cases", place_id="state_num_id")
        df = get_mavg_indicators(df, "new_deaths", place_id="state_num_id")

        # Get notification rates & active cases on date
        df = df.merge(
            get_notification_rate.now(df, "state_num_id"),
            on=["state_num_id", "last_updated"],
            how="left",
        ).assign(
            active_cases=lambda x: np.where(
                x["notification_rate"].isnull(),
                np.nan,  # round(x["infectious_period_cases"], 0),
                round(x["infectious_period_cases"] / x["notification_rate"], 0),
            )
        )

    return df


# Output dataframe tests to check data integrity. This is also going to be called
# by main.py
TESTS = {
    "not 27 states": lambda df: len(df["state_id"].unique()) == 27,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "population not fixed by state": lambda df: len(
        df[["population", "state_id"]].drop_duplicates()
    )
    == len(df["state_id"].drop_duplicates()),
}
