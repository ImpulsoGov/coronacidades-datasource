import pandas as pd
import datetime
import numpy as np
import json
import time

import gzip
import io
from urllib.request import Request, urlopen

from endpoints.helpers import allow_local
from endpoints import get_places_id
from endpoints.scripts import get_notification_rate
from utils import download_from_drive

from endpoints.get_cities_cases import (
    download_brasilio_table,
    correct_negatives,
    get_infectious_period_cases,
    get_mavg_indicators,
    get_until_last
)


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
        )

        # Fix places_ids by city_id => Get health_region_id
        places_ids = get_places_id.now(config).assign(
            city_id=lambda df: df["city_id"].astype(int),
            health_region_id=lambda df: df["health_region_id"].astype(int),
        )

        df = (
            df.drop(["city_name", "state_id"], 1)
            .assign(city_id=lambda df: df["city_id"].astype(int))
            .merge(
                places_ids[
                    [
                        "city_id",
                        "health_region_name",
                        "health_region_id",
                        "state_name",
                        "state_id",
                        "state_num_id",
                    ]
                ].drop_duplicates(),
                on="city_id",
            )
        )

        # Group cases by health region
        df = (
            df.groupby(
                [
                    "state_num_id",
                    "state_id",
                    "state_name",
                    "health_region_name",
                    "health_region_id",
                    "last_updated",
                ]
            )
            .agg(
                estimated_population_2019=("estimated_population_2019", sum),
                confirmed_cases=("confirmed_cases", sum),
                deaths=("deaths", sum),
                daily_cases=("daily_cases", sum),
                new_deaths=("new_deaths", sum),
            )
            .reset_index()
        )

        # Transform cases data
        df = (
            df.groupby(
                [
                    "state_num_id",
                    "state_id",
                    "state_name",
                    "health_region_name",
                    "health_region_id",
                ]
            )
            .apply(correct_negatives)  # correct negative values
            .pipe(
                get_infectious_period_cases,
                infectious_period,
                config["br"]["cases"],
                "health_region_id",
            )  # get infectious period cases
            .rename(columns=config["br"]["cases"]["rename"])
        )

        # Get indicators of mavg & growth
        df = get_mavg_indicators(df, "daily_cases", place_id="health_region_id")
        df = get_mavg_indicators(df, "new_deaths", place_id="health_region_id")

        # Get notification rates & active cases on date
        df = df.merge(
            get_notification_rate.now(df, "health_region_id"),
            on=["health_region_id", "last_updated"],
            how="left",
        ).assign(
            active_cases=lambda x: np.where(
                x["notification_rate"].isnull(),
                np.nan,  # round(x["infectious_period_cases"], 0),
                round(x["infectious_period_cases"] / x["notification_rate"], 0),
            )
        )

    return df


TESTS = {
    "more than 5570 cities": lambda df: len(df["health_region_id"].unique()) <= 450,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    # TODO: corrigir teste! => ultima taxa calculada 14 dias antes
    # "notification_rate == NaN": lambda df: len(
    #     df[(df["notification_rate"].isnull() == True) & (df["is_last"] == True)].values
    # )
    # == 0,
    # "max(confirmed_cases) != max(date)": lambda df: all(
    # (df.groupby("city_id").max()["confirmed_cases"] \
    #  == df.query("is_last==True").set_index("city_id").sort_index()["confirmed_cases"]).values),
    # "max(deaths) != max(date)": lambda df: all(
    # (df.groupby("city_id").max()["deaths"] \
    #  == df.query("is_last==True").set_index("city_id").sort_index()["deaths"]).values)
}
