import pandas as pd
import datetime
import numpy as np
from urllib.request import Request, urlopen
import json
import time

from endpoints.get_city_cases import(
    get_infectious_period_cases,
    _get_growth,
    get_mavg_indicators,
    correct_negatives,
    download_brasilio_table
)
from endpoints import get_places_id
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
        )

        # Fix places_ids
        places_ids = get_places_id.now(config).assign(
            city_id=lambda df: df["city_id"].astype(int)
        )
        df = (
            df.drop(["city_name"], 1)
            .assign(city_id=lambda df: df["city_id"].astype(int))
            .merge(
                places_ids[
                    [
                        "city_id",
                        "city_name",
                        "health_region_name",
                        "health_region_id",
                        "state_name",
                        "state_num_id",
                    ]
                ],
                on="city_id",
            )
        )

        # Aggregation by health region and last_updated 
        df = (
            df.groupby(["health_region_id","last_updated"])
            .agg(
                estimated_population_2019 = ('estimated_population_2019', sum),
                confirmed_cases = ("confirmed_cases", sum),
                deaths = ("deaths", sum),
                daily_cases = ("daily_cases", sum),
                new_deaths = ("new_deaths", sum)
            )
            .reset_index()
        )

        # Correct negative values, get infectious period cases and get median of new cases
        df = (
            df.groupby("health_region_id")
            .apply(correct_negatives)
            .pipe(
                get_infectious_period_cases,
                infectious_period,
                config["br"]["cases"],
                "health_region_id",
            )
            .rename(columns=config["br"]["cases"]["rename"])
        )

        # Get indicators of mavg & growth
        df = get_mavg_indicators(df, "daily_cases", place_id="health_region_id")
        df = get_mavg_indicators(df, "new_deaths", place_id="health_region_id")

        # Get notification rates & active cases on date
        df = df.merge(
            get_notification_rate.now(df, "health_region_id"),
            on=["state_num_id", "last_updated"],
            how="left",
        ).assign(
            active_cases=lambda x: np.where(
                x["notification_rate"].isnull(),
                np.nan, #round(x["infectious_period_cases"], 0),
                round(x["infectious_period_cases"] / x["notification_rate"], 0),
            ),
            state_id=lambda x: x["state_id"].astype(int),
        )

    return df


# Output dataframe tests to check data integrity. This is also going to be called
# by main.py
TESTS = {
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) <= 5570,
    "more than 27 states": lambda df: len(df["state_id"].unique()) <= 27,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),

}
